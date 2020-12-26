package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	//"strings"

	"github.com/gwDistSys20/project-kafkaesque/filemanager"
	"github.com/gwDistSys20/project-kafkaesque/utils"
	"github.com/lithammer/shortuuid/v3"
	"github.com/thoas/go-funk"
)

var topicManagerInstance TopicManager

type Topic struct {
	mutex        sync.Mutex
	name         string
	topicChannel chan Message
	nPartition   int
	partitions   []*partition
	leaderIds    []int
	leaderIndex  int
}

// TopicManager struct keeps track of all the topics in the broker
type TopicManager struct {
	topics map[string]*Topic
	folder int
	fm     *filemanager.FileManager
}

func GetTopicManager() *TopicManager {
	return &topicManagerInstance
}

func (tm *TopicManager) addTopic(newTopicName string, nPartition int) *TopicManager {

	topic := tm.topics[newTopicName]
	if topic != nil {
		return tm
	}
	topic = &Topic{name: newTopicName, topicChannel: make(chan Message), nPartition: nPartition}
	partitions := []*partition{}
	leaderIds := []int{}
	// loop over the nPartition, creating n partitions and adding it to topicManager
	for i := 0; i < nPartition; i++ {
		partitions = append(partitions, newPartition(i, newTopicName))
	}
	for _, partition := range partitions {
		if partition.raft.GetState() == "leader" {
			leaderIds = append(leaderIds, partition.id)
		}
	}

	topic.partitions = partitions
	topic.leaderIds = leaderIds
	tm.topics[newTopicName] = topic

	return tm
}

func (tm *TopicManager) getTopics() interface{} {
	return funk.Keys(tm.topics)
}

func (tm *TopicManager) getSingleTopic(t string) (*Topic, bool) {
	topic := tm.topics[t]
	if topic == nil {
		log.Println("topic not found")
		return nil, false
	}
	return topic, true
}

// used for debugging
func (tm *TopicManager) printPartitions(topicName string) {
	topic := tm.topics[topicName]

	if topic == nil {
		log.Println("topic not found")
	}

	for _, value := range topic.partitions {
		fmt.Println("partition", value)
	}
}

func (tm *TopicManager) publish(topicName string, msg Message) {
	// test message
	topic := tm.topics[topicName]
	if topic == nil {
		log.Println("topic not found")
		return
	}
	topic.mutex.Lock()
	leaderIndex := topic.leaderIndex
	topic.mutex.Unlock()

	leaderIds := topic.leaderIds
	nLeaders := len(leaderIds)

	if nLeaders == 0 {
		fmt.Println("nLeaders is 0")
		for _, partition := range topic.partitions {
			if partition.raft.GetState() == "leader" {
				leaderIds = append(leaderIds, partition.id)
			}
		}
		nLeaders = len(leaderIds)
	}
	var leaderPartition *partition

	// if key is provided, update the leader index according to hash function
	if len(msg.Key) > 0 {
		leaderIndex = utils.Ihash(msg.Key, nLeaders)
	}

	selectedLeaderID := leaderIds[leaderIndex]

	for _, p := range topic.partitions {
		if p.id == selectedLeaderID {
			leaderPartition = p
			break
		}
	}
	// encoding message to be saved on disk
	encMsg := msg.encodeMessage()

	leaderPartition.raft.Submit(encMsg)
	// wait for the message to be available for commit
	<-leaderPartition.taskCompChan
	// message added to struct
	messages := append(leaderPartition.messages, msg)
	leaderPartition.messages = messages

	tm.printPartitions(topic.name)

	// in each iteration choose the next partition unless a specific key is provided
	if nLeaders == leaderIndex+1 {
		leaderIndex = 0
	} else {
		leaderIndex++
	}
	// update the leader index in topic
	topic.mutex.Lock()
	topic.leaderIndex = leaderIndex
	topic.mutex.Unlock()
}

// can only subscribe to partitions that are leader
func (tm *TopicManager) subscribe(topicName string, groupName string, nPartition int) ([]string, error) {
	var ids []string
	topic := tm.topics[topicName]

	if topic == nil {
		log.Println("Error: topic does not exist")
	}
	// check if a partition has already been assigned to a subscriber from
	// the same consumer group
	// consumer group can only subscribe to a partition that has not been assigned
	// to the same consumer group
	// if there are not partitions that can be assigned then return an error

	subscriptionSuccess := false
	totalSubscription := 0

	for _, partition := range topic.partitions {
		if partition.raft.GetState() != "leader" {
			continue
		}
		subscribers := partition.subscribers

		alreadyAssignedFlag := false

		for _, subscriber := range subscribers {

			if subscriber.groupName == groupName {
				alreadyAssignedFlag = true
				break
			}

		}

		if alreadyAssignedFlag == false {
			id := shortuuid.New()
			sub := &subscriber{id: id, groupName: groupName, offset: 0}
			partition.subscribers = append(partition.subscribers, sub)
			ids = append(ids, id)
			subscriptionSuccess = true
			totalSubscription++
		}
		// already assigned desired number of partitions, exit
		if totalSubscription == nPartition {
			break
		}
	}

	if subscriptionSuccess == true {
		return ids, nil
	}
	return ids, errors.New("Failed to create subscription. All partitions are already assigned")
}

func (tm *TopicManager) consume(topicName string, subscriptionID string, offset string) (messages []Message, err error) {
	topic := tm.topics[topicName]
	fm := filemanager.GetFileManager()
	for _, partition := range topic.partitions {
		if partition.hasSubscription(subscriptionID) {
			logIndex, _ := fm.FetchLogIndex(partition.indexFile, offset)
			fm.FetchMessages(partition.logFile, logIndex, func(binaryMessage []byte) {
				// convert the binary message to object and append it to messages array
				var msg Message
				json.Unmarshal(binaryMessage, &msg)

				messages = append(messages, msg)
			})
			fmt.Println(messages)
			break
		}
	}
	return messages, err
}

type subscriber struct {
	id        string
	groupName string
	offset    int
}

// NewTopicManager constructor
func NewTopicManager() *TopicManager {
	// syncOnce.Do(func() {
	tp := TopicManager{topics: make(map[string]*Topic)}
	topicManagerInstance = tp
	// })
	return &topicManagerInstance
}
