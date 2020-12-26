package broker

import (
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/gwDistSys20/project-kafkaesque/config"

	"github.com/gwDistSys20/project-kafkaesque/filemanager"
	"github.com/gwDistSys20/project-kafkaesque/raft"
)

type partition struct {
	mutex        sync.Mutex
	id           int
	name         string
	indexFile    string
	logFile      string
	messages     []Message
	subscribers  []*subscriber
	offset       string
	raft         *raft.Raft
	commitChan   chan raft.Commit
	taskCompChan chan bool
	// segment       int
	activeSegment int
}

func (p *partition) hasSubscription(subscriptionID string) bool {
	for _, subscriber := range p.subscribers {
		if subscriber.id == subscriptionID {
			return true
		}
	}
	return false
}

func (p *partition) listenForCommits() {
	for {
		commit := <-p.commitChan
		byteData := commit.Data.([]byte)
		p.raft.PrintCurrentState()
		p.CommitMessageToFile(byteData)
		status := p.raft.GetState()
		// fmt.Println(status)
		if status == "leader" {
			fmt.Println("leader status getting called")
			p.taskCompChan <- true
			continue
		}
		// fmt.Println(commit.Data)
	}
}

func (p *partition) CommitMessageToFile(message []byte) {
	fm := filemanager.GetFileManager()
	conf := config.GetConfig()
	totalLines := fm.CountLines(p.logFile)
	// fmt.Println("Total Lines", totalLines)
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if totalLines >= conf.MaxFileLines {
		p.activeSegment++
		p.indexFile = path.Join(p.name+"-"+strconv.Itoa(p.id), "index"+strconv.Itoa(p.activeSegment)+".txt")
		p.logFile = path.Join(p.name+"-"+strconv.Itoa(p.id), "log"+strconv.Itoa(p.activeSegment)+".txt")

		wg := sync.WaitGroup{}
		wg.Add(2)
		go fm.AddFile(p.indexFile, &wg)
		go fm.AddFile(p.logFile, &wg)
		wg.Wait()
	}
	logIndex, _ := fm.Append(p.logFile, message)

	offset, _ := strconv.Atoi(p.offset)

	// index file will contain offset,logIndex
	indexFileEntry := strconv.Itoa(offset) + "," + strconv.Itoa(logIndex)

	// update the index file
	fm.Append(p.indexFile, []byte(indexFileEntry))

	// updating offset by 1
	p.offset = strconv.Itoa(offset + 1)
}

// NewPartition constructor
func newPartition(id int, topic string) *partition {
	activeSegment := 0
	indexFile := path.Join(topic+"-"+strconv.Itoa(id), "index"+strconv.Itoa(activeSegment)+".txt")
	logFile := path.Join(topic+"-"+strconv.Itoa(id), "log"+strconv.Itoa(activeSegment)+".txt")

	fm := filemanager.GetFileManager()
	// create physical partitions on disk
	var wg sync.WaitGroup
	wg.Add(2)
	go fm.AddFile(indexFile, &wg)
	go fm.AddFile(logFile, &wg)
	wg.Wait()
	// partition instance
	p := partition{
		id:            id,
		name:          topic,
		indexFile:     indexFile,
		logFile:       logFile,
		subscribers:   []*subscriber{},
		messages:      []Message{},
		offset:        "",
		activeSegment: activeSegment,
	}

	startElection := make(chan bool)

	serviceName := "partition:" + strconv.Itoa(id)

	// get the raft rpc Server
	raftServer := raft.GetServerInstance()

	// channel where all the commits are going to be available
	commitChan := make(chan raft.Commit)
	taskCompChan := make(chan bool)

	raftObj := raft.NewRaft(strconv.Itoa(id), serviceName, raftServer, commitChan, startElection)

	// register the partition service on rpc
	raft.RegisterService(serviceName, raftObj)

	p.raft = raftObj
	p.commitChan = commitChan
	p.taskCompChan = taskCompChan
	go p.listenForCommits()

	startElection <- true
	return &p
}
