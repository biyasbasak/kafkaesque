package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/gwDistSys20/project-kafkaesque/config"
	"github.com/gwDistSys20/project-kafkaesque/raft"
)

var syncOnce sync.Once

var brokerServerInstace Server

// Server struct
type Server struct {
	id           string
	addr         string
	listener     net.Listener
	tm           *TopicManager
	raft         *raft.Raft
	taskCompChan chan bool
	commitChan   chan raft.Commit
}

func (s *Server) listenforCommits() {
	for {
		commit := <-s.commitChan
		data := commit.Data.(string)
		s.raft.PrintCurrentState()
		fmt.Println(data)
		fields := strings.Split(data, ",")

		topicName := fields[1]
		nPartition, err := strconv.Atoi(fields[2])
		if err != nil {
			fmt.Println(err)
		}
		s.tm.addTopic(topicName, nPartition)

		// if this is the leader broker then broker needs to be notified so it can
		// return a response to the end user client
		status := s.raft.GetState()
		if status == "leader" {
			fmt.Println("leader status getting called")
			s.taskCompChan <- true
		}

	}

}

func (s *Server) Start() {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Fatalf("Unable to start server: %s", err.Error())
	}
	log.Println("Broker Server running on: ", s.addr)
	s.listener = listener
	// defer listener.Close()

	s.listenForConnection()
}

func (s *Server) listenForConnection() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Printf("Unable to accept connection %s", err.Error())
			continue
		}
		log.Print("New connection accepted")
		go s.handleConnection(&conn)
	}
}

func (s *Server) handleConnection(conn *net.Conn) {
	connReader := bufio.NewReader(*conn)
	connWriter := bufio.NewWriter(*conn)
	client := NewClient(connWriter, connReader)

	for {
		fields := client.read('\n')

		if len(fields) == 0 {
			log.Printf("Enter a command")
			continue
		}

		cmd := strings.ToUpper(fields[0])

		switch cmd {
		case "CRTP":
			nPartition := 1
			var err error
			if len(fields) < 2 {
				log.Println("Error while creating topic")
				client.writeString("Error, topic name is required field")
				continue
			}

			// topicName := fields[1]

			// check if the partition number is given
			if len(fields) == 3 {
				nPartition, err = strconv.Atoi(fields[2])
				if nPartition == 0 {
					log.Println("Error while converting nPartition")
				}
				if err != nil {
					log.Println("Error while converting nPartition")
					client.writeString("Please try again with correct Partition number")
				}
			}
			cmd := strings.Join(fields, ",")
			ok := s.raft.Submit(cmd)
			if ok == false {
				client.writeString("This is not the leader")
				continue
			}
			<-s.taskCompChan
			client.writeString("Done")
		case "PUBS":
			if len(fields) < 3 {
				log.Print("Error while publishing message to topic")
				client.writeString("Error, message is required field")
				continue
			}
			fmt.Println("Publish")

			topicName := fields[1]
			message := NewMessage(fields[2])

			s.tm.publish(topicName, message)
			client.writeString("Done")
		case "SUBS":
			if len(fields) < 3 {
				log.Print("Error while trying to subscribe to a topic")
				client.writeString("Error, topic and consumer_group is a required field")
				continue
			}
			nPartition := 1
			var err error
			topicName := fields[1]
			groupName := fields[2]

			// check if the partition number is given
			if len(fields) == 4 {
				nPartition, err = strconv.Atoi(fields[3])
				if err != nil {
					log.Println(err)
					client.writeString("error converting partition string from user input")
					continue
				}

				if nPartition == 0 {
					client.writeString("0 is not a valid value for partition number")
					continue
				}

			}

			ids, err := s.tm.subscribe(topicName, groupName, nPartition)
			if err != nil {
				client.writeString(err.Error())
			}
			res := ResSubscriptions{Ok: true, Data: ids}
			resJSON, err := json.Marshal(res)
			if err != nil {
				client.writeString(err.Error())
			}
			s.tm.printPartitions(topicName)
			client.writeByte(resJSON)
		case "CONS":
			if len(fields) < 3 {
				log.Print("Error while trying to consume data")
				client.writeString("Error, subscriptionId and offset value are a required field")
				continue
			}
			topicName := fields[1]
			subscriptionID := fields[2]
			offsetvalue := fields[3]

			messages, _ := s.tm.consume(topicName, subscriptionID, offsetvalue)
			res := ResMessages{Ok: true, Data: messages}
			resJSON, _ := json.Marshal(res)
			client.writeByte(resJSON)
		case "DISC":
			*client.Disc <- true
		}
	}
}

// NewServer
func NewServer(addr string, tm *TopicManager, raft *raft.Raft, commitChan chan raft.Commit) *Server {
	syncOnce.Do(func() {
		conf := config.GetConfig()
		s := new(Server)
		s.addr = conf.Host + ":" + conf.Port
		s.id = conf.ID
		s.tm = tm
		s.raft = raft
		s.taskCompChan = make(chan bool)
		s.commitChan = commitChan
		brokerServerInstace = *s
		go s.listenforCommits()
	})
	return &brokerServerInstace
}
