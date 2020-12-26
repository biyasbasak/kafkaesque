package main

import (
	"flag"

	"github.com/gwDistSys20/project-kafkaesque/broker"
	"github.com/gwDistSys20/project-kafkaesque/config"
	"github.com/gwDistSys20/project-kafkaesque/filemanager"
	"github.com/gwDistSys20/project-kafkaesque/raft"
	"github.com/lithammer/shortuuid/v3"
)

func parseCommandLineArgs() map[string]string {
	values := make(map[string]string)
	brokerIDPtr := flag.String("brokerId", shortuuid.New(), "ID of the broker")
	portPtr := flag.String("port", "8080", "Port the broker will run on")
	raftPtr := flag.String("raftport", "7070", "Port Raft will run on")
	flag.Parse()
	values["id"] = *brokerIDPtr
	values["port"] = *portPtr
	values["raftPort"] = *raftPtr
	//fmt.Print("\n Port set:",values["port"])
	//fmt.Print("\n Raft port set:",values["raftPort"])
	return values
}

func main() {
	values := parseCommandLineArgs()
	config.NewConfig(values)

	tm := broker.NewTopicManager()
	filemanager.NewFileManager()

	// notify when to start an election
	startElection := make(chan bool)
	commitChan := make(chan raft.Commit)

	// raft server handling all the rpc communication
	raftServer := raft.NewServer()

	serviceName := "Raft"
	// instance of Raft
	r := raft.NewRaft(values["id"], serviceName, raftServer, commitChan, startElection)

	// register raft instance on the raft rpc server
	raft.RegisterService(serviceName, r)

	// contact peers to check if majority is available
	quoramMet := raft.ContactPeers()

	if quoramMet == true {
		startElection <- true
	} else {
		panic("failed to form a majority")
	}
	b := broker.NewServer(":8080", tm, r, commitChan)
	// start the broker
	b.Start()
}
