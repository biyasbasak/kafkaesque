package raft

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	//"github.com/gwDistSys20/project-kafkaesque/broker"

	"github.com/gwDistSys20/project-kafkaesque/config"
	"github.com/thoas/go-funk"
)

// RaftServer to wrap a simple raft server
var syncOnce sync.Once
var raftServerInstance *RaftServer

// RaftServer defines a raft server
type RaftServer struct {
	mutex       sync.Mutex
	peerClients map[string]*rpc.Client
}

func (s *RaftServer) Call(peer string, service string, args interface{}, res interface{}) error {
	peerClient, ok := s.peerClients[peer]
	if ok == false {
		log.Println("Peer not found ", peer)
		panic(s.peerClients)
	}
	peerClient.Call(service, args, res)
	return nil
}

//NewServer registers server with rpc and returns it
func NewServer() *RaftServer {
	syncOnce.Do((func() {
		conf := config.GetConfig()
		s := &RaftServer{peerClients: make(map[string]*rpc.Client)}
		fmt.Println("Raft Server", conf.RaftPort)
		// rpc.Register(s)
		rpc.HandleHTTP()
		l, err := net.Listen("tcp", ":"+conf.RaftPort)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Raft Server running on port: ", conf.RaftPort)
		go http.Serve(l, nil)
		raftServerInstance = s
	}))
	return raftServerInstance
}

func GetServerInstance() *RaftServer {
	return raftServerInstance
}

func updateServerConnectedPeers(peer string) *RaftServer {
	client, err := rpc.DialHTTP("tcp", "127.0.0.1"+":"+peer)
	if err != nil {
		log.Fatal(err)
	}
	raftServerInstance.mutex.Lock()
	raftServerInstance.peerClients[peer] = client
	raftServerInstance.mutex.Unlock()
	return raftServerInstance
}

func ContactPeers() bool {
	conf := config.GetConfig()
	var connectedPeers []string
	var quoramMet = false
	var quorumReq = (len(conf.Peers) + 1) / 2 // change this later to majority
	// tick := time.NewTicker(2 * time.Second)
	// blocking call. Either majority is met or timeout after 1 mins
	for {
		if quoramMet == true {
			break
		}
		// select {
		// case <-time.After(1 * time.Minute):
		// 	panic("Broker was not able to connect with a majority")
		// case <-tick.C:
		for _, peer := range conf.Peers {
			if peer == conf.Me {
				continue
			}

			client, err := rpc.DialHTTP("tcp", "127.0.0.1"+":"+peer)
			if err != nil {
				log.Println("Waiting for replica broker to join at port: ", peer)
				time.Sleep(1 * time.Second)
				continue
			}
			if funk.ContainsString(connectedPeers, peer) {
				client.Close()
				continue
			}
			updateServerConnectedPeers(peer)

			connectedPeers = append(connectedPeers, peer)
			client.Close()
		}
		if len(connectedPeers)+1 >= quorumReq {
			fmt.Println("contact peers quoram condition met")
			quoramMet = true
		}
		// }
	}
	conf.ConnectedPeers = connectedPeers
	return quoramMet
}

func RegisterService(serviceName string, raft *Raft) {
	rpc.RegisterName(serviceName, raft)
}

// DisconnectAll closes all the client connections to peers for this server.
// func (s *RaftServer) DisconnectAll() {
// 	s.mutex.Lock()
// 	defer s.mutex.Unlock()
// 	for id := range s.peerClients {
// 		if s.peerClients[id] != nil {
// 			s.peerClients[id].Close()
// 			s.peerClients[id] = nil
// 		}
// 	}
// }

// Shutdown closes the server and waits for it to shut down properly.
// func (s *RaftServer) Shutdown() {
// 	s.r.Stop()
// 	close(s.quit)
// 	s.listener.Close()
// 	s.wg.Wait()
// }

// // GetListenAddr - to get listener address
// func (s *RaftServer) GetListenAddr() net.Addr {
// 	s.mutex.Lock()
// 	defer s.mutex.Unlock()
// 	return s.listener.Addr()
// }

// // ConnectToPeer - to connect to peer
// func (s *RaftServer) ConnectToPeer(peer string, addr net.Addr) error {
// 	s.mutex.Lock()
// 	defer s.mutex.Unlock()
// 	if s.peerClients[peer] == nil {
// 		client, err := rpc.Dial(addr.Network(), addr.String())
// 		if err != nil {
// 			return err
// 		}
// 		s.peerClients[peer] = client
// 	}
// 	return nil
// }

// DisconnectPeer disconnects this server from the peer identified by peerId.
// func (s *RaftServer) DisconnectPeer(peer string) error {
// 	s.mutex.Lock()
// 	defer s.mutex.Unlock()
// 	if s.peerClients[peer] != nil {
// 		err := s.peerClients[peer].Close()
// 		s.peerClients[peer] = nil
// 		return err
// 	}
// 	return nil
// }
