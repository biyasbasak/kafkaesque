package config

import (
	"os/user"
	"path"
	"sync"
)

var once sync.Once

var mutex sync.Mutex

var instance config

type config struct {
	ID             string
	BrokerDir      string
	Host           string
	Port           string
	Peers          []string
	ConnectedPeers []string
	RaftPort       string
	Me             string
	MaxFileLines   int
}

func NewConfig(values map[string]string) config {
	once.Do(func() {
		id := values["id"]
		usr, _ := user.Current()
		instance.ID = id
		instance.BrokerDir = path.Join(usr.HomeDir, "kafkaesque-"+id)
		instance.Host = "127.0.0.1"
		instance.Port = values["port"]
		instance.RaftPort = values["raftPort"]
		instance.Peers = []string{"7070", "7071", "7072"}
		instance.Me = values["raftPort"]
		instance.MaxFileLines = 200
	})
	return instance

}

func GetConfig() config {
	return instance
}
