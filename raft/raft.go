package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type state string

const (
	leader    state = "leader"
	follower  state = "follower"
	candidate state = "candidate"
)

// Raft struct
type Raft struct {
	mutex              sync.Mutex
	Server             *RaftServer
	id                 string
	state              state
	term               int
	votedFor           string
	electionTimeout    time.Duration
	electionResetEvent time.Time
	// service names
	appendEntriesService string
	reqVoteService       string
	// log replication
	logs               []Log
	commitChan         chan<- Commit
	newCommitReadyChan chan struct{}
	commitIndex        int
	lastApplied        int
	nextIndex          map[string]int
	matchIndex         map[string]int
}

// PrintCurrentState is for debugging purposes
func (r *Raft) PrintCurrentState() {
	fmt.Println(r.id, "is ", r.state, "and in term ", r.term)
	fmt.Println(r.logs)
}

func (r *Raft) GetState() state {
	r.mutex.Lock()
	s := r.state
	r.mutex.Unlock()
	return s
}

func (r *Raft) commitChanSender() {
	for range r.newCommitReadyChan {
		// Find which entries we have to apply.

		r.mutex.Lock()
		savedTerm := r.term
		savedLastApplied := r.lastApplied
		var logs []Log
		// all the logs that have not been committed
		if r.commitIndex > r.lastApplied {
			logs = r.logs[r.lastApplied+1 : r.commitIndex+1]
			r.lastApplied = r.commitIndex
		}
		r.mutex.Unlock()
		// log.Printf("commitChanSender entries=%v, savedLastApplied=%d", logs, savedLastApplied)

		for i, log := range logs {
			fmt.Println("commitsender chan log called")
			r.commitChan <- Commit{
				Data:  log.Data,
				Index: savedLastApplied + i + 1,
				Term:  savedTerm,
			}
		}
	}
}

// Submit is used to submit any data to raft for replication
// returns false if the current raft is not the leader
func (r *Raft) Submit(data interface{}) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// log.Printf("Submit received by %v: %v", r.state, data)
	if r.state == leader {
		r.logs = append(r.logs, Log{Data: data, Term: r.term})
		// fmt.Println(r.logs)
		// log.Printf("... log=%v", r.logs)
		return true
	}
	return false
}

// runElectionTimer runs a timer in background to start a new election
func (r *Raft) runElectionTimer() {
	electionTimeout := r.electionTimeout
	r.mutex.Lock()
	currentTerm := r.term
	r.mutex.Unlock()
	log.Println("timer started for", r.id, electionTimeout, currentTerm)
	ticker := time.NewTicker(10 * time.Millisecond)

	defer ticker.Stop()
	for {
		<-ticker.C
		r.mutex.Lock()
		// return if the current raft instance is a leader
		if r.state != candidate && r.state != follower {
			log.Print(r.id, "is the leader")
			r.mutex.Unlock()
			return
		}
		if currentTerm != r.term {
			log.Printf("in election timer term changed from %d to %d, bailing out", currentTerm, r.term)
			r.mutex.Unlock()
			return
		}
		elapsed := time.Since(r.electionResetEvent)
		if elapsed >= electionTimeout {
			log.Print("Did not hear from leader, started election")
			r.startElection()
			r.mutex.Unlock()
			return
		}
		r.mutex.Unlock()
	}
}

// startElection to start a new election within current term
func (r *Raft) startElection() {
	fmt.Println("start election received. starting election")
	// server := GetServerInstance()
	// peers := config.GetConfig().ConnectedPeers
	r.state = candidate
	r.term++
	currentTerm := r.term
	r.electionResetEvent = time.Now()
	r.votedFor = r.id
	log.Printf("becomes Candidate (currentTerm=%d);", currentTerm)

	// use go atomics to update this value concurrently
	// avoid race condition
	var totalVotes int32 = 1

	for peer := range r.Server.peerClients {
		go func(peer string) {
			r.mutex.Lock()
			savedLastLogIndex, savedLastLogTerm := r.lastLogIndexAndTerm()
			r.mutex.Unlock()

			arg := ReqVoteArg{
				Term:         currentTerm,
				CandidateID:  r.id,
				LastLogIndex: savedLastLogIndex, // log replication
				LastLogTerm:  savedLastLogTerm,  // log replication
			}
			var res ReqVoteRes

			log.Println("sending RequestVote ", peer, arg)
			if err := r.Server.Call(peer, r.reqVoteService, arg, &res); err == nil {
				r.mutex.Lock()
				defer r.mutex.Unlock()
				log.Printf("received RequestVoteReply %+v\n", res)

				if r.state != candidate {
					log.Printf("while waiting for reply, state = %v\n", r.state)
					return
				}

				if res.Term > currentTerm {
					log.Println("term out of date in reply")
					r.changeStateToFollower(res.Term)
					return
				} else if res.Term == currentTerm {
					if res.VoteGranted {
						votes := int(atomic.AddInt32(&totalVotes, 1))
						if votes*2 > len(r.Server.peerClients)+1 {
							log.Printf("wins election with %d votes\n", votes)
							r.changeStateToLeader()
							return
						}
					}
				}
			}
		}(peer)
	}
	go r.runElectionTimer()
}

// RequestVote RPC.
func (r *Raft) RequestVote(args ReqVoteArg, reply *ReqVoteRes) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	lastLogIndex, lastLogTerm := r.lastLogIndexAndTerm() // log replication

	log.Printf("RequestVote: %+v [currentTerm=%d, votedFor=%v, log index/term=(%d, %d)]", args, r.term, r.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > r.term {
		log.Println("... term out of date in RequestVote")
		r.changeStateToFollower(args.Term)
	}

	if r.term == args.Term &&
		(r.votedFor == "" || r.votedFor == args.CandidateID) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		r.votedFor = args.CandidateID
		r.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = r.term
	log.Printf("... RequestVote reply: %+v", reply)
	return nil
}

// AppendEntries to check the append entry logs sent by leaders
func (r *Raft) AppendEntries(args AppendEntryArg, reply *AppendEntryRes) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if args.Term > r.term {
		log.Println("... term out of date in AppendEntries")
		r.changeStateToFollower(args.Term)
	}

	reply.Success = false
	if args.Term == r.term {
		if r.state != follower {
			r.changeStateToFollower(args.Term)
		}
		r.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(r.logs) && args.PrevLogTerm == r.logs[args.PrevLogIndex].Term) {
			reply.Success = true

			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(r.logs) || newEntriesIndex >= len(args.Logs) {
					break
				}
				if r.logs[logInsertIndex].Term != args.Logs[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			if newEntriesIndex < len(args.Logs) {
				// log.Printf("... inserting entries %v from index %d", args.Logs[newEntriesIndex:], logInsertIndex)
				r.logs = append(r.logs[:logInsertIndex], args.Logs[newEntriesIndex:]...)
				// log.Printf("... log is now: %v", r.logs)
			}

			// Set commit index.
			if args.LeaderCommit > r.commitIndex {
				if args.LeaderCommit < len(r.logs)-1 {
					r.commitIndex = args.LeaderCommit
				} else {
					r.commitIndex = len(r.logs) - 1
				}
				// cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
				log.Printf("Setting commitIndex=%d", r.commitIndex)
				r.newCommitReadyChan <- struct{}{}
			}
		}
	}

	reply.Term = r.term
	// log.Printf("AppendEntries reply: %+v", *reply)
	return nil
}

//changeStateToFollower makes r a follower and resets its state.
func (r *Raft) changeStateToFollower(term int) {
	log.Printf("becomes Follower with term=%d;", term)
	r.state = follower
	r.term = term
	r.votedFor = ""
	r.electionResetEvent = time.Now()

	go r.runElectionTimer()
}

// changeStateToLeader switches r into a leader state and begins process of heartbeats.
func (r *Raft) changeStateToLeader() {
	r.state = leader

	for peer := range r.Server.peerClients {
		r.nextIndex[peer] = len(r.logs)
		r.matchIndex[peer] = -1
	}
	log.Printf("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", r.term, r.nextIndex, r.matchIndex, r.logs)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// Send periodic heartbeats, as long as still leader.
		for {
			r.leaderSendHeartbeats()
			<-ticker.C

			r.mutex.Lock()
			if r.state != leader {

				r.mutex.Unlock()
				return
			}
			r.mutex.Unlock()
		}
	}()

}

// leaderSendHeartbeats sends a round of heartbeats to all peers, collects their
// replies and adjusts cm's state.
func (r *Raft) leaderSendHeartbeats() {
	r.mutex.Lock()
	savedCurrentTerm := r.term
	r.mutex.Unlock()

	// Log replication code
	for peer := range r.Server.peerClients {
		go func(peer string) {
			r.mutex.Lock()
			ni := r.nextIndex[peer]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = r.logs[prevLogIndex].Term
			}
			logs := r.logs[ni:]
			args := AppendEntryArg{
				Term:     savedCurrentTerm,
				LeaderID: r.id,
				// log replication
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Logs:         logs,
				LeaderCommit: r.commitIndex,
			}
			r.mutex.Unlock()
			// log.Printf("Leader sending AppendEntries to %v: ni=%d, args=%+v", peer, 0, args)
			var reply AppendEntryRes
			if err := r.Server.Call(peer, r.appendEntriesService, args, &reply); err == nil {
				r.mutex.Lock()
				defer r.mutex.Unlock()
				if reply.Term > savedCurrentTerm {
					log.Printf("term out of date in heartbeat reply")
					r.changeStateToFollower(reply.Term)
					return
				}

				if r.state == leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						r.nextIndex[peer] = ni + len(logs)
						r.matchIndex[peer] = r.nextIndex[peer] - 1
						// log.Printf("AppendEntries reply from %s success: nextIndex := %v, matchIndex := %v", peer, r.nextIndex, r.matchIndex)

						savedCommitIndex := r.commitIndex
						for i := r.commitIndex + 1; i < len(r.logs); i++ {
							if r.logs[i].Term == r.term {
								matchCount := 1
								for peer := range r.Server.peerClients {
									if r.matchIndex[peer] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(r.Server.peerClients)+1 {
									r.commitIndex = i
								}
							}
						}
						if r.commitIndex != savedCommitIndex {
							log.Printf("leader sets commitIndex := %d", r.commitIndex)
							r.newCommitReadyChan <- struct{}{}
						}
					} else {
						r.nextIndex[peer] = ni - 1
						// log.Printf("AppendEntries reply from %s !success: nextIndex := %d", peer, ni-1)
					}
				}
			}
		}(peer)
	}
}

// lastLogIndexAndTerm returns the last log index and the last log entry's term
// (or -1 if there's no log) for this server.
// Expects cm.mu to be locked. (Log Replication code)
func (r *Raft) lastLogIndexAndTerm() (int, int) {
	if len(r.logs) > 0 {
		lastIndex := len(r.logs) - 1
		return lastIndex, r.logs[lastIndex].Term
	}

	return -1, -1
}

// NewRaft returns a new instance of raft
func NewRaft(id string, serviceName string, raftServer *RaftServer, commitChan chan<- Commit, startElection <-chan bool) *Raft {

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	r := new(Raft)
	r.id = id
	r.Server = raftServer
	// r.peers = conf.Peers
	r.state = follower
	r.votedFor = ""
	r.appendEntriesService = serviceName + "." + "AppendEntries"
	r.reqVoteService = serviceName + "." + "RequestVote"
	r.commitChan = commitChan
	// generates random time between 150-300 millisecond
	r.electionTimeout = time.Duration(150+r1.Intn(150)) * time.Millisecond

	// Log replication
	r.newCommitReadyChan = make(chan struct{}, 10)
	r.commitIndex = -1
	r.lastApplied = -1
	r.nextIndex = make(map[string]int)
	r.matchIndex = make(map[string]int)

	go func() {
		<-startElection
		// if election timer has already been triggered return
		if r.state == follower && r.votedFor != "" {
			return
		}
		r.mutex.Lock()
		r.electionResetEvent = time.Now()
		r.mutex.Unlock()
		r.runElectionTimer()
	}()

	// Log replication
	go r.commitChanSender()
	return r
}
