package raft

type Log struct {
	Data interface{}
	Term int
}

// CommitEntry is the data reported by Raft to the commit channel. Each commit
// entry notifies the client that consensus was reached on a command and it can
// be applied to the client's state machine.
type Commit struct {
	Data  interface{}
	Index int
	Term  int
}

// ReqVoteArg struct to define the request structure
type ReqVoteArg struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

// ReqVoteRes determines result of request vote
type ReqVoteRes struct {
	Term        int
	VoteGranted bool
}

// AppendEntryArg defines structure of appended log entries
type AppendEntryArg struct {
	Term     int
	LeaderID string

	PrevLogIndex int
	PrevLogTerm  int
	Logs         []Log
	LeaderCommit int
}

// AppendEntryRes defines structure of appended log request response
type AppendEntryRes struct {
	Term    int
	Success bool
}
