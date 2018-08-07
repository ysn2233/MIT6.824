package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER

	HB_INTERVAL = 75 * time.Millisecond
)

type LogEntry struct {
	Term  int
	Cmd   interface{}
	Index int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	state       int        // Peer's current state(Follower, Candidate, Leader)
	currentTerm int        // Peer's current term
	votedFor    int        // The candidate this peer voted for
	log         []LogEntry // Logs on this peer

	commitIndex int // Current index of log this peer has committed
	lastApplied int // Last command this server has applied

	votedCount int   // Vote candidate has received
	nextIndex  []int // For each server, next log entry to send to
	matchIndex []int // For each server, index of highest log entry known to be replicated

	chanVote      chan bool     // channel for getting vote
	chanWinLeader chan bool     // channel for winning the leader election
	chanHb        chan bool     // channel for heartbeating
	chanCommit    chan bool     // channel for commiting msg
	chanApply     chan ApplyMsg // channel for applying msg
}

// Return the last log index on this peer
func (rf *Raft) LastLogIndex() int {
	return len(rf.log) - 1
}

// Return the term of last log entry on this peer
func (rf *Raft) LastLogTerm() int {
	return rf.log[rf.LastLogIndex()].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

// RequestVote RPC args
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC Reply
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false

	// Reply false if term < current term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// Update currentTerm and vote for the candidate
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.state = FOLLOWER
	}

	// Justify if candidate's term is up to date
	logUpToDate := false
	if args.LastLogTerm > rf.LastLogTerm() {
		logUpToDate = true
	} else if args.LastLogTerm == rf.LastLogTerm() && args.LastLogIndex >= rf.LastLogIndex() {
		logUpToDate = true
	} else {
		logUpToDate = false
	}

	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receivers'log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.chanVote <- true
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Brodacast request vote to other peers to get vote
func (rf *Raft) BroadcastRequestVote() {
	defer rf.persist()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.LastLogIndex(),
		LastLogTerm:  rf.LastLogTerm(),
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, args *RequestVoteArgs) {
			var reply RequestVoteReply
			if rf.state != CANDIDATE {
				return
			}
			if rf.SendRequestVote(server, args, &reply) {
				if rf.currentTerm != reply.Term {
					return
				}
				if reply.VoteGranted {
					rf.votedCount++
					if rf.state == CANDIDATE && rf.votedCount*2 > len(rf.peers) {
						// If one has won leader, prevent later goroutine write chanWinLeader
						rf.state = FOLLOWER
						rf.chanWinLeader <- true
					}
				} else {
					if rf.currentTerm < reply.Term {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.mu.Unlock()
					}
				}
			} else {
				//log.Printf("Leader %v failed to communicate with peer %v\n", rf.me, server)
			}
		}(i, args)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	UpdatedNextId int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.UpdatedNextId = rf.LastLogIndex() + 1
		return
	}

	// Periodically heartbeat
	rf.chanHb <- true

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	if args.PrevLogIndex > rf.LastLogIndex() {
		reply.UpdatedNextId = rf.LastLogIndex() + 1
		return
	}

	baseIndex := rf.log[0].Index
	if args.PrevLogIndex > baseIndex {
		term := rf.log[args.PrevLogIndex].Term
		// Set nextIndex to the last index of last different term
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				if term != rf.log[i-baseIndex].Term {
					reply.UpdatedNextId = i + 1
					return
				}
			}
		}
	}

	// Failure case is at above
	reply.Success = true
	if args.Entries != nil {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		reply.UpdatedNextId = rf.LastLogIndex() + 1
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= rf.LastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.LastLogIndex()
		}
		rf.chanCommit <- true
	}
	reply.UpdatedNextId = rf.LastLogIndex() + 1
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) BroadcastAppendEntries() {
	defer rf.persist()
	if rf.state != LEADER {
		return
	}

	baseIndex := rf.log[0].Index
	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {
			if rf.nextIndex[i] > baseIndex {
				if rf.nextIndex[i] > rf.LastLogIndex() {
					rf.nextIndex[i] = rf.LastLogIndex() + 1
				}
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = rf.nextIndex[i] - 1
				//log.Printf("server %v, nextid: %v\n", i, rf.nextIndex[i])
				//log.Printf("server: %v, prelog: %v, base: %v, loglen:%v\n",i, args.PrevLogIndex, baseIndex, len(rf.log))
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
				args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1-baseIndex:]))
				copy(args.Entries, rf.log[args.PrevLogIndex+1-baseIndex:])
				go func(server int, args AppendEntriesArgs) {
					defer rf.persist()
					var reply AppendEntriesReply
					if rf.SendAppendEntries(server, &args, &reply) {
						if rf.state != LEADER {
							return
						}
						if args.Term != rf.currentTerm {
							return
						}
						if rf.currentTerm < reply.Term {
							rf.mu.Lock()
							rf.currentTerm = reply.Term
							rf.state = FOLLOWER
							rf.votedFor = -1
							rf.mu.Unlock()
							return
						}
						if reply.Success {
							if len(args.Entries) > 0 {
								rf.nextIndex[server] += len(args.Entries)
								rf.matchIndex[server] = rf.nextIndex[server] - 1
							}
						} else {
							rf.nextIndex[server] = reply.UpdatedNextId
						}
					} else {
					}
				}(i, args)
			}
		}
	}
}

func (rf *Raft) FollowerWork() {
	select {
	case <-rf.chanHb:
	case <-rf.chanVote:
	case <-time.After(time.Duration(rand.Intn(300)+500) * time.Millisecond):
		rf.state = CANDIDATE
	}
}

func (rf *Raft) CandidateWork() {
	defer rf.persist()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.votedCount = 1
	go rf.BroadcastRequestVote()
	select {
	case <-time.After(time.Duration(rand.Intn(300)+500) * time.Millisecond):
	case <-rf.chanHb:
		rf.state = FOLLOWER
	case <-rf.chanWinLeader:
		rf.mu.Lock()
		rf.state = LEADER
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = rf.LastLogIndex() + 1
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) LeaderCommit() {
	baseIndex := rf.log[0].Index
	waitingCommit := rf.commitIndex
	for i := rf.commitIndex + 1; i < len(rf.log); i++ {
		count := 1
		for j := range rf.peers {
			if j == rf.me {
				continue
			} else if rf.matchIndex[j] >= i && rf.log[i-baseIndex].Term == rf.currentTerm {
				count++
			}
		}
		if 2*count > len(rf.peers) {
			waitingCommit = i
		}
	}
	if waitingCommit != rf.commitIndex {
		rf.commitIndex = waitingCommit
		rf.chanCommit <- true
	}
}

func (rf *Raft) LeaderWork() {
	time.Sleep(HB_INTERVAL)
	rf.LeaderCommit()
	go rf.BroadcastAppendEntries()
}

func (rf *Raft) ApplyCommittedLog() {
	for {
		select {
		case <-rf.chanCommit:
			rf.mu.Lock()
			if rf.commitIndex > rf.lastApplied {
				baseIndex := rf.log[0].Index
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.log[i-baseIndex].Cmd}
					rf.chanApply <- msg
					rf.lastApplied = i
				}
			}
			rf.mu.Unlock()
		}
	}

}

func (rf *Raft) MainLoop() {
	for {
		switch rf.state {
		case FOLLOWER:
			rf.FollowerWork()
		case CANDIDATE:
			rf.CandidateWork()
		case LEADER:
			rf.LeaderWork()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.chanVote = make(chan bool)
	rf.chanWinLeader = make(chan bool)
	rf.chanHb = make(chan bool, 100)
	rf.chanCommit = make(chan bool)
	rf.chanApply = applyCh

	rf.votedCount = 1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.MainLoop()
	go rf.ApplyCommittedLog()
	return rf
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term, isLeader := rf.GetState()
	if isLeader {
		index = rf.LastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{Term: term, Cmd: command})
		rf.persist()
	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {

}
