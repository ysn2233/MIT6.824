package raft

import (
	"sync"
	"6.824/src/labrpc"
	"time"
	"math/rand"
	"log"
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

const  (
	FOLLOWER = iota
	CANDIDATE
	LEADER

	HB_INTERVAL = 50 * time.Millisecond
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
	mu        	sync.Mutex          // Lock to protect shared access to this peer's state
	peers     	[]*labrpc.ClientEnd // RPC end points of all peers
	persister 	*Persister          // Object to hold this peer's persisted state
	me        	int                 // this peer's index into peers[]

	// Persistant state on all servers
	state		int
	currentTerm	int
	votedFor	int
	log		[]LogEntry

	// Voliate state on all servers
	commitIndex	int
	lastApplied	int

	// Voliate state on leaders
	votedCount	int
	nextIndex	[]int
	matchIndex	[]int

	// Some channel to control the system
	chanVote	chan bool
	chanWinLeader	chan bool
	chanHb		chan bool
	chanCommit	chan bool
	chanApply	chan ApplyMsg

}

func (rf *Raft) LastLogIndex() int {
	return len(rf.log) - 1
}


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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term		int
	CandidateId 	int
	LastLogIndex 	int
	LastLogTerm 	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term 		int
	VoteGranted 	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.state = FOLLOWER
	}


	logUpToDate := false
	if args.LastLogTerm > rf.LastLogTerm() {
		logUpToDate = true
	} else if args.LastLogTerm == rf.LastLogTerm() && args.LastLogIndex >= rf.LastLogIndex() {
		logUpToDate = true
	} else {
		logUpToDate = false
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId ) && logUpToDate {
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


func (rf *Raft) BroadcastRequestVote() {
	args := &RequestVoteArgs{
		Term:		rf.currentTerm,
		CandidateId:	rf.me,
		LastLogIndex:	rf.LastLogIndex(),
		LastLogTerm:	rf.LastLogTerm(),
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
					if rf.votedCount * 2 > len(rf.peers) {
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
	Term 		int
	LeaderId 	int
	PrevLogIndex 	int
	PrevLogTerm 	int
	Entries 	[]LogEntry
	LeaderCommit 	int
}


type AppendEntriesReply struct {
	Term 		int
	Success 	bool
	UpdatedNextId	int
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success =  false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.UpdatedNextId = rf.LastLogIndex() + 1
		log.Printf("Server %v's term > Server %v\n", rf.me, args.LeaderId)
		return
	}

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
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex-1; i >= baseIndex; i-- {
				if term != rf.log[i-baseIndex].Term {
					reply.UpdatedNextId = i + 1
					return
				}
			}
		}
	}

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
}


func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


func (rf *Raft) BroadcastAppendEntries() {
	if rf.state != LEADER {
		return
	}

	baseIndex := rf.log[0].Index
	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {
			if rf.nextIndex[i] > baseIndex {
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
				args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex + 1-baseIndex:]))
				copy(args.Entries, rf.log[args.PrevLogIndex + 1-baseIndex:])
				go func(server int, args AppendEntriesArgs) {
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
							log.Printf("Server %v become follower\n", rf.me)
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
					}
				}(i, args)
			}
		}
	}
}


func (rf *Raft) FollowerWork() {
	select {
	case <- rf.chanHb:
	case <- rf.chanVote:
	case <- time.After(time.Duration(rand.Intn(350) + 500) * time.Millisecond):
		rf.state = CANDIDATE
	}
}


func (rf *Raft) CandidateWork() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votedCount = 1
	go rf.BroadcastRequestVote()
	select {
	case <- time.After(time.Duration(rand.Intn(350) + 500) * time.Millisecond):
	case <- rf.chanHb:
		rf.state = FOLLOWER
	case <- rf.chanWinLeader:
		rf.state = LEADER
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = rf.LastLogIndex() + 1
			rf.matchIndex[i] = 0
		}
		log.Printf("Server %v become the leader, now term is %v\n", rf.me, rf.currentTerm)
	}

}


func (rf *Raft) LeaderCommit() {
	baseIndex := rf.log[0].Index
	waitingCommit := rf.commitIndex
	for i:=rf.commitIndex + 1; i<len(rf.log); i++ {
		count := 1
		for j:= range rf.peers {
			//log.Printf("%v %v %v\n", j, i, rf.matchIndex[j])
			if j == rf.me {
				continue
			} else if rf.matchIndex[j] >= i && rf.log[i-baseIndex].Term == rf.currentTerm {
				count++
			}
		}
		if 2 * count > len(rf.peers) {
			waitingCommit = i
			break
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
		case <- rf.chanCommit:
			if rf.commitIndex > rf.lastApplied {
				baseIndex := rf.log[0].Index
				for i := rf.lastApplied + 1; i<= rf.commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command:rf.log[i-baseIndex].Cmd}
					rf.chanApply <- msg
					rf.lastApplied = i
					//log.Printf("Server %v Apply %v, Term: %v\n", rf.me, msg, rf.currentTerm)
				}
			}
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
	rf.log = append(rf.log, LogEntry{Term:0, Index:0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.chanVote = make(chan bool)
	rf.chanWinLeader = make(chan bool)
	rf.chanHb = make(chan bool)
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
	}


	return index, term, isLeader
}


func (rf *Raft) Kill() {
	// Your code here, if desired.
}