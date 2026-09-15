package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command any
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	role        Role

	lastElectionReset time.Time
	electionTimeout   time.Duration

	log         []LogEntry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond

	lastLogIndex int
	lastLogTerm  int
}

const heartbeatInterval = 150 * time.Millisecond

func randomElectionTimeout() time.Duration {
	return time.Duration(400+rand.Intn(300)) * time.Millisecond
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.lastElectionReset = time.Now()
	rf.electionTimeout = randomElectionTimeout()
}

func (rf *Raft) becomeFollowerLocked(term int) { // 同 任期 不能清空voted，否则会再次投票
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	rf.role = Follower
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// var term int
	// var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.role == Leader

	// return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	// 旧任期请求
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// 新任期
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId // 防止 回复丢包
	if canVote {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimerLocked() // Optional?
	}
	reply.Term = rf.currentTerm
}

type AppendEntriesArgs struct {
	// lab3a
	Term     int
	LeaderId int
	// lab3b
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// lab3a
	Term    int
	Success bool
	// lab3b
	ConflictIndex int
	ConflictTerm  int
	ConflictLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Success = true
	rf.becomeFollowerLocked(args.Term) // 收到心跳，说明已经产生 Leader
	reply.Term = rf.currentTerm
	rf.resetElectionTimerLocked() // 一段时间没有收到心跳，开始选举
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.

		// release lock before sending RPCs

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)

		time.Sleep(10 * time.Millisecond)
		rf.startElection()
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	if rf.role == Leader || time.Since(rf.lastElectionReset) < rf.electionTimeout {
		rf.mu.Unlock()
		return
	}

	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimerLocked()

	electionTerm := rf.currentTerm
	votes := 1
	majority := len(rf.peers)/2 + 1

	args := RequestVoteArgs{
		Term:         electionTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	if votes >= majority {
		rf.role = Leader
		rf.mu.Unlock()
		rf.broadcastHeartbeats()
		return
	}

	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int, request RequestVoteArgs) {
			var reply RequestVoteReply

			ok := rf.sendRequestVote(server, &request, &reply)
			if !ok {
				return
			}

			becameLeader := false
			rf.mu.Lock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.resetElectionTimerLocked()
				rf.mu.Unlock()
				return
			}

			if rf.role == Candidate && rf.currentTerm == electionTerm && reply.Term == electionTerm && reply.VoteGranted {
				votes++
				if votes >= majority {
					rf.role = Leader
					becameLeader = true
				}
			}

			rf.mu.Unlock()
			if becameLeader {
				rf.broadcastHeartbeats()
			}

		}(peer, args)
	}
}

func (rf *Raft) heartbeatTicker() { // the leader sends heartbeats no more than 10 times per second
	for {
		time.Sleep(heartbeatInterval)
		rf.broadcastHeartbeats()
	}

}

func (rf *Raft) broadcastHeartbeats() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int, request AppendEntriesArgs) {
			var reply AppendEntriesReply

			ok := rf.sendAppendEntries(server, &request, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.resetElectionTimerLocked()
			}
		}(peer, args)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.resetElectionTimerLocked()
	// rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond

	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.lastLogIndex = 0
	rf.lastLogTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatTicker()

	return rf
}
