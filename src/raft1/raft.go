package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"bytes"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

	// replicating []bool

	lastIncludedIndex int
	snapshot          []byte

	// for Installing Snapshot
	pendingSnapshot *raftapi.ApplyMsg
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.log); err != nil {
		panic(err)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// if data == nil || len(data) < 1 { // bootstrap without any state?
	// 	return
	// }
	if len(data) < 1 {
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var lastIncludedIndex int
	var logEntries []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&logEntries) != nil {
		return
	}

	if len(logEntries) == 0 {
		return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIncludedIndex
	rf.log = logEntries
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	if index > rf.lastApplied || index > rf.lastLogIndexLocked() {
		return
	}

	snapshotTerm := rf.termAtLocked(index)
	oldOffset := rf.logOffsetLocked(index)

	newLog := make([]LogEntry, len(rf.log)-oldOffset)
	newLog[0] = LogEntry{Term: snapshotTerm}

	copy(newLog[1:], rf.log[oldOffset+1:])

	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.snapshot = append([]byte(nil), snapshot...)

	rf.persist()
	/*
		原全局日志：0 ... 9 10 11 12
		Snapshot(10)

		新日志：
		log[0] = index 10 的 dummy
		log[1] = index 11
		log[2] = index 12
	*/
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
		// rf.resetElectionTimerLocked() // 这里不能重置，重置后会有错误，可能 Term 更高 但是 日志落后
		// 选举计时器重置：
		// 1. 收到 AppendEntries 2. 确实授予投票 3. 自己开始新一轮选举
	}

	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId // 防止 回复丢包
	myLastIndex := rf.lastLogIndexLocked()
	myLastTerm := rf.lastLogTermLocked()
	candidateUpToDate := args.LastLogTerm > myLastTerm || (args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)
	if canVote && candidateUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimerLocked() // 投完票需要等待，防止 reply 没收到重试
		rf.persist()
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

	lastIndex := rf.lastLogIndexLocked()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictTerm = -1
	reply.ConflictIndex = lastIndex + 1
	reply.ConflictLen = lastIndex + 1

	// 旧任期
	if args.Term < rf.currentTerm {
		return
	}

	rf.becomeFollowerLocked(args.Term)
	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm // 成为了 follower 后，任期更新，回复的任期也要更新

	// if args.PrevLogIndex >= len(rf.log) { // follower log 太短，导致 log.[args.PrefLobIndex] 越界
	// 	reply.ConflictIndex = len(rf.log) // 从 哪一条开始 conflict
	// 	reply.ConflictLen = len(rf.log)
	// 	return
	// }

	if args.PrevLogIndex < rf.lastIncludedIndex {
		// 以及被截断了
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}

	if args.PrevLogIndex > rf.lastLogIndexLocked() {
		reply.ConflictIndex = rf.lastLogIndexLocked() + 1
		return
	}

	// 如果 不越界，开始从这个 index 的地方修改 follower 的日志，不管 follower 是否有多余的
	if rf.termAtLocked(args.PrevLogIndex) != args.PrevLogTerm { // 找到 follower 中 冲突 Term 的第一个
		conflictTerm := rf.termAtLocked(args.PrevLogIndex)
		first := args.PrevLogIndex
		for first > rf.lastIncludedIndex && rf.termAtLocked(first-1) == conflictTerm {
			first--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = first

		return
	}

	// 如果 不越界，开始从这个 index 的地方修改 follower 的日志，不管 follower 是否有多余的
	// if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // 找到 follower 中 冲突 Term 的第一个
	// 	conflictTerm := rf.log[args.PrevLogIndex].Term
	// 	first := args.PrevLogIndex
	// 	for first > 0 && rf.log[first-1].Term == conflictTerm {
	// 		first--
	// 	}
	// 	reply.ConflictTerm = conflictTerm
	// 	reply.ConflictIndex = first
	// 	reply.ConflictLen = len(rf.log)
	// 	return
	// }

	// 修改 现有 不匹配部分
	insertIndex := args.PrevLogIndex + 1
	i := 0
	logChanged := false
	for i < len(args.Entries) {
		globalIndex := insertIndex + i
		if globalIndex > rf.lastLogIndexLocked() {
			break
		}
		// if localIndex >= len(rf.log) {
		// 	break
		// }
		// if rf.log[localIndex].Term != args.Entries[i].Term { // 发现任期不匹配
		// 	rf.log = rf.log[:localIndex] // 截短
		// 	logChanged = true
		// 	break
		// }
		if rf.termAtLocked(globalIndex) != args.Entries[i].Term { // 发现任期不匹配
			offset := rf.logOffsetLocked(globalIndex)
			newLog := append([]LogEntry(nil), rf.log[:offset]...)
			rf.log = newLog
			logChanged = true
			break
		}

		i++
	}
	// 追加
	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
		logChanged = true
	}

	if args.LeaderCommit > rf.commitIndex {
		// matchedThrough := args.PrevLogIndex + len(args.Entries)
		newCommit := min(args.LeaderCommit, rf.lastLogIndexLocked())
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Broadcast()
		}
	}

	if logChanged {
		rf.persist()
	}

	reply.Success = true
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.becomeFollowerLocked(args.Term)
	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	if args.LastIncludedIndex <= rf.lastApplied {
		return
	}

	var newLog []LogEntry

	if rf.containsLogLocked(args.LastIncludedIndex) && rf.termAtLocked(args.LastIncludedIndex) == args.LastIncludedTerm {
		offset := rf.logOffsetLocked(args.LastIncludedIndex)
		suffix := rf.log[offset+1:]
		newLog = make([]LogEntry, len(suffix)+1)
		newLog[0] = LogEntry{Term: args.LastIncludedTerm}
		copy(newLog[1:], suffix)
	} else {
		newLog = []LogEntry{{Term: args.LastIncludedTerm}}
	}
	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.snapshot = append([]byte(nil), args.Data...)

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      append([]byte(nil), args.Data...),
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	if rf.pendingSnapshot != nil || rf.pendingSnapshot.SnapshotIndex < msg.SnapshotIndex {
		rf.pendingSnapshot = &msg
	}

	rf.persist()
	rf.applyCond.Broadcast()
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	// index := -1
	// term := -1
	// isLeader := true

	// return index, term, isLeader

	// Your code here (3B).

	rf.mu.Lock()
	term := rf.currentTerm

	if rf.role != Leader {
		rf.mu.Unlock()
		return -1, term, false
	}

	// index := len(rf.log)
	index := rf.lastLogIndexLocked() + 1
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})

	rf.persist()

	rf.matchIndex[rf.me] = index
	rf.advanceCommitLocked()

	rf.mu.Unlock()

	rf.broadcastHeartbeats()

	return index, term, true
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

	// persist state before sending RPCs, to avoid losing votes if we crash
	rf.persist()

	electionTerm := rf.currentTerm
	votes := 1
	majority := len(rf.peers)/2 + 1

	args := RequestVoteArgs{
		Term:         electionTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndexLocked(),
		LastLogTerm:  rf.lastLogTermLocked(),
	}

	if votes >= majority {
		rf.becomeLeaderLocked()
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
					rf.becomeLeaderLocked()
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

	rf.lastIncludedIndex = 0
	rf.log = []LogEntry{{Term: 0}}
	rf.snapshot = nil
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// rf.replicating = make([]bool, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	for peer := range peers {
		rf.nextIndex[peer] = rf.lastLogIndexLocked() + 1 // 自己的日志中，该 peer 的下一个日志的索引
		rf.matchIndex[peer] = rf.lastIncludedIndex
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatTicker()
	go rf.applier()

	return rf
}
