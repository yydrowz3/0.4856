package raft

import (
	"time"
)

func (rf *Raft) resetElectionTimerLocked() {
	rf.lastElectionReset = time.Now()
	rf.electionTimeout = randomElectionTimeout()
}

func (rf *Raft) lastLogIndexLocked() int {
	return len(rf.log) - 1
}

func (rf *Raft) lastLogTermLocked() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) becomeFollowerLocked(term int) { // 同 任期 不能清空voted，否则会再次投票
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	rf.role = Follower
}

func (rf *Raft) becomeLeaderLocked() {
	rf.role = Leader

	lastIndex := rf.lastLogIndexLocked()

	for peer := range rf.peers {
		rf.nextIndex[peer] = lastIndex + 1
		rf.matchIndex[peer] = 0 // 成为 Leader 后需要重新 确认 match
	}

	rf.matchIndex[rf.me] = lastIndex
}

func (rf *Raft) heartbeatTicker() { // the leader sends heartbeats no more than 10 times per second
	for {
		time.Sleep(heartbeatInterval)
		rf.broadcastHeartbeats()
	}

}

func (rf *Raft) applier() {
	for {
		// if rf.lastApplied >= rf.commitIndex {
		// 	time.Sleep(10 * time.Millisecond)
		// 	continue
		// }
		// rf.mu.Lock()
		// next := rf.lastApplied + 1
		// entryCommand := rf.log[next].Command
		// rf.lastApplied = next
		// rf.mu.Unlock()
		// rf.applyCh <- raftapi.ApplyMsg{
		// 	CommandValid: true,
		// 	Command:      entryCommand,
		// 	CommandIndex: next,
		// }
	}

}

func (rf *Raft) broadcastHeartbeats() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	// args := AppendEntriesArgs{
	// 	Term:     rf.currentTerm,
	// 	LeaderId: rf.me,
	// }
	args := make([]AppendEntriesArgs, len(rf.peers))
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		next := rf.nextIndex[peer]
		args[peer] = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: next - 1,
			PrevLogTerm:  rf.log[next-1].Term,
			// Entries:      rf.log[next:], // unsafe，会共用底层数组
			Entries:      append([]LogEntry(nil), rf.log[next:]...), // 用于复制切片，避免修改副本时影响
			LeaderCommit: rf.commitIndex,
		}
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

			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.resetElectionTimerLocked()
				rf.mu.Unlock()
				return
			}
			if rf.role != Leader || rf.currentTerm != request.Term {
				rf.mu.Unlock()
				return
			}

			if reply.Success {
				matched := request.PrevLogIndex + len(request.Entries)
				if matched > rf.matchIndex[server] {
					rf.matchIndex[server] = matched
				}
				if matched+1 > rf.nextIndex[server] {
					rf.nextIndex[server] = matched + 1
				}
				rf.advanceCommitLocked()
				rf.mu.Unlock()
				return
			}

			sentNext := request.PrevLogIndex + 1

		}(peer, args[peer])
	}
}

func (rf *Raft) advanceCommitLocked() {
	if rf.role != Leader {
		return
	}
	majority := len(rf.peers)/2 + 1

	for index := len(rf.log) - 1; index > rf.commitIndex; index-- {
		if rf.log[index].Term != rf.currentTerm { // TODO: 怎么理解？
			continue
		}
		count := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			if rf.matchIndex[peer] >= index {
				count++
			}
		}
		if count >= majority {
			rf.commitIndex = index
			rf.applyCond.Broadcast()
			return
		}
	}
}
