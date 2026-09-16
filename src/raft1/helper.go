package raft

import (
	"time"

	"6.5840/raftapi"
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
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex { // 不能使用 if，防止虚假唤醒, 正常情况下不会 大于
			rf.applyCond.Wait() // 暂时释放 mu
		}
		index := rf.lastApplied + 1
		command := rf.log[index].Command
		rf.lastApplied = index
		rf.mu.Unlock()
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
		}
	}
}

func (rf *Raft) broadcastHeartbeats() {
	rf.mu.Lock()

	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	leaderTerm := rf.currentTerm
	targets := make([]int, 0, len(rf.peers)-1)

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		if rf.replicating[peer] {
			continue
		}

		rf.replicating[peer] = true
		targets = append(targets, peer)
	}

	rf.mu.Unlock()

	for _, peer := range targets {
		go rf.replicationWorker(peer, leaderTerm)
	}
}

func (rf *Raft) replicationWorker(peer int, leaderTerm int) {
	defer func() {
		rf.mu.Lock()
		rf.replicating[peer] = false
		rf.mu.Unlock()
	}()

	rf.replicateToPeer(peer, leaderTerm)
}

func (rf *Raft) replicateToPeer(peer int, leaderTerm int) {
	for {
		rf.mu.Lock()

		if rf.role != Leader || rf.currentTerm != leaderTerm {
			rf.mu.Unlock()
			return
		}

		next := rf.nextIndex[peer]
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: next - 1,
			PrevLogTerm:  rf.log[next-1].Term,
			Entries:      append([]LogEntry(nil), rf.log[next:]...),
			LeaderCommit: rf.commitIndex,
		}

		rf.mu.Unlock()

		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(peer, &args, &reply)
		if !ok { // 网络问题，不要死循环，等待下一次heartbeat
			return
		}

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			rf.resetElectionTimerLocked()
			rf.mu.Unlock()
			return
		}
		if rf.role != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		sentNext := args.PrevLogIndex + 1

		if reply.Success {
			matched := args.PrevLogIndex + len(args.Entries)
			if matched > rf.matchIndex[peer] {
				rf.matchIndex[peer] = matched
			}
			if matched+1 > rf.nextIndex[peer] {
				rf.nextIndex[peer] = matched + 1
			}

			rf.advanceCommitLocked()

			stillBehind := rf.nextIndex[peer] < len(rf.log) // 当前的复制任务会检查是否原始数据已经变化，所以不用启动新的 replication
			rf.mu.Unlock()
			if stillBehind {
				continue
			}
			return
		}

		if rf.nextIndex[peer] != sentNext { // 说明是旧值
			rf.mu.Unlock()
			return
		}
		newNext := reply.ConflictIndex
		if reply.ConflictTerm != -1 {
			lastIndexWithTerm := -1
			for index := len(rf.log) - 1; index >= 1; index-- {
				if rf.log[index].Term == reply.ConflictTerm {
					lastIndexWithTerm = index
					break
				}
			}
			if lastIndexWithTerm != -1 {
				newNext = lastIndexWithTerm + 1
			}
		}

		if newNext < 1 {
			newNext = 1
		}

		if newNext >= sentNext {
			rf.mu.Unlock()
			return
		}

		rf.nextIndex[peer] = newNext
		rf.mu.Unlock()
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
