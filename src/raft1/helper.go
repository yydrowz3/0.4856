package raft

import (
	"time"

	"6.5840/raftapi"
)

func (rf *Raft) resetElectionTimerLocked() {
	rf.lastElectionReset = time.Now()
	rf.electionTimeout = randomElectionTimeout()
}

func (rf *Raft) becomeFollowerLocked(term int) { // 同 任期 不能清空voted，否则会再次投票
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
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
		rf.broadcastHeartbeats() // 内部确认状态，这是一种常见写法，优点时状态切换简单
	}

}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for rf.pendingSnapshot == nil && rf.lastApplied >= rf.commitIndex { // 不能使用 if，防止虚假唤醒, 正常情况下不会 大于
			rf.applyCond.Wait() // 暂时释放 mu
		}
		if rf.pendingSnapshot != nil {
			msg := *rf.pendingSnapshot
			rf.pendingSnapshot = nil
			rf.mu.Unlock()

			rf.applyCh <- msg
			continue
		}

		index := rf.lastApplied + 1

		// 正常情况下不会发生，只是保护日志访问。
		if index <= rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
			rf.mu.Unlock()
			continue
		}

		command := rf.log[rf.logOffsetLocked(index)].Command
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

		// if rf.replicating[peer] {
		// 	continue
		// }
		// rf.replicating[peer] = true
		targets = append(targets, peer)
	}

	rf.mu.Unlock()

	for _, peer := range targets {
		// go rf.replicationWorker(peer, leaderTerm)
		go rf.replicateToPeer(peer, leaderTerm)
	}
}

// func (rf *Raft) replicationWorker(peer int, leaderTerm int) {
// 	defer func() {
// 		rf.mu.Lock()
// 		rf.replicating[peer] = false
// 		rf.mu.Unlock()
// 	}()

// 	rf.replicateToPeer(peer, leaderTerm)
// }

func (rf *Raft) replicateToPeer(peer int, leaderTerm int) {
	for {
		rf.mu.Lock()

		if rf.role != Leader || rf.currentTerm != leaderTerm {
			rf.mu.Unlock()
			return
		}

		next := rf.nextIndex[peer]

		if next <= rf.lastIncludedIndex { // 发现没有办法跟上的情况，会 send InstallSnapshot
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.log[0].Term, // 这是一个 dummy
				Data:              append([]byte(nil), rf.snapshot...),
			}
			rf.mu.Unlock()

			var reply InstallSnapshotReply
			ok := rf.sendInstallSnapshot(peer, &args, &reply)
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

			if rf.role != Leader || rf.currentTerm != args.Term {
				rf.mu.Unlock()
				return
			}

			if rf.matchIndex[peer] < args.LastIncludedIndex {
				rf.matchIndex[peer] = args.LastIncludedIndex
			}
			if rf.nextIndex[peer] < args.LastIncludedIndex+1 {
				rf.nextIndex[peer] = args.LastIncludedIndex + 1
			}

			rf.mu.Unlock()

			// 快照之后也要发送
			continue
		}

		prevIndex := next - 1
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  rf.termAtLocked(prevIndex),
			Entries:      append([]LogEntry(nil), rf.log[rf.logOffsetLocked(next):]...),
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

		sentNext := args.PrevLogIndex + 1 // rf.nextIndex[peer] 可能已经被其他线程修改了，所以需要保存一个副本

		if reply.Success {
			matched := args.PrevLogIndex + len(args.Entries)
			if matched > rf.matchIndex[peer] { // 等于是空心跳，小于是旧回复
				rf.matchIndex[peer] = matched
			}
			if matched+1 > rf.nextIndex[peer] {
				rf.nextIndex[peer] = matched + 1
			}

			rf.advanceCommitLocked()

			// stillBehind := rf.nextIndex[peer] < len(rf.log) // 当前的复制任务会检查是否原始数据已经变化，所以不用启动新的 replication
			// rf.mu.Unlock()
			// if stillBehind {
			// 	continue
			// }
			rf.mu.Unlock()
			return
		}

		// reply not Success
		if rf.nextIndex[peer] != sentNext { // 说明是旧值，已经被处理
			rf.mu.Unlock()
			return
		}

		newNext := reply.ConflictIndex
		// 相同索引但任期不同
		if reply.ConflictTerm != -1 {
			lastIndexWithTerm := -1
			// for index := len(rf.log) - 1; index >= 1; index--
			for index := rf.lastLogIndexLocked(); index >= rf.lastIncludedIndex; index-- { // 可以优化，因为任期是递增的，不用无限找下去
				if rf.termAtLocked(index) == reply.ConflictTerm {
					lastIndexWithTerm = index
					break
				}
			}
			if lastIndexWithTerm != -1 {
				newNext = lastIndexWithTerm + 1
			}
		}
		/*
			Leader:   [0, 1, 1, 2, 2, 3]
			Follower: [0, 1, 1, 4, 4]
															↑ 冲突
			这种情况 Follower 的任期 4 会被覆盖，因为 Leader 没找到 任期 4 的

			Leader:   [0, 1, 1, 4, 4, 5]
			Follower: [0, 1, 1, 4, 4]
															↑ 冲突
			还是从 5 开始

			Leader 如果有没有 commit 的部分，但是 退化成了 follower，那么不一致的地方会被删除
		*/

		// follower 日志太短

		// if newNext < 1 { // 应该不会发生
		// 	newNext = 1
		// }
		// if newNext >= sentNext { // 应该不会发生，因为不可能回复一个大于的值
		// 	rf.mu.Unlock()
		// 	return
		// }

		rf.nextIndex[peer] = newNext
		rf.mu.Unlock()
	}
}

func (rf *Raft) advanceCommitLocked() {
	if rf.role != Leader {
		return
	}
	majority := len(rf.peers)/2 + 1

	// commit 可能会大量落后，这是因为 复制不到多数节点

	// for index := len(rf.log) - 1; index > rf.commitIndex; index--
	for index := rf.lastLogIndexLocked(); index > rf.commitIndex; index-- { // 一次确定较多的 index，可以快速推进
		// if rf.log[index].Term != rf.currentTerm
		if rf.termAtLocked(index) != rf.currentTerm { // 实现 raft 的一个 安全策略
			continue // 就任期的日志不能在新任期中提交，但是可以通过新任期的新提交来被间接提交，防止可能会被覆盖的旧日志
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

func (rf *Raft) firstLogIndexLocked() int {
	return rf.lastIncludedIndex
	// return rf.lastIncludedIndex + 1
}

func (rf *Raft) logOffsetLocked(globalIndex int) int {
	return globalIndex - rf.lastIncludedIndex
}

func (rf *Raft) containsLogLocked(globalIndex int) bool {
	return globalIndex >= rf.firstLogIndexLocked() && globalIndex <= rf.lastLogIndexLocked()
}

func (rf *Raft) lastLogIndexLocked() int {
	// return len(rf.log) - 1
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) lastLogTermLocked() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) termAtLocked(globalIndex int) int {
	return rf.log[rf.logOffsetLocked(globalIndex)].Term
}
