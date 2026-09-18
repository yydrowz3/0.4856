package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type applyResult struct {
	op    Op
	value any
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	nextID  int
	waiters map[int]chan applyResult
	done    chan struct{}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waiters:      make(map[int]chan applyResult),
		done:         make(chan struct{}),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.reader()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) reader() {
	defer close(rsm.done)

	for msg := range rsm.applyCh {
		if !msg.CommandValid {
			continue
		}
		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		rsm.mu.Lock()
		value := rsm.sm.DoOp(op.Req)

		if ch, waiting := rsm.waiters[msg.CommandIndex]; waiting {
			delete(rsm.waiters, msg.CommandIndex)
			ch <- applyResult{
				op:    op,
				value: value,
			}
		}
		rsm.mu.Unlock()
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) { // 应该是 通过 goroutine 的方式 submit
	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	// return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	rsm.mu.Lock()
	rsm.nextID++
	op := Op{
		Me:  rsm.me,
		Id:  rsm.nextID,
		Req: req,
	}

	index, startTerm, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan applyResult, 1)
	rsm.waiters[index] = ch
	rsm.mu.Unlock()

	cleanup := func() {
		rsm.mu.Lock()
		if current, ok := rsm.waiters[index]; ok && current == ch {
			delete(rsm.waiters, index)
		}
		rsm.mu.Unlock()
	}
	defer cleanup()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case result := <-ch:
			if result.op.Me != op.Me || result.op.Id != op.Id {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, result.value
		case <-ticker.C:
			currentTerm, _ := rsm.rf.GetState()
			if currentTerm != startTerm {
				select {
				case result := <-ch:
					if result.op.Me == op.Me && result.op.Id == op.Id {
						return rpc.OK, result.value
					}
				default:
				}
				return rpc.ErrWrongLeader, nil
			}
		case <-rsm.done:
			return rpc.ErrWrongLeader, nil
		}
	}

}
