package rsm

import (
	"bytes"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/raftapi"
)

type snapshotData struct {
	LastApplied int
	State       []byte
}

func encodeSnapshot(data snapshotData) []byte {
	var buf bytes.Buffer
	encoder := labgob.NewEncoder(&buf)
	if err := encoder.Encode(data); err != nil {
		panic(err)
	}

	return buf.Bytes()
}

func decodeSnapshot(raw []byte) snapshotData {
	decoder := labgob.NewDecoder(bytes.NewBuffer(raw))
	var data snapshotData
	if err := decoder.Decode(&data); err != nil {
		panic(err)
	}

	return data
}

func (rsm *RSM) installSnapshot(msg raftapi.ApplyMsg) {
	rsm.mu.Lock()

	if msg.SnapshotIndex <= rsm.lastApplied { // 过期快照
		rsm.mu.Unlock()
		return
	}
	snap := decodeSnapshot(msg.Snapshot)
	if snap.LastApplied != msg.SnapshotIndex {
		rsm.mu.Unlock()
		panic("rsm: snapshot index mismatch")
	}

	rsm.sm.Restore(snap.State)
	rsm.lastApplied = snap.LastApplied

	var failed []chan applyResult
	for index, ch := range rsm.waiters {
		if index <= rsm.lastApplied {
			delete(rsm.waiters, index)
			failed = append(failed, ch)
		}
	}

	rsm.mu.Unlock()
	for _, ch := range failed {
		ch <- applyResult{err: rpc.ErrWrongLeader}
	}
}
