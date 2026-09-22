package shardgrp

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type Entry struct {
	Value   string
	Version rpc.Tversion
}

type ShardPhase uint8

const (
	ShardAbsent  ShardPhase = iota // 不拥有该 shard
	ShardServing                   // 正常提供服务
	ShardFrozen                    // shard 正在迁移，暂时冻结
)

type KVSnapshot struct {
	Entries map[string]Entry
	Phase   [shardcfg.NShards]ShardPhase
	Seen    [shardcfg.NShards]shardcfg.Tnum
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// Your code here
	mu      sync.Mutex
	entries map[string]Entry
	phase   [shardcfg.NShards]ShardPhase
	seen    [shardcfg.NShards]shardcfg.Tnum
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch args := req.(type) {
	case rpc.GetArgs:
		s := shardcfg.Key2Shard(args.Key)
		if kv.phase[s] != ShardServing {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
		entry, ok := kv.entries[args.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{
			Value:   entry.Value,
			Version: entry.Version,
			Err:     rpc.OK,
		}
	case rpc.PutArgs:
		s := shardcfg.Key2Shard(args.Key)
		if kv.phase[s] != ShardServing {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
		entry, ok := kv.entries[args.Key]
		if !ok {
			if args.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
			kv.entries[args.Key] = Entry{Value: args.Value, Version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		if args.Version != entry.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		kv.entries[args.Key] = Entry{Value: args.Value, Version: entry.Version + 1}
		return rpc.PutReply{Err: rpc.OK}

	case shardrpc.FreezeShardArgs:
		s := args.Shard
		previousNum := kv.seen[s]
		reply := shardrpc.FreezeShardReply{
			Num: kv.seen[s],
		}
		if args.Num < previousNum {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		// 即使下面的状态转换失败，也会更新 seen
		isNewNum := args.Num > previousNum
		if isNewNum {
			kv.seen[s] = args.Num
			reply.Num = args.Num
		}

		switch kv.phase[s] {
		case ShardServing:
			kv.phase[s] = ShardFrozen
			reply.State = encodeShard(kv.shardEntriesLocked(s))
			reply.Err = rpc.OK
		case ShardFrozen: // Freeze 重试
			reply.State = encodeShard(kv.shardEntriesLocked(s))
			reply.Err = rpc.OK
		case ShardAbsent:
			if isNewNum { // 新迁移，但是当前 Absent
				reply.Err = rpc.ErrWrongGroup
			} else { // 旧迁移，当前可能已经 install + delete 了
				// Freeze 的响应丢失后，另一 controller 可能已经
				// Install + Delete。目标 group 会忽略重复 Install。
				reply.State = encodeShard(kv.shardEntriesLocked(s))
				reply.Err = rpc.OK
			}

		}

		return reply
	case shardrpc.InstallShardArgs: // 先删除到 absent 后才能 install
		s := args.Shard
		previousNum := kv.seen[s]
		if args.Num < previousNum {
			return shardrpc.InstallShardReply{
				Err: rpc.ErrWrongGroup,
			}
		}

		isNewNum := args.Num > previousNum
		if isNewNum {
			kv.seen[s] = args.Num
		}

		switch kv.phase[s] {
		case ShardServing:
			if !isNewNum {
				// 重复 Install
				return shardrpc.InstallShardReply{
					Err: rpc.OK,
				}
			}
			return shardrpc.InstallShardReply{
				Err: rpc.ErrWrongGroup,
			}
		case ShardFrozen:
			// 一个 group 不应该在同一轮同时作为源和目标。
			return shardrpc.InstallShardReply{
				Err: rpc.ErrWrongGroup,
			}
		case ShardAbsent: // 不能直接跳过，因为如果完成 install，会是 serving 状态
			incoming := decodeShard(args.State)
			kv.deleteShardLocked(s)

			for key, entry := range incoming {
				if shardcfg.Key2Shard(key) == s {
					kv.entries[key] = entry
				}
			}

			kv.phase[s] = ShardServing
			return shardrpc.InstallShardReply{
				Err: rpc.OK,
			}
		}

	case shardrpc.DeleteShardArgs:
		s := args.Shard
		previousNum := kv.seen[s]
		if args.Num < previousNum {
			return shardrpc.DeleteShardReply{
				Err: rpc.ErrWrongGroup,
			}
		}
		isNewNum := args.Num > previousNum
		if isNewNum {
			kv.seen[s] = args.Num
		}
		switch kv.phase[s] {
		case ShardFrozen:
			kv.deleteShardLocked(s)
			kv.phase[s] = ShardAbsent
			return shardrpc.DeleteShardReply{
				Err: rpc.OK,
			}
		case ShardAbsent:
			if !isNewNum {
				return shardrpc.DeleteShardReply{
					Err: rpc.OK,
				}
			}
			return shardrpc.DeleteShardReply{
				Err: rpc.ErrWrongGroup,
			}
		case ShardServing:
			return shardrpc.DeleteShardReply{
				Err: rpc.ErrWrongGroup,
			}
		}

	default:
		panic("ShardGrp: unknown operation")
	}

	panic("ShardGrp.DoOp: operation produced no reply")
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)
	snapshot := KVSnapshot{
		Entries: kv.entries,
		Phase:   kv.phase,
		Seen:    kv.seen,
	}

	if err := encoder.Encode(snapshot); err != nil {
		panic(err)
	}

	return buffer.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if len(data) == 0 {
		return
	}

	var snapshot KVSnapshot
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := decoder.Decode(&snapshot); err != nil {
		panic(err)
	}
	if snapshot.Entries == nil {
		snapshot.Entries = make(map[string]Entry)
	}
	kv.mu.Lock()
	kv.entries = snapshot.Entries
	kv.phase = snapshot.Phase
	kv.seen = snapshot.Seen
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(shardrpc.DeleteShardReply)
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.entries = make(map[string]Entry)

	// Your code here
	if gid == shardcfg.Gid1 {
		for shard := range kv.phase {
			kv.phase[shard] = ShardServing
			kv.seen[shard] = shardcfg.NumFirst
		}
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
