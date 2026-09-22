package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args := rpc.GetArgs{Key: key}
	attempts := 0

	for {
		server := ck.leader
		var reply rpc.GetReply

		ok := ck.Call(ck.servers[server], "KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				return reply.Value, reply.Version, rpc.OK
			case rpc.ErrNoKey:
				return "", 0, rpc.ErrNoKey
			case rpc.ErrWrongGroup:
				return "", 0, rpc.ErrWrongGroup
			}
		}

		ck.leader = (server + 1) % len(ck.servers)
		attempts++

		// if attempts%len(ck.servers) == 0 {
		// 	time.Sleep(20 * time.Millisecond)
		// }
		if attempts >= len(ck.servers) {
			return "", 0, rpc.ErrWrongGroup
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	retried := false
	attempts := 0

	for {
		server := ck.leader
		var reply rpc.PutReply

		ok := ck.Call(ck.servers[server], "KVServer.Put", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				return rpc.OK
			case rpc.ErrNoKey:
				return rpc.ErrNoKey
			case rpc.ErrVersion:
				if retried {
					return rpc.ErrMaybe
				}
				return rpc.ErrVersion
			case rpc.ErrWrongGroup:
				// if retried {
				// 	return rpc.ErrMaybe
				// }
				return rpc.ErrWrongGroup
			}
		}

		retried = true
		ck.leader = (server + 1) % len(ck.servers)
		attempts++

		// if attempts%len(ck.servers) == 0 {
		// 	time.Sleep(20 * time.Millisecond)
		// }
		if attempts >= len(ck.servers) {
			return rpc.ErrWrongGroup
		}
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}
	attempts := 0

	for {
		server := ck.leader
		var reply shardrpc.FreezeShardReply

		ok := ck.Call(ck.servers[server], "KVServer.FreezeShard", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				if reply.Num != num {
					return nil, rpc.ErrWrongGroup
				}
				return reply.State, rpc.OK
			case rpc.ErrWrongGroup:
				return nil, rpc.ErrWrongGroup
			}
		}

		ck.leader = (server + 1) % len(ck.servers)
		attempts++

		if attempts%len(ck.servers) == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}
	attempts := 0

	for {
		server := ck.leader
		var reply shardrpc.InstallShardReply

		ok := ck.Call(ck.servers[server], "KVServer.InstallShard", &args, &reply)

		if ok {
			switch reply.Err {
			case rpc.OK:
				return rpc.OK
			case rpc.ErrWrongGroup:
				return rpc.ErrWrongGroup
			}
		}
		ck.leader = (server + 1) % len(ck.servers)
		attempts++

		if attempts%len(ck.servers) == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}

}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}
	attempts := 0
	for {
		server := ck.leader
		var reply shardrpc.DeleteShardReply

		ok := ck.Call(ck.servers[server], "KVServer.DeleteShard", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				return rpc.OK
			case rpc.ErrWrongGroup:
				return rpc.ErrWrongGroup
			}
		}
		ck.leader = (server + 1) % len(ck.servers)
		attempts++
		if attempts%len(ck.servers) == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
}
