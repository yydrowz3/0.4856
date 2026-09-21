package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
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

		if attempts%len(ck.servers) == 0 {
			time.Sleep(20 * time.Millisecond)
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
				if retried {
					return rpc.ErrMaybe
				}
				return rpc.ErrWrongGroup
			}
		}

		retried = true
		ck.leader = (server + 1) % len(ck.servers)
		attempts++

		if attempts%len(ck.servers) == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
