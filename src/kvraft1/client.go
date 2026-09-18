package kvraft

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{Err: rpc.ErrMaybe}

	for {
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if !ok {
			log.Printf("Clerk.Get: Call failed for key %s, resend", key)
		} else {
			if reply.Err == rpc.OK {
				return reply.Value, reply.Version, rpc.OK
			}
			if reply.Err == rpc.ErrNoKey {
				return "", 0, rpc.ErrNoKey
			}
			if reply.Err == rpc.ErrWrongLeader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{Err: rpc.ErrMaybe}

	firstOk := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
	if firstOk {
		// message not drop
		if reply.Err == rpc.OK {
			return rpc.OK
		}
		if reply.Err == rpc.ErrVersion {
			return rpc.ErrVersion
		}
		if reply.Err == rpc.ErrNoKey {
			return rpc.ErrNoKey
		}
	} else {
		// message drop
		log.Printf("Clerk.Put: First call trial failed for key %s, start resending", key)
		for {
			time.Sleep(100 * time.Millisecond)
			ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
			if !ok {
				log.Printf("Clerk.Put: Call failed for key %s, resend", key)
			} else {
				if reply.Err == rpc.OK {
					return rpc.OK
				}
				if reply.Err == rpc.ErrVersion {
					return rpc.ErrMaybe
				}
				if reply.Err == rpc.ErrNoKey {
					return rpc.ErrNoKey
				}
			}
		}
	}

	return rpc.ErrMaybe
}
