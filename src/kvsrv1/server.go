package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Entry struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	entries map[string]Entry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.entries = make(map[string]Entry)

	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	e, exists := kv.entries[args.Key]
	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = e.value
	reply.Version = e.version
	reply.Err = rpc.OK

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	e, exists := kv.entries[args.Key]
	if exists {
		if args.Version == e.version {
			// update
			// kv.entries[args.Key].value = args.Value // 这种写法不对，因为取出来的是临时值
			// kv.entries[args.Key].version++
			e.value = args.Value
			e.version++
			kv.entries[args.Key] = e
			reply.Err = rpc.OK
			return
		} else {
			reply.Err = rpc.ErrVersion
			return
		}
	} else {
		if args.Version == 0 {
			kv.entries[args.Key] = Entry{value: args.Value, version: 1}
			reply.Err = rpc.OK
			return
		} else {
			reply.Err = rpc.ErrNoKey
			return
		}
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
