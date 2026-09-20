package shardgrp

import (
	"bytes"

	"6.5840/labgob"
	"6.5840/shardkv1/shardcfg"
)

func encodeShard(entries map[string]Entry) []byte {
	var buf bytes.Buffer
	enc := labgob.NewEncoder(&buf)
	if err := enc.Encode(entries); err != nil {
		panic(err)
	}

	return buf.Bytes()
}

func decodeShard(data []byte) map[string]Entry {
	result := make(map[string]Entry)
	if len(data) == 0 {
		return result
	}

	dec := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&result); err != nil {
		panic(err)
	}

	return result
}

func (kv *KVServer) shardEntriesLocked(s shardcfg.Tshid) map[string]Entry {
	result := make(map[string]Entry)
	for key, entry := range kv.entries {
		if shardcfg.Key2Shard(key) == s {
			result[key] = entry
		}
	}

	return result
}

func (kv *KVServer) deleteShardLocked(s shardcfg.Tshid) {
	for key := range kv.entries {
		if shardcfg.Key2Shard(key) == s {
			delete(kv.entries, key)
		}
	}
}
