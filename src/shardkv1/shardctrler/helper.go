package shardctrler

import (
	"fmt"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

func (sck *ShardCtrler) loadConfig(key string) (*shardcfg.ShardConfig, rpc.Tversion, rpc.Err) {
	value, version, err := sck.Get(key)
	if err != rpc.OK {
		return nil, version, err
	}
	return shardcfg.FromString(value), version, rpc.OK
}

func (sck *ShardCtrler) storeConfig(key string, cfg *shardcfg.ShardConfig, version rpc.Tversion) bool {
	want := cfg.String()
	err := sck.Put(key, want, version)
	if err == rpc.OK {
		return true
	}
	if err == rpc.ErrMaybe || err == rpc.ErrVersion {
		got, _, getErr := sck.Get(key)
		return getErr == rpc.OK && got == want
	}

	return false
}

func (sck *ShardCtrler) finishChange(old *shardcfg.ShardConfig, new *shardcfg.ShardConfig, currentVersion rpc.Tversion) bool {
	clerks := make(map[tester.Tgid]*shardgrp.Clerk)
	getClerk := func(gid tester.Tgid, servers []string) *shardgrp.Clerk {
		ck, ok := clerks[gid]
		if !ok {
			ck = shardgrp.MakeClerk(sck.clnt, servers)
			clerks[gid] = ck
		}
		return ck
	}

	for shardIndex := 0; shardIndex < shardcfg.NShards; shardIndex++ {
		shard := shardcfg.Tshid(shardIndex)
		oldGid := old.Shards[shard]
		newGid := new.Shards[shard]

		if oldGid == newGid {
			continue
		}

		var state []byte
		var err rpc.Err

		// 1. 冻结原 group，并取得 shard 的完整 state
		if oldGid != 0 {
			sourceServers, ok := old.Groups[oldGid]
			if !ok {
				panic(fmt.Sprintf("ChangeConfigTo: missing source group %d", oldGid))
			}
			source := getClerk(oldGid, sourceServers)
			state, err = source.FreezeShard(shard, new.Num)
			if err != rpc.OK { // 该controller 可能过期
				return false
			}

		}

		// 2. 状态 install 到目标 group
		if newGid != 0 {
			destinationServers, ok := new.Groups[newGid]
			if !ok {
				panic(fmt.Sprintf("ChangeConfigTo: missing destination group %d", newGid))
			}
			destination := getClerk(newGid, destinationServers)
			err = destination.InstallShard(shard, state, new.Num)
			if err != rpc.OK {
				return false
			}
		}

		// 3. install 成功后才删除原数据
		if oldGid != 0 {
			sourceServers := old.Groups[oldGid]
			source := getClerk(oldGid, sourceServers)

			err = source.DeleteShard(shard, new.Num)
			if err != rpc.OK {
				return false
			}
		}
	}

	return sck.storeConfig(currentConfigKey, new, currentVersion)
}
