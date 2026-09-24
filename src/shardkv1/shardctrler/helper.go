package shardctrler

import (
	"fmt"
	"time"

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
			for {
				if !sck.stillPending(old, new) {
					return false
				}

				state, err = source.FreezeShard(shard, new.Num)

				if err == rpc.OK {
					break
				}
				if err == rpc.ErrWrongGroup {
					return false
				}
				if !sck.stillPending(old, new) {
					return false
				}

				time.Sleep(20 * time.Millisecond)
			}
		}

		// 2. 状态 install 到目标 group
		if newGid != 0 {
			destinationServers, ok := new.Groups[newGid]
			if !ok {
				panic(fmt.Sprintf("ChangeConfigTo: missing destination group %d", newGid))
			}
			destination := getClerk(newGid, destinationServers)
			for {
				if !sck.stillPending(old, new) {
					return false
				}

				err = destination.InstallShard(shard, state, new.Num)
				if err == rpc.OK {
					break
				}
				if err == rpc.ErrWrongGroup {
					return false
				}
				if !sck.stillPending(old, new) {
					return false
				}

				time.Sleep(20 * time.Millisecond)
			}
		}

		// 3. install 成功后才删除原数据
		if oldGid != 0 {
			sourceServers := old.Groups[oldGid]
			source := getClerk(oldGid, sourceServers)
			for {
				if !sck.stillPending(old, new) {
					return false
				}

				err = source.DeleteShard(shard, new.Num)
				if err == rpc.OK {
					break
				}
				// 更高配置已经到达该 shard group。
				if err == rpc.ErrWrongGroup {
					return false
				}
				// 暂时不可达。先判断当前 controller 是否仍有效。
				if !sck.stillPending(old, new) {
					return false
				}

				time.Sleep(20 * time.Millisecond)
			}
		}
	}

	return sck.storeConfig(currentConfigKey, new, currentVersion)
}

func (sck *ShardCtrler) tryPublishNext(cfg *shardcfg.ShardConfig, version rpc.Tversion) bool {
	want := cfg.String()
	err := sck.Put(nextConfigKey, want, version)

	switch err {
	case rpc.OK:
		return true
	case rpc.ErrVersion:
		// 第一次 RPC 明确返回版本错误，本 controller 没有成功。
		return false
	case rpc.ErrMaybe:
		got, _, getErr := sck.Get(nextConfigKey)
		return getErr == rpc.OK && got == want
	default:
		return false
	}

}

func (sck *ShardCtrler) stillPending(old *shardcfg.ShardConfig, new *shardcfg.ShardConfig) bool {
	current, _, err := sck.loadConfig(currentConfigKey)
	if err != rpc.OK {
		return false
	}

	// current 已经提交 new，或者已经进入更高配置。
	if current.Num >= new.Num {
		return false
	}

	// 确认当前迁移仍然是自己正在执行的那一次。
	if current.String() != old.String() {
		return false
	}

	next, _, err := sck.loadConfig(nextConfigKey)
	if err != rpc.OK {
		return false
	}

	return next.String() == new.String()
}
