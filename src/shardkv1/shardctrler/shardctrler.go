package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
)

const currentConfigKey = "shardctrler-current-config"
const nextConfigKey = "shardctrler-next-config"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	if !sck.storeConfig(currentConfigKey, cfg, 0) {
		panic("InitConfig: cannot store initial current config")
	}
	if !sck.storeConfig(nextConfigKey, cfg, 0) {
		panic("InitConfig: cannot store initial next config")
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	current, currentVersion, err := sck.loadConfig(currentConfigKey)
	if err != rpc.OK {
		panic(fmt.Sprintf("ChangeConfigTo: cannot read current config %v", err))
	}

	// old := shardcfg.FromString(currentValue)
	// if old.Num >= new.Num {
	// 	return
	// }
	if current.Num >= new.Num {
		return
	}

	if new.Num != current.Num+1 {
		panic(fmt.Sprintf("ChangeConfigTo: current config %d, new config %d", current.Num, new.Num))
	}

	next, nextVersion, err := sck.loadConfig(nextConfigKey)
	if err != rpc.OK {
		panic(fmt.Sprintf("ChangeConfigTo: cannot read next config: %v", err))
	}

	switch {
	case next.Num == current.Num:
		// 正常空闲状态，可以发布本次迁移意图。
	case next.Num == current.Num+1:
		// 已经有一次未完成的迁移。
		// 先恢复旧迁移，不能用 new 覆盖它。
		sck.finishChange(current, next, currentVersion)
		return
	default:
		panic(fmt.Sprintf("ChangeConfigTo: invalid current/next: %d/%d", current.Num, next.Num))
	}

	if !sck.storeConfig(nextConfigKey, new, nextVersion) {
		// 另一个控制器可能抢先写入了 next。
		return
	}

	sck.finishChange(current, new, currentVersion)

	// clerks := make(map[tester.Tgid]*shardgrp.Clerk)
	// getClerk := func(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	// 	ck, ok := clerks[gid]
	// 	if !ok {
	// 		ck = shardgrp.MakeClerk(sck.clnt, servers)
	// 		clerks[gid] = ck
	// 	}
	// 	return ck
	// }

	// for shardIndex := 0; shardIndex < shardcfg.NShards; shardIndex++ {
	// 	shard := shardcfg.Tshid(shardIndex)
	// 	oldGid := old.Shards[shard]
	// 	newGid := new.Shards[shard]

	// 	if oldGid == newGid {
	// 		continue
	// 	}

	// 	var state []byte

	// 	// 1. 冻结原 group，并取得 shard 的完整 state
	// 	if oldGid != 0 {
	// 		sourceServers, ok := old.Groups[oldGid]
	// 		if !ok {
	// 			panic(fmt.Sprintf("ChangeConfigTo: missing source group %d", oldGid))
	// 		}
	// 		source := getClerk(oldGid, sourceServers)
	// 		state, err = source.FreezeShard(shard, new.Num)
	// 		if err != rpc.OK { // 该controller 可能过期
	// 			return
	// 		}

	// 	}

	// 	// 2. 状态 install 到目标 group
	// 	if newGid != 0 {
	// 		destinationServers, ok := new.Groups[newGid]
	// 		if !ok {
	// 			panic(fmt.Sprintf("ChangeConfigTo: missing destination group %d", newGid))
	// 		}
	// 		destination := getClerk(newGid, destinationServers)
	// 		err = destination.InstallShard(shard, state, new.Num)
	// 		if err != rpc.OK {
	// 			return
	// 		}
	// 	}

	// 	// 3. install 成功后才删除原数据
	// 	if oldGid != 0 {
	// 		sourceServers := old.Groups[oldGid]
	// 		source := getClerk(oldGid, sourceServers)

	// 		err = source.DeleteShard(shard, new.Num)
	// 		if err != rpc.OK {
	// 			return
	// 		}
	// 	}
	// }

	// // 4. 更新配置
	// newValue := new.String()
	// err = sck.Put(currentConfigKey, newValue, currentVersion)
	// if err == rpc.OK {
	// 	return
	// }

	// if err == rpc.ErrMaybe || err == rpc.ErrVersion { // 有可能有 两个 controller 发布同一个 配置，导致的冲突
	// 	value, _, getErr := sck.Get(currentConfigKey)
	// 	if getErr == rpc.OK && value == newValue {
	// 		return
	// 	}
	// }

	panic(fmt.Sprintf("ChangeConfigTo: cannot publish config %d: %v", new.Num, err))
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	cfg, _, err := sck.loadConfig(currentConfigKey)
	if err != rpc.OK {
		panic(fmt.Sprintf("Query: cannot read current config: %v", err))
	}
	return cfg
}
