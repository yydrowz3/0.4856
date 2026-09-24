package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"
	"time"

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
	for {
		current, currentVersion, err := sck.loadConfig(currentConfigKey) // 崩溃后重启是否应该继续 迁移
		if err != rpc.OK {
			panic(fmt.Sprintf("InitController: cannot read config: %v", err))
		}
		next, _, err := sck.loadConfig(nextConfigKey)
		if err != rpc.OK {
			panic(fmt.Sprintf("InitController: cannot read next config: %v", err))
		}

		switch {
		case next.Num == current.Num:
			// 空闲，没有待恢复迁移。
			return
		case next.Num == current.Num+1:
			sck.finishChange(current, next, currentVersion)
			return
		default:
			// 两次 Get 跨越了多个配置提交，不是原子快照。
			// 重新读取，不要 panic。
			time.Sleep(20 * time.Millisecond)
			continue
		}

	}
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

	// 过期
	if new.Num <= current.Num {
		return
	}

	if new.Num != current.Num+1 {
		// panic(fmt.Sprintf("ChangeConfigTo: current config %d, new config %d", current.Num, new.Num))
		return
	}

	next, nextVersion, err := sck.loadConfig(nextConfigKey)
	if err != rpc.OK {
		panic(fmt.Sprintf("ChangeConfigTo: cannot read next config: %v", err))
	}

	// next != current 表示已经有人发布了迁移意图。
	// 当前 controller 是竞争失败者，必须立即返回。
	if next.Num != current.Num {
		return
	}

	// if !sck.storeConfig(nextConfigKey, new, nextVersion) {
	// 	return
	// }

	// 使用 nextConfigKey 的 KV version 做 CAS。
	if !sck.tryPublishNext(new, nextVersion) {
		// 另一个控制器可能抢先写入了 next。
		return
	}

	// switch {
	// case next.Num == current.Num:
	// 	// 正常空闲状态，可以发布本次迁移意图。
	// case next.Num == current.Num+1:
	// 	// 已经有一次未完成的迁移。
	// 	// 先恢复旧迁移，不能用 new 覆盖它。
	// 	sck.finishChange(current, next, currentVersion)
	// 	return
	// default:
	// 	panic(fmt.Sprintf("ChangeConfigTo: invalid current/next: %d/%d", current.Num, next.Num))
	// }

	// if !sck.storeConfig(nextConfigKey, new, nextVersion) {
	// 	return
	// }

	// 只有成功发布 next 的 controller 执行迁移。
	sck.finishChange(current, new, currentVersion)
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
