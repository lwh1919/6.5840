package shardctrler

import (
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const configKey = "shardctrler/config"

type ShardCtrler struct {
	clnt            *tester.Clnt //
	kvtest.IKVClerk              //单机KV存储，存储配置

	killed int32 // set by Kill()

	// Your data here.
}

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
	if cfg == nil {
		return
	}

	// Retry InitConfig in case of network failures
	for i := 0; ; i++ {
		if err := sck.Put(configKey, cfg.String(), 0); err == rpc.OK {
			return
		}
		// Retry on failure with exponential backoff
		time.Sleep(10 * time.Millisecond)
	}

}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	if new == nil {
		return
	}

	old := sck.Query()
	if old == nil {
		return
	}

	version := rpc.Tversion(old.Num)

	type migration struct {
		shard  int
		oldGid tester.Tgid
		newGid tester.Tgid
		state  []byte
	}

	var migrations []migration

	shard2gid := make(map[int]int)
	for i, gid := range new.Shards {
		shard2gid[i] = int(gid)
	}

	//遍历所有shard，判断是否需要迁移
	for shard := 0; shard < shardcfg.NShards; shard++ {
		oldGid := old.Shards[shard]
		newGid := new.Shards[shard]

		if oldGid != newGid {
			migrations = append(migrations, migration{
				shard:  shard,
				oldGid: oldGid,
				newGid: newGid,
			})
		}
	}

	//遍历所有需要迁移的shard，冻结shard
	for i := range migrations {
		m := &migrations[i]
		if m.oldGid != 0 {
			if servers, exists := old.Groups[m.oldGid]; exists {
				clerk := shardgrp.MakeClerk(sck.clnt, servers)
				for retry := 0; retry < 20; retry++ {
					state, err := clerk.FreezeShard(shardcfg.Tshid(m.shard), new.Num, shard2gid)
					if err == rpc.OK {
						m.state = state
						break
					}
					time.Sleep(time.Duration(retry*10) * time.Millisecond)
				}
			}
		}
	}

	//遍历所有需要迁移的shard，安装shard
	for _, m := range migrations {
		if m.newGid != 0 {
			if servers, exists := new.Groups[m.newGid]; exists {
				clerk := shardgrp.MakeClerk(sck.clnt, servers)
				state := m.state
				if state == nil {
					state = []byte{}
				}
				for retry := 0; retry < 20; retry++ {
					err := clerk.InstallShard(shardcfg.Tshid(m.shard), state, new.Num, shard2gid)
					if err == rpc.OK {
						break
					}
					// Retry on failure with backoff
					time.Sleep(time.Duration(retry*10) * time.Millisecond)
				}
			}
		}
	}

	//遍历所有需要迁移的shard，删除shard
	for _, m := range migrations {
		if m.oldGid != 0 {
			if servers, exists := old.Groups[m.oldGid]; exists {
				clerk := shardgrp.MakeClerk(sck.clnt, servers)
				for retry := 0; retry < 20; retry++ {
					err := clerk.DeleteShard(shardcfg.Tshid(m.shard), new.Num, shard2gid)
					if err == rpc.OK {
						break
					}
					// Retry on failure with backoff
					time.Sleep(time.Duration(retry*10) * time.Millisecond)
				}
			}
		}
	}

	//存储新的配置
	for i := 0; i < 50; i++ {
		if putErr := sck.Put(configKey, new.String(), version); putErr == rpc.OK {
			return
		}
		// Retry on failure with exponential backoff
		if i < 10 {
			time.Sleep(10 * time.Millisecond)
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	value, _, err := sck.Get(configKey)
	if err != rpc.OK {
		return shardcfg.MakeShardConfig()
	}

	return shardcfg.FromString(value)
}
