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
const nextConfigKey = "shardctrler/next_config"

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
	current := sck.Query()
	next := sck.QueryNext()

	if next != nil && current != nil && next.Num > current.Num {
		sck.performMigration(current, next)

		version := rpc.Tversion(current.Num)

		for i := 0; i < 50; i++ {
			if putErr := sck.Put(configKey, next.String(), version); putErr == rpc.OK {
				sck.clearNextConfig()
				return
			}
			if i < 10 {
				time.Sleep(10 * time.Millisecond)
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
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

	for i := 0; ; i++ {
		if err := sck.Put(configKey, cfg.String(), 0); err == rpc.OK {
			return
		}
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

	sck.clearNextConfig()

	if !sck.putNextConfig(new) {
		return
	}

	sck.performMigration(old, new)

	version := rpc.Tversion(old.Num)

	//存储新的配置
	for i := 0; i < 50; i++ {
		if putErr := sck.Put(configKey, new.String(), version); putErr == rpc.OK {
			sck.clearNextConfig()
			return
		}
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

// Return the next configuration if it exists
func (sck *ShardCtrler) QueryNext() *shardcfg.ShardConfig {
	value, _, err := sck.Get(nextConfigKey)
	if err != rpc.OK || value == "" {
		return nil
	}

	return shardcfg.FromString(value)
}

// Store the next configuration
func (sck *ShardCtrler) putNextConfig(cfg *shardcfg.ShardConfig) bool {
	if cfg == nil {
		return false
	}

	cfgStr := cfg.String()

	for i := 0; i < 20; i++ {
		_, version, getErr := sck.Get(nextConfigKey)
		var putErr rpc.Err

		if getErr == rpc.ErrNoKey {
			putErr = sck.Put(nextConfigKey, cfgStr, 0)
		} else if getErr == rpc.OK {
			putErr = sck.Put(nextConfigKey, cfgStr, version)
		} else {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if putErr == rpc.OK {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func (sck *ShardCtrler) clearNextConfig() {
	for i := 0; i < 20; i++ {
		_, version, getErr := sck.Get(nextConfigKey)
		if getErr == rpc.ErrNoKey {
			return
		} else if getErr == rpc.OK {
			if err := sck.Put(nextConfigKey, "", version); err == rpc.OK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (sck *ShardCtrler) performMigration(old, new *shardcfg.ShardConfig) {
	if old == nil || new == nil {
		return
	}

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
					time.Sleep(time.Duration(retry*10) * time.Millisecond)
				}
			}
		}
	}

	for _, m := range migrations {
		if m.oldGid != 0 {
			if servers, exists := old.Groups[m.oldGid]; exists {
				clerk := shardgrp.MakeClerk(sck.clnt, servers)
				for retry := 0; retry < 20; retry++ {
					err := clerk.DeleteShard(shardcfg.Tshid(m.shard), new.Num, shard2gid)
					if err == rpc.OK {
						break
					}
					time.Sleep(time.Duration(retry*10) * time.Millisecond)
				}
			}
		}
	}
}
