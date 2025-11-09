package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	mu     sync.Mutex
	clnt   *tester.Clnt
	sck    *shardctrler.ShardCtrler
	clerks map[tester.Tgid]*shardgrp.Clerk
}

func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	return &Clerk{
		clnt:   clnt,
		sck:    sck,
		clerks: make(map[tester.Tgid]*shardgrp.Clerk),
	}
}

func (ck *Clerk) getGroupClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if clerk, ok := ck.clerks[gid]; ok {
		return clerk
	}
	clerk := shardgrp.MakeClerk(ck.clnt, servers)
	ck.clerks[gid] = clerk
	return clerk
}

func (ck *Clerk) invalidateGroup(gid tester.Tgid) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	delete(ck.clerks, gid)
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		cfg := ck.sck.Query()
		shard := shardcfg.Key2Shard(key)
		gid, servers, ok := cfg.GidServers(shard)
		if !ok || len(servers) == 0 {
			continue
		}

		clerk := ck.getGroupClerk(gid, servers)
		value, version, err := clerk.Get(key)
		if err == rpc.ErrWrongGroup {
			ck.invalidateGroup(gid)
			continue
		}
		return value, version, err
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	for {
		cfg := ck.sck.Query()
		shard := shardcfg.Key2Shard(key)
		gid, servers, ok := cfg.GidServers(shard)
		if !ok || len(servers) == 0 {
			continue
		}

		clerk := ck.getGroupClerk(gid, servers)
		err := clerk.Put(key, value, version)
		if err == rpc.ErrWrongGroup {
			ck.invalidateGroup(gid)
			continue
		}
		return err
	}
}
