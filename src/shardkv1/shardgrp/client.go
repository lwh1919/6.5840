package shardgrp

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	mu      sync.Mutex
	clnt    *tester.Clnt
	servers []string
	leader  int

	clientId int64
	seq      int64
}

var globalClientID int64

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{
		clnt:     clnt,
		servers:  append([]string(nil), servers...),
		leader:   0,
		clientId: atomic.AddInt64(&globalClientID, 1),
	}
	return ck
}

func (ck *Clerk) nextSeq() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seq++
	return ck.seq
}

func (ck *Clerk) getLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leader
}

func (ck *Clerk) setLeader(leader int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leader = leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}

	retryCount := 0
	maxRetries := len(ck.servers) * 2 // Retry timeout mechanism

	for {
		start := ck.getLeader()
		for i := 0; i < len(ck.servers); i++ {
			server := (start + i) % len(ck.servers)
			var reply rpc.GetReply
			ok := ck.clnt.Call(ck.servers[server], "KVServer.Get", args, &reply)
			if ok {
				if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
					ck.setLeader(server)
					return reply.Value, reply.Version, reply.Err
				}
				if reply.Err == rpc.ErrWrongLeader {
					continue
				}
				if reply.Err == rpc.ErrWrongGroup {
					return "", 0, rpc.ErrWrongGroup
				}
			}
		}
		retryCount++
		if retryCount >= maxRetries {
			return "", 0, rpc.ErrWrongGroup // Timeout, assume wrong group
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	seq := ck.nextSeq()
	args := &rpc.PutArgs{
		Key:      key,
		Value:    value,
		Version:  version,
		ClientId: ck.clientId,
		Seq:      seq,
	}

	firstAttempt := true
	retryCount := 0
	maxRetries := len(ck.servers) * 2 // Retry timeout mechanism

	for {
		start := ck.getLeader()
		for i := 0; i < len(ck.servers); i++ {
			server := (start + i) % len(ck.servers)
			var reply rpc.PutReply
			ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", args, &reply)
			if ok {
				switch reply.Err {
				case rpc.OK:
					ck.setLeader(server)
					return rpc.OK
				case rpc.ErrVersion:
					if firstAttempt {
						return rpc.ErrVersion
					}
					return rpc.ErrMaybe
				case rpc.ErrWrongLeader:
					firstAttempt = false
					continue
				case rpc.ErrWrongGroup:
					return rpc.ErrWrongGroup
				default:
					firstAttempt = false
				}
			} else {
				firstAttempt = false
			}
		}
		retryCount++
		if retryCount >= maxRetries {
			return rpc.ErrWrongGroup // Timeout, assume wrong group
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum, shard2gid map[int]int) ([]byte, rpc.Err) {
	args := &shardrpc.FreezeShardArgs{Shard: s, Num: num, Shard2Gid: shard2gid}
	retryCount := 0
	maxRetries := len(ck.servers) * 2

	for {
		start := ck.getLeader()
		for i := 0; i < len(ck.servers); i++ {
			server := (start + i) % len(ck.servers)
			var reply shardrpc.FreezeShardReply
			ok := ck.clnt.Call(ck.servers[server], "KVServer.FreezeShard", args, &reply)
			if ok {
				if reply.Err == rpc.ErrWrongLeader {
					continue
				}
				if reply.Err == rpc.ErrWrongGroup {
					return nil, rpc.ErrWrongGroup
				}
				ck.setLeader(server)
				return reply.State, reply.Err
			}
		}
		retryCount++
		if retryCount >= maxRetries {
			return nil, rpc.ErrWrongGroup
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum, shard2gid map[int]int) rpc.Err {
	args := &shardrpc.InstallShardArgs{Shard: s, State: state, Num: num, Shard2Gid: shard2gid}
	retryCount := 0
	maxRetries := len(ck.servers) * 2

	for {
		start := ck.getLeader()
		for i := 0; i < len(ck.servers); i++ {
			server := (start + i) % len(ck.servers)
			var reply shardrpc.InstallShardReply
			ok := ck.clnt.Call(ck.servers[server], "KVServer.InstallShard", args, &reply)
			if ok {
				if reply.Err == rpc.ErrWrongLeader {
					continue
				}
				if reply.Err == rpc.ErrWrongGroup {
					return rpc.ErrWrongGroup
				}
				ck.setLeader(server)
				return reply.Err
			}
		}
		retryCount++
		if retryCount >= maxRetries {
			return rpc.ErrWrongGroup
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum, shard2gid map[int]int) rpc.Err {
	args := &shardrpc.DeleteShardArgs{Shard: s, Num: num, Shard2Gid: shard2gid}
	retryCount := 0
	maxRetries := len(ck.servers) * 2

	for {
		start := ck.getLeader()
		for i := 0; i < len(ck.servers); i++ {
			server := (start + i) % len(ck.servers)
			var reply shardrpc.DeleteShardReply
			ok := ck.clnt.Call(ck.servers[server], "KVServer.DeleteShard", args, &reply)
			if ok {
				if reply.Err == rpc.ErrWrongLeader {
					continue
				}
				if reply.Err == rpc.ErrWrongGroup {
					return rpc.ErrWrongGroup
				}
				ck.setLeader(server)
				return reply.Err
			}
		}
		retryCount++
		if retryCount >= maxRetries {
			return rpc.ErrWrongGroup
		}
		time.Sleep(10 * time.Millisecond)
	}
}
