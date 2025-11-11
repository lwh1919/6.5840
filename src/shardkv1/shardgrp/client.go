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

// Get不检查version是因为它只是读取当前最新状态，而Put需要version检查来防止过时更新。线性一致性通过RSM的日志复制和顺序执行机制来保证，而不是通过Get操作的version检查。
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
					//一旦我们遇到超时、重定向或网络失败，就会把 firstAttempt 置为 false，随后再次收到的 ErrVersion 就说明“状态变了，但到底是我们之前的某次请求成功了，还是别人写入了，无法确认”，只能返回 ErrMaybe。这和线性一致性不冲突，属于客户端对“请求是否已经执行过”的不确定性处理。
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
