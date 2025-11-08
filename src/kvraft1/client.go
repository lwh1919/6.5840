package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	mu       sync.Mutex
	clnt     *tester.Clnt
	servers  []string
	leader   int
	clientId int64
	seq      int64
}

var globalClientID int64

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:     clnt,
		servers:  servers,
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

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}

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
			}
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.
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

	for {
		start := ck.getLeader()
		for i := 0; i < len(ck.servers); i++ {
			server := (start + i) % len(ck.servers)
			var reply rpc.PutReply
			ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", args, &reply)
			if ok {
				if reply.Err == rpc.OK {
					ck.setLeader(server)
					return rpc.OK
				}
				if reply.Err == rpc.ErrVersion {
					if firstAttempt {
						return rpc.ErrVersion
					}
					return rpc.ErrMaybe
				}
				if reply.Err == rpc.ErrWrongLeader {
					firstAttempt = false
					continue
				}
			}
			firstAttempt = false
		}
	}
}
