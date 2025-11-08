package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	keyValue map[string]string       // 存储键值对
	versions map[string]rpc.Tversion // 存储每个键的版本号
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		keyValue: make(map[string]string),
		versions: make(map[string]rpc.Tversion),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exists := kv.keyValue[args.Key]
	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = value
	reply.Version = kv.versions[args.Key]
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	currentVersion, exists := kv.versions[args.Key]

	if !exists {
		// 键不存在
		if args.Version == 0 {
			// 创建新键
			kv.keyValue[args.Key] = args.Value
			kv.versions[args.Key] = 1
			reply.Err = rpc.OK
		} else {
			// version > 0 但键不存在
			reply.Err = rpc.ErrNoKey
		}
	} else {
		// 键存在
		if args.Version == currentVersion {
			// 版本匹配，更新
			kv.keyValue[args.Key] = args.Value
			kv.versions[args.Key] = currentVersion + 1
			reply.Err = rpc.OK
		} else {
			// 版本不匹配
			reply.Err = rpc.ErrVersion
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
