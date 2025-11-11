package shardgrp

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

// 幂等性就是“同一条请求重复执行多次，结果和只执行一次一样”。在 server.go 里这么做是必要的，因为系统里存在两种会导致重复提交的情况：
// 网络不可靠：客户端（Clerk）调用 RPC 时，如果网络抖动没拿到回复，它会重试。同一个 Put 有可能被服务器执行两次。
// 共识层（RSM/Raft）重放日志：系统崩溃重启或需要回放日志时，老的操作会被再执行一遍。
type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	mu              sync.Mutex
	Kvs             map[string]KeyValueStore // Key -> {Value, Version}
	lastOps         map[int64]lastOperation  //缓存客户端的请求，防止重复处理
	shard2gid       map[int]int              // shard -> gid mapping，记录每个shard的gid
	lastFreezeNum   map[int]int              // shard -> last freeze num，记录每个shard的最后一次冻结的num
	lastInstallNum  map[int]int              // shard -> last install num，记录每个shard的最后一次安装的num
	lastDeleteNum   map[int]int              // shard -> last delete num，记录每个shard的最后一次删除的num
	lastFreezeState map[int][]byte           // shard -> last freeze state，记录每个shard的最后一次冻结的状态
}

type KeyValueStore struct {
	Value   string
	Version rpc.Tversion
}

type lastOperation struct {
	Seq   int64
	Reply rpc.PutReply
}

// Operations for RSM
type getOp rpc.GetArgs
type putOp rpc.PutArgs
type freezeOp shardrpc.FreezeShardArgs
type installOp shardrpc.InstallShardArgs
type deleteOp shardrpc.DeleteShardArgs

// Results for RSM
type getResult rpc.GetReply
type putResult rpc.PutReply
type freezeResult shardrpc.FreezeShardReply
type installResult shardrpc.InstallShardReply
type deleteResult shardrpc.DeleteShardReply

func (kv *KVServer) DoOp(req any) any {
	//
	var requestId int64
	var clientId int64
	isShard := false

	switch args := req.(type) {
	case *rpc.PutArgs:
		requestId = args.Seq
		clientId = args.ClientId
	case rpc.PutArgs:
		requestId = args.Seq
		clientId = args.ClientId
	case *rpc.GetArgs:
		//记录仅用于“去重写请求”
		//lastOps 用来防止 Put 被重复执行——不管是客户端因为超时重发相同的 Put，还是 Raft 在崩溃恢复时重放已提交日志，写请求都可能再次进入状态机。如果不做去重，旧请求会覆盖新值，破坏一致性。缓存 Put 的结果就能保证“同一请求只生效一次”。
		//Get操作不需要幂等性检查
	case rpc.GetArgs:
		//Get操作不需要幂等性检查
	case *shardrpc.FreezeShardArgs:
		isShard = true
	case shardrpc.FreezeShardArgs:
		isShard = true
	case *shardrpc.InstallShardArgs:
		isShard = true
	case shardrpc.InstallShardArgs:
		isShard = true
	case *shardrpc.DeleteShardArgs:
		isShard = true
	case shardrpc.DeleteShardArgs:
		isShard = true
	}

	if isShard {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		switch args := req.(type) {
		case *shardrpc.FreezeShardArgs:
			return kv.doFreezeShardInternal(args)
		case shardrpc.FreezeShardArgs:
			return kv.doFreezeShardInternal(&args)
		case *shardrpc.InstallShardArgs:
			return kv.doInstallShardInternal(args)
		case shardrpc.InstallShardArgs:
			return kv.doInstallShardInternal(&args)
		case *shardrpc.DeleteShardArgs:
			return kv.doDeleteShardInternal(args)
		case shardrpc.DeleteShardArgs:
			return kv.doDeleteShardInternal(&args)
		}
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	//
	if requestId != 0 && clientId != 0 {
		if record, ok := kv.lastOps[clientId]; ok && requestId <= record.Seq {
			return record.Reply
		}
	}

	var result any
	switch args := req.(type) {
	case *rpc.GetArgs:
		result = kv.handleGet(args)
	case rpc.GetArgs:
		result = kv.handleGet(&args)
	case *rpc.PutArgs:
		result = kv.handlePut(args)
	case rpc.PutArgs:
		result = kv.handlePut(&args)
	}

	//
	if _, ok := req.(*rpc.PutArgs); ok {
		if putResult, ok := result.(rpc.PutReply); ok {
			kv.lastOps[clientId] = lastOperation{Seq: requestId, Reply: putResult}
		}
	} else if _, ok := req.(rpc.PutArgs); ok {
		if putResult, ok := result.(rpc.PutReply); ok {
			kv.lastOps[clientId] = lastOperation{Seq: requestId, Reply: putResult}
		}
	}

	return result
}

func (kv *KVServer) handleGet(args *rpc.GetArgs) rpc.GetReply {
	//判断是否是该group负责的shard
	if kv.isNotThisGroup(args.Key) {
		return rpc.GetReply{Err: rpc.ErrWrongGroup}
	}

	if kvStore, ok := kv.Kvs[args.Key]; ok {
		return rpc.GetReply{Value: kvStore.Value, Version: kvStore.Version, Err: rpc.OK}
	}
	return rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrNoKey}
}

func (kv *KVServer) handlePut(args *rpc.PutArgs) rpc.PutReply {
	// Check if this server is responsible for this key
	if kv.isNotThisGroup(args.Key) {
		return rpc.PutReply{Err: rpc.ErrWrongGroup}
	}

	if kvStore, ok := kv.Kvs[args.Key]; ok {
		if kvStore.Version != args.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		kv.Kvs[args.Key] = KeyValueStore{Value: args.Value, Version: kvStore.Version + 1}
	} else {
		if args.Version != 0 {
			return rpc.PutReply{Err: rpc.ErrNoKey}
		}
		kv.Kvs[args.Key] = KeyValueStore{Value: args.Value, Version: 1}
	}
	return rpc.PutReply{Err: rpc.OK}
}

func (kv *KVServer) isNotThisGroup(key string) bool {
	shard := int(shardcfg.Key2Shard(key))
	gid, exists := kv.shard2gid[shard]
	if !exists {
		// 如果没有任何映射，则假设这个group负责（用于初始化）
		//为避免分片迁移期间的错写/脏读，建议缺映射默认拒绝，或至少只在明确的初始化场景下放行。
		return true
	}
	return gid != int(kv.gid)
}

func (kv *KVServer) doFreezeShardInternal(args *shardrpc.FreezeShardArgs) *shardrpc.FreezeShardReply {
	// 幂等性检查
	//Idempotency: applying the same operation multiple times has the same effect as applying it once.
	//直观理解：重复执行不会产生额外副作用；重放/重试是安全的。
	//容忍客户端/网络重试：RPC 超时后重发相同 FreezeShard(num) 不会重复复制或生成不同的状态；直接返回上次的结果。
	// 容忍共识层日志重放：崩溃恢复时同一条日志再次执行，也不会改变系统状态（不会多次 bump 计数、不会重复删/装/冻）。
	// 提供“至少一次”语义的安全落地：即便同一请求多次到达，最终状态一致，且返回结果可预测（相同 num 返回相同值，旧 num 直接 OK）。
	if lastNum, ok := kv.lastFreezeNum[int(args.Shard)]; ok && lastNum >= int(args.Num) {

		if lastNum == int(args.Num) {
			kv.shard2gid = args.Shard2Gid
			return &shardrpc.FreezeShardReply{Err: rpc.OK, State: kv.lastFreezeState[int(args.Shard)], Num: shardcfg.Tnum(lastNum)}
		}
		//因为这是一个“旧配置号”的请求，不能也没必要发送旧数据。返回空数据并带上当前最新的 Num，提示对端“你落后了，请按新配置迁移”。
		return &shardrpc.FreezeShardReply{Err: rpc.OK, State: []byte{}, Num: shardcfg.Tnum(lastNum)}
	}

	kv.shard2gid = args.Shard2Gid

	// 复制shard数据
	shardData := make(map[string]KeyValueStore)
	for k, v := range kv.Kvs {
		if shardcfg.Key2Shard(k) == args.Shard {
			shardData[k] = v
		}
	}

	// 序列化shard数据
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(shardData)
	stateData := w.Bytes()

	kv.lastFreezeNum[int(args.Shard)] = int(args.Num)
	kv.lastFreezeState[int(args.Shard)] = stateData

	return &shardrpc.FreezeShardReply{Err: rpc.OK, State: stateData, Num: args.Num}
}

func (kv *KVServer) doInstallShardInternal(args *shardrpc.InstallShardArgs) *shardrpc.InstallShardReply {
	//
	if lastNum, ok := kv.lastInstallNum[int(args.Shard)]; ok && lastNum >= int(args.Num) {
		kv.shard2gid = args.Shard2Gid
		return &shardrpc.InstallShardReply{Err: rpc.OK}
	}

	// 反序列化shard数据
	if len(args.State) > 0 {
		d := labgob.NewDecoder(bytes.NewBuffer(args.State))
		var shardData map[string]KeyValueStore
		if d.Decode(&shardData) == nil {
			for k, v := range shardData {
				kv.Kvs[k] = v
			}
		}
	}

	kv.lastInstallNum[int(args.Shard)] = int(args.Num)
	kv.shard2gid = args.Shard2Gid

	return &shardrpc.InstallShardReply{Err: rpc.OK}
}

func (kv *KVServer) doDeleteShardInternal(args *shardrpc.DeleteShardArgs) *shardrpc.DeleteShardReply {
	// 幂等性检查
	if lastNum, ok := kv.lastDeleteNum[int(args.Shard)]; ok && lastNum >= int(args.Num) {
		kv.shard2gid = args.Shard2Gid
		return &shardrpc.DeleteShardReply{Err: rpc.OK}
	}

	kv.shard2gid = args.Shard2Gid

	// 删除shard数据
	deletedCount := 0
	for k := range kv.Kvs {
		if shardcfg.Key2Shard(k) == args.Shard {
			delete(kv.Kvs, k)
			deletedCount++
		}
	}

	kv.lastDeleteNum[int(args.Shard)] = int(args.Num)

	// 清理冻结状态，减少快照大小
	delete(kv.lastFreezeState, int(args.Shard))

	if deletedCount > 0 {
		// fmt.Printf("DeleteShard: gid=%d, shard=%d, deleted %d keys\n", kv.gid, args.Shard, deletedCount)
	}

	return &shardrpc.DeleteShardReply{Err: rpc.OK}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	snapshot := struct {
		Kvs             map[string]KeyValueStore
		LastOps         map[int64]lastOperation
		Shard2Gid       map[int]int
		LastFreezeNum   map[int]int
		LastInstallNum  map[int]int
		LastDeleteNum   map[int]int
		LastFreezeState map[int][]byte
	}{
		Kvs:             copyKvs(kv.Kvs),
		LastOps:         copyLastOps(kv.lastOps),
		Shard2Gid:       copyIntMap(kv.shard2gid),
		LastFreezeNum:   copyIntMap(kv.lastFreezeNum),
		LastInstallNum:  copyIntMap(kv.lastInstallNum),
		LastDeleteNum:   copyIntMap(kv.lastDeleteNum),
		LastFreezeState: copyByteMap(kv.lastFreezeState),
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(snapshot)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot struct {
		Kvs             map[string]KeyValueStore
		LastOps         map[int64]lastOperation
		Shard2Gid       map[int]int
		LastFreezeNum   map[int]int
		LastInstallNum  map[int]int
		LastDeleteNum   map[int]int
		LastFreezeState map[int][]byte
	}

	if d.Decode(&snapshot) != nil {
		return
	}

	kv.Kvs = snapshot.Kvs
	kv.lastOps = snapshot.LastOps
	kv.shard2gid = snapshot.Shard2Gid
	kv.lastFreezeNum = snapshot.LastFreezeNum
	kv.lastInstallNum = snapshot.LastInstallNum
	kv.lastDeleteNum = snapshot.LastDeleteNum
	kv.lastFreezeState = snapshot.LastFreezeState

	// Ensure maps are not nil
	if kv.Kvs == nil {
		kv.Kvs = make(map[string]KeyValueStore)
	}
	if kv.lastOps == nil {
		kv.lastOps = make(map[int64]lastOperation)
	}
	if kv.shard2gid == nil {
		kv.shard2gid = make(map[int]int)
	}
	if kv.lastFreezeNum == nil {
		kv.lastFreezeNum = make(map[int]int)
	}
	if kv.lastInstallNum == nil {
		kv.lastInstallNum = make(map[int]int)
	}
	if kv.lastDeleteNum == nil {
		kv.lastDeleteNum = make(map[int]int)
	}
	if kv.lastFreezeState == nil {
		kv.lastFreezeState = make(map[int][]byte)
	}
}

// RPC handlers
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, result := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Value = ""
		reply.Version = 0
		reply.Err = err
		return
	}
	*reply = result.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, result := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(rpc.PutReply)
}

func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, result := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = *result.(*shardrpc.FreezeShardReply)
}

func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, result := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = *result.(*shardrpc.InstallShardReply)
}

func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, result := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = *result.(*shardrpc.DeleteShardReply)
}

// Helper functions
func copyKvs(src map[string]KeyValueStore) map[string]KeyValueStore {
	dst := make(map[string]KeyValueStore, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyLastOps(src map[int64]lastOperation) map[int64]lastOperation {
	dst := make(map[int64]lastOperation, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyIntMap(src map[int]int) map[int]int {
	dst := make(map[int]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyByteMap(src map[int][]byte) map[int][]byte {
	dst := make(map[int][]byte, len(src))
	for k, v := range src {
		dst[k] = make([]byte, len(v))
		copy(dst[k], v)
	}
	return dst
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Note: RSM doesn't have a Kill method, it's handled by the framework
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// Register types for labgob
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(getOp{})
	labgob.Register(putOp{})
	labgob.Register(freezeOp{})
	labgob.Register(installOp{})
	labgob.Register(deleteOp{})
	labgob.Register(getResult{})
	labgob.Register(putResult{})
	labgob.Register(freezeResult{})
	labgob.Register(installResult{})
	labgob.Register(deleteResult{})
	labgob.Register(KeyValueStore{})

	kv := &KVServer{
		gid:             gid,
		me:              me,
		Kvs:             make(map[string]KeyValueStore),
		lastOps:         make(map[int64]lastOperation),
		shard2gid:       make(map[int]int),
		lastFreezeNum:   make(map[int]int),
		lastInstallNum:  make(map[int]int),
		lastDeleteNum:   make(map[int]int),
		lastFreezeState: make(map[int][]byte),
	}

	// Initialize all shards for Gid1 if it's the first group
	if gid == shardcfg.Gid1 {
		for i := 0; i < shardcfg.NShards; i++ {
			kv.shard2gid[i] = int(shardcfg.Gid1)
		}
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []tester.IService{kv, kv.rsm.Raft()}
}
