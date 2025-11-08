package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu       sync.Mutex
	keyValue map[string]string       // 存储键值对
	versions map[string]rpc.Tversion // 存储每个键的版本号，其实是给client 用的，防止并发
	lastOps  map[int64]lastOperation // 记录每个客户端最后一次请求
}

type lastOperation struct {
	Seq   int64
	Reply rpc.PutReply
}

// 初始状态：
// x = "100元", version = 1
//
// 时间线：
// T1: 客户端A Get(x) → value="100元", version=1
// T2: 客户端B Get(x) → value="100元", version=1
//
// T3: 客户端A 计算：100 - 50 = 50
// Put(x, "50元", version=1)
// → 检查：当前version=1，匹配！✅
// → 更新：x="50元", version=2
// → 返回：OK
//
// T4: 客户端B 计算：100 - 30 = 70
// Put(x, "70元", version=1)
// → 检查：当前version=2，不匹配！❌
// → 返回：ErrVersion (版本冲突)
//
// T5: 客户端B 收到 ErrVersion，重新操作：
// Get(x) → value="50元", version=2
// 计算：50 - 30 = 20
// Put(x, "20元", version=2)
// → 检查：当前version=2，匹配！✅
// → 更新：x="20元", version=3
// → 返回：OK
//
// 最终结果：x = "20元", version = 3 ✅ 正确！
// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 根据请求类型处理
	switch args := req.(type) {
	case rpc.GetArgs:
		// 处理Get请求
		value, exists := kv.keyValue[args.Key]
		if !exists {
			return rpc.GetReply{
				Value:   "",
				Version: 0,
				Err:     rpc.ErrNoKey,
			}
		}
		version := kv.versions[args.Key]
		return rpc.GetReply{
			Value:   value,
			Version: version,
			Err:     rpc.OK,
		}

	case rpc.PutArgs:
		// 幂等性检查：如果已经处理过该客户端的该请求，直接返回之前的结果
		if record, ok := kv.lastOps[args.ClientId]; ok && args.Seq <= record.Seq {
			return record.Reply
		}

		// 处理Put请求
		currentVersion, exists := kv.versions[args.Key]
		if !exists {
			currentVersion = 0
		}

		// 检查版本是否匹配
		// cap
		var reply rpc.PutReply
		if args.Version != currentVersion {
			reply = rpc.PutReply{
				Err: rpc.ErrVersion,
			}
		} else {
			// 更新键值对和版本
			kv.keyValue[args.Key] = args.Value
			kv.versions[args.Key] = currentVersion + 1

			reply = rpc.PutReply{
				Err: rpc.OK,
			}
		}

		// 记录最新处理的客户端请求，确保去重
		kv.lastOps[args.ClientId] = lastOperation{
			Seq:   args.Seq,
			Reply: reply,
		}
		return reply

	default:
		// 未知请求类型
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// 使用rsm.Submit提交Get请求到Raft
	//注意这里也需要用submit，为了线形一致性
	//Leader 尝试将 Get 操作添加到日志
	//需要与大多数节点通信（心跳/追加日志）
	//如果这个节点不再是真正的Leader，会失败并返回 ErrWrongLeader
	//只有真正的Leader才能成功提交
	//确保读取的是最新提交的数据
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Value = ""
		reply.Version = 0
		reply.Err = err
		return
	}

	// 将结果转换为GetReply
	*reply = result.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// 使用rsm.Submit提交Put请求到Raft
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	// 将结果转换为PutReply
	*reply = result.(rpc.PutReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me:       me,
		keyValue: make(map[string]string),
		versions: make(map[string]rpc.Tversion),
		lastOps:  make(map[int64]lastOperation),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}

//线性一致性要求：
//> 如果操作A在操作B开始之前完成，那么A必须在B之前生效。
//注意：是完成时间，不是发起时间！
//客户端发送Put(x,100), ID=123
//- 第一次：网络超时，客户端重试
//- 第二次：同样的Put(x,100), ID=123
//- Raft可能应用两次，但ID相同
//- 状态机只执行一次，返回相同结果
//- 客户端收到正确响应

//初始：x = "v0", version = 0
//
//并发操作：
//- A: Get(x) → ("v0", 0)
//- B: Get(x) → ("v0", 0)
//- C: Get(x) → ("v0", 0)
//
//A: Put(x, "A", 0) → 成功！x = "A", version = 1
//B: Put(x, "B", 0) → 失败！version不匹配（现在是1）
//C: Put(x, "C", 0) → 失败！version不匹配（现在是1）
//
//B重试：
//- Get(x) → ("A", 1)
//- Put(x, "B", 1) → 成功！x = "B", version = 2
//
//C重试：
//- Get(x) → ("B", 2)
//- Put(x, "C", 2) → 成功！x = "C", version = 3
//
//最终顺序：A → B → C

//时间线（真实时间）：
//
//T1: 客户端A发送 Put(x, "A", v=5) [操作A开始]
//T2: Leader1写入日志
//T3: 客户端C发送 Put(x, "C", v=5) [操作C开始]
//T4: Leader1宕机
//T5: Leader2当选，覆盖日志，提交 Put(x, "C", v=5)
//T6: 客户端C收到成功响应 [操作C完成 ✓]
//T7: 客户端A收到 ErrMaybe [操作A未完成！]
//T8: 客户端A重新Get
//T9: 客户端A重新Put
//T10: 客户端A收到成功响应 [操作A完成 ✓]
