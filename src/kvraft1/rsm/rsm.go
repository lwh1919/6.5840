package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

//每个台服务器都是一个 状态机（数据库）+ rsm（被封装的数据管家）+ raft算法-rpc调用（保证数据一致性）

// op-operation 也就是对rsm的一次操作的封装，通过id来找 响应
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id  int64 // 唯一操作ID
	Req any   // 实际的请求内容
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
// 一个希望实现自我复制的服务器（例如，在 ../server.go中）会调用 MakeRSM，
// 并且必须实现 StateMachine接口。这个接口允许 rsm包与服务器进行交互，
// 以执行特定于服务器的操作：服务器必须实现 DoOp方法来执行一个操作
// （例如，一个 Get 或 Put 请求），并实现 Snapshot/Restore方法来
// 对服务器的状态进行快照和恢复。
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

// 日志复制中间器的抽象
type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft          //rf节点，是leader就操作，不会就重定向到raft
	applyCh      chan raftapi.ApplyMsg //接收leader的反馈
	maxraftstate int                   // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	nextId      int64
	mapChan     map[int]*OPrsChan
	lastApplied int
}
type OPrsChan struct {
	Opid     int64
	Term     int
	ResultCh chan OpResult
}
type OpResult struct {
	V   any // DoOp()的返回值
	Err rpc.Err
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
// servers[] 包含了那些将通过 Raft 协议进行协作、
// 共同构成容错键值服务的一组服务器的端口号。

// me 是当前服务器在 servers[] 中的索引。

// 键值（k/v）服务器应通过底层的 Raft 实现来存储快照。
// Raft 会调用 persister.SaveStateAndSnapshot() 来原子性地保存其状态以及快照。
// 当 Raft 已保存的状态大小超过 maxraftstate 字节时，RSM 应当生成快照，
// 以便让 Raft 能够对其日志进行垃圾回收。如果 maxraftstate 为 -1，
// 则表示您不需要生成快照。

// MakeRSM() 函数必须快速返回，因此对于任何耗时较长的任务，
// 它应该启动新的 goroutine 来执行。
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		nextId:       0,
		mapChan:      make(map[int]*OPrsChan),
		lastApplied:  0,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	// 恢复快照（如果存在）
	if maxraftstate != -1 {
		snapshot := persister.ReadSnapshot()
		if len(snapshot) > 0 {
			rsm.sm.Restore(snapshot)
		}
	}
	go rsm.reader()

	return rsm
}
func (rsm *RSM) handleSnapshot(msg raftapi.ApplyMsg) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	// 更新lastApplied
	if msg.SnapshotIndex > rsm.lastApplied {
		rsm.lastApplied = msg.SnapshotIndex

		// 清理过期的等待通道
		for index := range rsm.mapChan {
			if index <= msg.SnapshotIndex {
				delete(rsm.mapChan, index)
			}
		}

		// 恢复状态机状态
		rsm.sm.Restore(msg.Snapshot)
	}
}

func (rsm *RSM) checkSnapshot(appliedIndex int) {
	if rsm.maxraftstate == -1 {
		return // 不需要快照
	}

	// 检查Raft状态大小
	if rsm.rf.PersistBytes() >= rsm.maxraftstate {
		// 创建快照
		snapshot := rsm.sm.Snapshot()
		rsm.rf.Snapshot(appliedIndex, snapshot)
	}
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh { //go特色语法糖

		if !msg.CommandValid {
			continue
		}
		if msg.SnapshotValid {
			// 处理快照消息
			rsm.handleSnapshot(msg)
			continue
		}
		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		// 调用StateMachine的DoOp执行操作 - 不持有锁避免死锁
		value := rsm.sm.DoOp(op.Req)

		rsm.mu.Lock()
		rsm.lastApplied = msg.CommandIndex

		var opres *OPrsChan
		if found, exists := rsm.mapChan[msg.CommandIndex]; exists {
			opres = found
		}
		rsm.mu.Unlock()

		// 在锁外发送结果，避免死锁
		if opres != nil {
			// 检查操作ID是否匹配
			var ressult OpResult
			if opres.Opid == op.Id {
				// 发送成功结果
				ressult = OpResult{
					V:   value,
					Err: rpc.OK,
				}
			} else {
				// 如果ID不匹配，发送错误让Submit继续
				ressult = OpResult{
					V:   nil,
					Err: rpc.ErrWrongLeader,
				}
			}
			opres.ResultCh <- ressult
		}
		// 检查是否需要创建快照
		rsm.checkSnapshot(msg.CommandIndex)
	}
}

// 关于	if opres.Opid == op.Id ，我原本有以下疑问 ,他只能防止同一个操作A的重复提交
// 集群有两台服务器，分别运行 RSM-A（原 Leader 节点）和 RSM-B（新 Leader 节点）。
// RSM-A 提交操作 OpA，生成 Id = 10，把它交给原 Leader 的 Raft（假设 index = 42）。
// 原 Leader 在多数派提交之前就挂了。
// RSM-B 成为新 Leader，并提交自己的操作 OpB，它的 nextId 恰好也跑到了 10，于是 OpB.Id = 10。新 Leader 提交成功，把日志写到了 index = 42。
// 问题：当 RSM-A 恢复时，Raft 会把 index = 42 的日志应用给它，里头的 Command 等于 OpB{Id:10, ...}。RSM-A 的 reader() 收到这个消息时，发现 mapChan[42].Opid 埋的是 10（提交 OpA 时记录的），而 op.Id 也等于 10（这是 OpB），两者相等，于是这个判断会进入“发送成功结果”。看起来 RSM-A 就会错把 OpB 当成自己的结果返回给客户端。
// 但是实际上不会出现这种情况
// Submit() 定期检查是否还是 Leader；一旦掉队就清理 mapChan[index]，返回 ErrWrongLeader，客户端重新找 Leader。
// 日志应用到旧 Leader 时，mapChan[index] 已经不存在（或在未来几毫秒内被删除），reader() 自然不会把 OpB 的结果发回给 OpA 的客户端。

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	rsm.mu.Lock()
	opReq := Op{
		Id:  rsm.nextId,
		Req: req,
	}
	rsm.nextId++
	rsm.mu.Unlock()

	index, term, isLeader := rsm.rf.Start(opReq)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}
	//创建接收通道
	rsm.mu.Lock()
	resChan := make(chan OpResult, 1) //细节是1，和非阻塞相对应
	rsm.mapChan[index] = &OPrsChan{
		Opid:     opReq.Id,
		Term:     term,
		ResultCh: resChan,
	}
	rsm.mu.Unlock()

	// 同时定期检查term是否改变（说明leader已改变）,只想leader写操作
	ticker := time.NewTicker(1 * time.Millisecond)
	timeout := time.After(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case res := <-resChan:
			rsm.mu.Lock()
			delete(rsm.mapChan, index)
			rsm.mu.Unlock()
			return res.Err, res.V

		case <-ticker.C:
			nowTerm, isLeader := rsm.rf.GetState()
			if !isLeader || nowTerm != term {
				rsm.mu.Lock()
				delete(rsm.mapChan, index)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}

		case <-timeout:
			// 防止超长测试的goroutine超时
			rsm.mu.Lock()
			delete(rsm.mapChan, index)
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
	}
}
