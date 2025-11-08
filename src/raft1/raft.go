package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	//Write-Ahead Logging (WAL)
	//这个原则实际上来自数据库系统的 Write-Ahead Logging 概念：
	//
	//- 规则 ：在修改数据之前，必须先将修改记录写入日志
	//- 目的 ：确保即使在崩溃时也能恢复到一致状态
	//- 在 Raft 中 ：在响应 RPC 之前，必须先持久化状态变化
	//也就是持久化变量一旦变化就要持久化，且要加锁，且持久化才能发送信息

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//持久化
	currentTerm int //服务器最后一次知道的任期号，0
	votedFor    int //该服务器投给谁，-1，持久化，同义任期重复的投票
	Logs        []LogEntry

	//
	CommitIndex int // 已知最大提交索引
	LastApplied int // 最后被应用于状态机的索引
	//
	NextIndex  []int
	MatchIndex []int

	State     int
	Heartbeat time.Time

	ElectionTimeout time.Duration

	// 防止并发的LeaderAppendEntries调用
	appendInProgress bool

	ApplyCh chan raftapi.ApplyMsg

	// 条件变量用于applier函数的优化
	applyCond *sync.Cond

	// 日志快照压缩
	lastKzTerm  int
	lastKzIndex int
	data        []byte
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int //日志压缩才有用
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.State == 1 {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(sna []byte) {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.lastKzTerm)
	e.Encode(rf.lastKzIndex)
	raftstate := w.Bytes()

	if sna == nil {
		sna = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, sna)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data) //将静态的字节切片转换为可读取的流。
	d := labgob.NewDecoder(r)  //创建一个能理解特定编码规则的解释器
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastKzTerm int
	var lastKzIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastKzTerm) != nil ||
		d.Decode(&lastKzIndex) != nil {

		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.Logs = logs
		rf.lastKzTerm = lastKzTerm
		rf.lastKzIndex = lastKzIndex
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 创建快照并压缩日志：
// 检查快照索引是否有效（不能比现有快照旧，不能超过最后日志）
// 获取快照位置的任期号
// 创建新的日志数组，保留虚拟节点和快照之后的日志
// 更新快照状态
// 持久化新状态
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastKzIndex {
		return
	}

	if index > rf.GetLastIndexBySna() {
		return
	}

	newLastKzTerm := rf.Logs[rf.GetNewIndex(index)].Term

	newLogs := make([]LogEntry, 0)
	newLogs = append(newLogs, LogEntry{Term: -1, Command: nil, Index: 0}) // 虚拟节点

	// 保留 index 之后的日志
	if rf.GetNewIndex(index) < len(rf.Logs)-1 {
		newLogs = append(newLogs, rf.Logs[rf.GetNewIndex(index)+1:]...)
	}

	// 更新日志数组
	rf.Logs = newLogs

	// 更新快照索引和任期
	rf.lastKzIndex = index
	rf.lastKzTerm = newLastKzTerm

	// 持久化 Raft 状态和快照
	rf.persist(snapshot)

	DPrintf("[%d] Snapshot created, lastKzIndex=%d, lastKzTerm=%d, remaining logs=%d\n",
		rf.me, rf.lastKzIndex, rf.lastKzTerm, len(rf.Logs))
}

type RequestSnapshot struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type ReplySnapshot struct {
	Term    int
	Success bool
}

// 严重落后的时候传日志
func (rf *Raft) InstallSnapshot(args *RequestSnapshot, reply *ReplySnapshot) {
	//但是还是要做检验，万一这个请求很落后呢
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false

	// 1. 任期检查
	if rf.currentTerm > args.Term {
		return
	}

	// 更新心跳时间
	rf.Heartbeat = time.Now()

	// 2. 如果任期更新，转为 Follower
	if rf.currentTerm < args.Term {
		rf.ToFollower(args.Term)
	}

	// 3. 如果快照比当前快照旧，拒绝
	if args.LastIncludedIndex <= rf.lastKzIndex {
		reply.Success = true //也当作成功了
		return
	}

	reply.Success = true

	// 4. 准备发送快照到 applyCh
	applyMsg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	// 5. 根据情况处理日志
	if args.LastIncludedIndex >= rf.GetLastIndexBySna() {
		// 快照包含所有日志，清空日志但保留虚拟节点
		rf.Logs = []LogEntry{{Term: -1, Command: nil, Index: 0}}
	} else if args.LastIncludedTerm != rf.Logs[rf.GetNewIndex(args.LastIncludedIndex)].Term {
		// 快照位置的日志任期不匹配，说明冲突，丢弃所有日志
		rf.Logs = []LogEntry{{Term: -1, Command: nil, Index: 0}}
	} else {
		// 快照位置的日志匹配，保留快照之后的日志
		newLogs := make([]LogEntry, 0)
		newLogs = append(newLogs, LogEntry{Term: -1, Command: nil, Index: 0})
		if rf.GetNewIndex(args.LastIncludedIndex)+1 < len(rf.Logs) {
			newLogs = append(newLogs, rf.Logs[rf.GetNewIndex(args.LastIncludedIndex)+1:]...)
		}
		rf.Logs = newLogs
	}

	// 6. 更新快照状态
	rf.lastKzIndex = args.LastIncludedIndex
	rf.lastKzTerm = args.LastIncludedTerm

	// 7. 更新 CommitIndex 和 LastApplied
	if rf.CommitIndex < args.LastIncludedIndex {
		rf.CommitIndex = args.LastIncludedIndex
	}
	if rf.LastApplied < args.LastIncludedIndex {
		rf.LastApplied = args.LastIncludedIndex
	}

	// 8. 持久化
	rf.persist(args.Data)

	// 9. 发送快照到 applyCh（在释放锁之后）
	go func() {
		rf.ApplyCh <- applyMsg
	}()
}

func (rf *Raft) SendSnapshot(server int, args *RequestSnapshot, reply *ReplySnapshot) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int // 冲突条目的任期号
	XIndex int // 该任期的第一个条目索引
	XLen   int // 日志长度
	Xlkz   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//init
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.Xlkz = rf.lastKzIndex

	DPrintf("[%d] AppendEntries from %d: term=%d (myTerm=%d), PrevLogIndex=%d, PrevLogTerm=%d, entries=%d, leaderCommit=%d\n",
		rf.me, args.LeaderId, args.Term, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)

	// 1. 任期检查
	if args.Term < rf.currentTerm {
		return
	}
	//记录心跳时间
	rf.Heartbeat = time.Now()

	// 只有在任期变化时才调用ToFollower，避免不必要的持久化
	if args.Term > rf.currentTerm {
		rf.ToFollower(args.Term)
	} else if rf.State != -1 {
		// 同任期但状态不是Follower，转为Follower但不持久化
		rf.State = -1
	}

	// 检查日志一致性
	logConsistent := false
	if args.PrevLogIndex == -1 {
		// PrevLogIndex为-1表示从头开始，总是一致的
		logConsistent = true
	} else if args.PrevLogIndex < rf.lastKzIndex {
		// PrevLogIndex 在快照范围内，说明这部分已经被快照包含
		// 这种情况下认为一致，因为快照已经包含了这些日志
		logConsistent = true
	} else if args.PrevLogIndex == rf.lastKzIndex {
		// PrevLogIndex 正好是快照的最后一个索引
		if args.PrevLogTerm == rf.lastKzTerm {
			logConsistent = true
		} else {
			// 快照的任期不匹配，告诉Leader需要发送快照
			reply.XTerm = rf.lastKzTerm
			reply.XIndex = 0 // 表示在快照范围内
		}
	} else if rf.GetNewIndex(args.PrevLogIndex) < len(rf.Logs) {
		// PrevLogIndex 在当前日志范围内
		if rf.Logs[rf.GetNewIndex(args.PrevLogIndex)].Term == args.PrevLogTerm {
			logConsistent = true
		} else {
			// 日志不一致，设置快速回退优化字段
			reply.XTerm = rf.Logs[rf.GetNewIndex(args.PrevLogIndex)].Term
			// 找到该任期的第一个条目
			firstIndexOfTerm := rf.GetNewIndex(args.PrevLogIndex)
			for i := rf.GetNewIndex(args.PrevLogIndex); i >= 1; i-- {
				if rf.Logs[i].Term != reply.XTerm {
					firstIndexOfTerm = i + 1
					break
				}
				if i == 1 {
					// 到达虚拟节点之后，说明这是第一个条目
					firstIndexOfTerm = 1
				}
			}
			// 转换为实际索引返回给Leader
			reply.XIndex = rf.lastKzIndex + firstIndexOfTerm
		}
	} else {
		// PrevLogIndex 超出日志范围，follower 的日志太短
		reply.XLen = len(rf.Logs)
		DPrintf("[%d] Log too short: PrevLogIndex=%d, myLogLen=%d, lastKzIndex=%d\n",
			rf.me, args.PrevLogIndex, len(rf.Logs), rf.lastKzIndex)
	}

	if !logConsistent {
		DPrintf("[%d] Log inconsistent: XTerm=%d, XIndex=%d, XLen=%d\n",
			rf.me, reply.XTerm, reply.XIndex, reply.XLen)
	}

	//开始日志复制
	if len(args.Entries) > 0 {
		if logConsistent {
			// 如果 PrevLogIndex 在快照范围内，需要跳过/删除已在快照中的日志
			startIdx := 0
			if args.PrevLogIndex < rf.lastKzIndex {
				skipCount := rf.lastKzIndex - args.PrevLogIndex
				if skipCount >= len(args.Entries) {
					reply.Success = true
				} else {
					startIdx = skipCount
					rf.Logs = rf.Logs[:1] // 只保留虚拟节点
					rf.Logs = append(rf.Logs, args.Entries[startIdx:]...)
					rf.persist(nil)
					reply.Success = true
					DPrintf("[%d] Appended entries from leader %d (skipped %d in snapshot), log length: %d\n",
						rf.me, args.LeaderId, startIdx, len(rf.Logs))
				}
			} else {
				// 正常情况：删除从PrevLogIndex+1开始的所有现有日志条目
				// 使用 GetNewIndex 转换实际索引到日志数组索引
				newIndex := rf.GetNewIndex(args.PrevLogIndex + 1)
				rf.Logs = rf.Logs[:newIndex]
				// 追加所有新的日志条目
				rf.Logs = append(rf.Logs, args.Entries...)
				rf.persist(nil)
				reply.Success = true
				DPrintf("[%d] Appended entries from leader %d, log length: %d\n", rf.me, args.LeaderId, len(rf.Logs))
			}
		}
	} else {
		//心跳包 - 只有在日志一致时才成功
		if logConsistent {
			reply.Success = true
		}
	}

	//更新commitIndex - 只在日志一致且成功时推进
	if args.LeaderCommit > rf.CommitIndex && reply.Success {
		rf.CommitIndex = min(args.LeaderCommit, rf.GetLastIndexBySna())
		DPrintf("[%d] Updated commitIndex to %d\n", rf.me, rf.CommitIndex)
		// 通知applier有新的日志可以应用
		rf.applyCond.Signal()
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) GetLogsLastTerm() int {
	LogsLastIndex := rf.GetLogsLastIndex()
	if LogsLastIndex < 0 {
		return 0
	}
	return rf.Logs[LogsLastIndex].Term
}

func (rf *Raft) GetLogsLastIndex() int {
	return len(rf.Logs) - 1
}

func (rf *Raft) GetLastTermBySna() int {
	LogsLastArrayIndex := len(rf.Logs) - 1
	if LogsLastArrayIndex <= 0 {
		// 如果只有虚拟节点或没有日志，返回快照的任期,这一点很重要
		return rf.lastKzTerm
	}
	return rf.Logs[LogsLastArrayIndex].Term
}

func (rf *Raft) GetLastIndexBySna() int {
	return len(rf.Logs) - 1 + rf.lastKzIndex
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 初始化回复
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	DPrintf("[%d] RequestVote from %d: term=%d (myTerm=%d), lastLogIndex=%d, lastLogTerm=%d (myLastIndex=%d, myLastTerm=%d)\n",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm,
		rf.GetLastIndexBySna(), rf.GetLastTermBySna())

	// 1. 任期检查
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.ToFollower(args.Term)
	}

	// 2. 日志新旧程度检查 - 候选人的日志必须至少和自己的一样新
	myLastLogTerm := rf.GetLastTermBySna()
	myLastLogIndex := rf.GetLastIndexBySna()

	logUpToDate := false
	if args.LastLogTerm > myLastLogTerm {
		logUpToDate = true
	} else if args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex {
		logUpToDate = true
	}

	if !logUpToDate {
		return
	}

	//处理同任期下 已经票投了的情况
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// 本任期已经投给了其他人，拒绝投票
		return
	}

	//所有的选举通过
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persist(nil)
	DPrintf("[%d] Voted for %d in term %d\n", rf.me, rf.votedFor, rf.currentTerm)
}

func (rf *Raft) ToFollower(Term int) {
	rf.currentTerm = Term
	rf.votedFor = -1
	rf.State = -1
	rf.persist(nil)
	rf.ElectionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	DPrintf("[%d] -> Follower, term=%d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) ToLeader() {
	rf.State = 1
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex[i] = rf.GetLastIndexBySna() + 1
		rf.MatchIndex[i] = 0
	}
	rf.MatchIndex[rf.me] = rf.GetLastIndexBySna()
	go rf.HeartbeatLoop()
	DPrintf("[%d] -> Leader, term=%d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// 等待有新的日志需要应用
		for rf.LastApplied >= rf.CommitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}

		if rf.killed() {
			rf.mu.Unlock()
			break
		}

		// 批量应用日志
		var applyMsgs []raftapi.ApplyMsg
		commitIndex := rf.CommitIndex
		lastKzIndex := rf.lastKzIndex

		for rf.LastApplied < commitIndex {
			rf.LastApplied++
			if rf.LastApplied <= lastKzIndex {
				continue
			}

			logIndex := rf.GetNewIndex(rf.LastApplied)
			if logIndex >= 0 && logIndex < len(rf.Logs) {
				applyMsg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.Logs[logIndex].Command,
					CommandIndex: rf.LastApplied,
				}
				applyMsgs = append(applyMsgs, applyMsg)
			}
		}
		rf.mu.Unlock()

		// 批量发送消息
		for _, msg := range applyMsgs {
			rf.ApplyCh <- msg
		}
	}
}

func (rf *Raft) GetNewIndex(i int) int {
	if i > rf.lastKzIndex {
		return i - rf.lastKzIndex
	} else {
		return 0
	}
}

// tryCommit 尝试提交新的日志条目（调用时需要持有锁）

func (rf *Raft) tryCommit() {
	if rf.State != 1 || rf.CommitIndex >= rf.GetLastIndexBySna() {
		return
	}

	// 从后往前遍历，找到最后一个当前任期的、被多数派复制的日志
	// 优化：提前计算多数派阈值，减少重复计算
	majority := len(rf.peers) / 2
	lastIndex := rf.GetLastIndexBySna()

	for N := lastIndex; N > rf.CommitIndex; N-- {
		// 检查日志N是否为当前任期
		logIndex := rf.GetNewIndex(N)
		if logIndex < 0 || logIndex >= len(rf.Logs) {
			continue
		}

		if rf.Logs[logIndex].Term == rf.currentTerm {
			count := 0
			// 优化：提前终止循环
			for i := 0; i < len(rf.peers) && count <= majority; i++ {
				if rf.MatchIndex[i] >= N {
					count++
				}
			}

			if count > majority {
				rf.CommitIndex = N
				DPrintf("[%d] Advanced commitIndex to %d (term=%d)\n",
					rf.me, rf.CommitIndex, rf.Logs[logIndex].Term)
				// 通知applier有新的日志可以应用
				rf.applyCond.Signal()
				return
			}
		}
	}
}

func (rf *Raft) LeaderAppendEntries() {
	rf.mu.Lock()
	if rf.State != 1 || rf.appendInProgress {
		rf.mu.Unlock()
		return
	}
	rf.appendInProgress = true
	rf.mu.Unlock()

	defer func() {
		rf.mu.Lock()
		rf.appendInProgress = false
		rf.mu.Unlock()
	}()

	var cnt atomic.Int32
	cnt.Store(1) // Leader自己算一票
	var wg sync.WaitGroup

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rf.replicateToPeer(i, &cnt)
		}(i)
	}

	// 等待所有复制完成或超时
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 所有RPC完成
	case <-time.After(50 * time.Millisecond):
		// 超时
	}

	// 检查提交
	rf.mu.Lock()
	if rf.State == 1 {
		rf.tryCommit()
	}
	rf.mu.Unlock()
}

func (rf *Raft) replicateToPeer(peer int, cnt *atomic.Int32) {
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if rf.State != 1 {
			rf.mu.Unlock()
			return
		}

		prevLogIndex := rf.NextIndex[peer] - 1

		// 如果需要发送快照
		if prevLogIndex < rf.lastKzIndex {
			snapshotData := rf.persister.ReadSnapshot()
			req := RequestSnapshot{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastKzIndex,
				LastIncludedTerm:  rf.lastKzTerm,
				Data:              snapshotData,
			}
			rf.mu.Unlock()

			var reply ReplySnapshot
			ok := rf.SendSnapshot(peer, &req, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.ToFollower(reply.Term)
				rf.mu.Unlock()
				return
			}
			if reply.Success {
				rf.NextIndex[peer] = rf.lastKzIndex + 1
				rf.MatchIndex[peer] = rf.lastKzIndex
				cnt.Add(1)
			}
			rf.mu.Unlock()
			continue
		}

		// 准备AppendEntries请求
		req := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: rf.CommitIndex,
		}

		// 设置PrevLogTerm
		if prevLogIndex == rf.lastKzIndex {
			req.PrevLogTerm = rf.lastKzTerm
		} else if prevLogIndex >= 0 && prevLogIndex <= rf.GetLastIndexBySna() {
			req.PrevLogTerm = rf.Logs[rf.GetNewIndex(prevLogIndex)].Term
		}

		// 复制日志条目
		if rf.NextIndex[peer] <= rf.GetLastIndexBySna() {
			req.Entries = make([]LogEntry, len(rf.Logs[rf.GetNewIndex(rf.NextIndex[peer]):]))
			copy(req.Entries, rf.Logs[rf.GetNewIndex(rf.NextIndex[peer]):])
		}
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		ok := rf.SendAppendEntries(peer, &req, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.ToFollower(reply.Term)
			rf.mu.Unlock()
			return
		}
		if rf.State != 1 {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			cnt.Add(1)
			rf.NextIndex[peer] = prevLogIndex + 1 + len(req.Entries)
			rf.MatchIndex[peer] = rf.NextIndex[peer] - 1
			rf.mu.Unlock()
			return
		} else {
			// 快速回退优化
			if reply.XLen > 0 {
				rf.NextIndex[peer] = reply.Xlkz + reply.XLen
			} else if reply.XTerm > 0 {
				if reply.XIndex == 0 {
					// 需要发送快照
					rf.mu.Unlock()
					continue
				}

				// 查找XTerm的最后一个条目
				hasXTerm := false
				lastXTermIndex := -1
				for j := len(rf.Logs) - 1; j >= 1; j-- {
					if rf.Logs[j].Term == reply.XTerm {
						hasXTerm = true
						lastXTermIndex = rf.lastKzIndex + j
						break
					}
					if rf.Logs[j].Term < reply.XTerm {
						break
					}
				}

				if hasXTerm {
					rf.NextIndex[peer] = lastXTermIndex + 1
				} else {
					rf.NextIndex[peer] = reply.XIndex
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) HeartbeatLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.State != 1 {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// 发送心跳
		rf.sendHeartbeat()

		// 检查是否需要复制日志
		rf.mu.Lock()
		hasNewEntries := false
		lastIndex := rf.GetLastIndexBySna()
		for j := 0; j < len(rf.peers) && !hasNewEntries; j++ {
			if j != rf.me && rf.NextIndex[j] <= lastIndex {
				hasNewEntries = true
			}
		}
		if hasNewEntries {
			go rf.LeaderAppendEntries()
		}
		rf.mu.Unlock()

		time.Sleep(50 * time.Millisecond) // 减少心跳间隔
	}
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	lastIndex := rf.GetLastIndexBySna()
	lastTerm := rf.GetLastTermBySna()
	req := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: lastIndex,
		PrevLogTerm:  lastTerm,
		Entries:      []LogEntry{},
		LeaderCommit: rf.CommitIndex,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			if rf.killed() {
				return
			}
			var reply AppendEntriesReply
			ok := rf.SendAppendEntries(i, &req, &reply)
			if ok && !rf.killed() {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.ToFollower(reply.Term)
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) ToCandidate() {
	rf.State = 0
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.persist(nil)
	DPrintf("[%d] -> Candidate, term=%d\n", rf.me, rf.currentTerm)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.State == 1

	if !isLeader {
		return -1, term, false
	}

	index := rf.GetLastIndexBySna() + 1
	newLog := LogEntry{
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.Logs = append(rf.Logs, newLog)

	// Leader添加日志后，立即更新自己的MatchIndex
	rf.MatchIndex[rf.me] = index

	rf.persist(nil)
	DPrintf("[%d] Start: new command, index=%d, term=%d, MatchIndex[me]=%d\n",
		rf.me, index, term, rf.MatchIndex[rf.me])

	go rf.LeaderAppendEntries() // 由新日志触发AppendEntries
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// 唤醒可能在等待的applier goroutine
	rf.applyCond.Signal()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		// 只有Follower和Candidate需要检查选举超时
		if rf.State != 1 {
			// 检查是否超过选举超时时间
			if time.Since(rf.Heartbeat) > rf.ElectionTimeout {
				// 触发新的选举
				rf.mu.Unlock()
				rf.startElection()
				rf.mu.Lock()
			}
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 30 and 100 milliseconds
		ms := 30 + (rand.Int63() % 70)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.State == 1 {
		rf.mu.Unlock()
		return
	}
	rf.ToCandidate()
	currentElectionTerm := rf.currentTerm
	req := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.GetLastIndexBySna(),
		LastLogTerm:  rf.GetLastTermBySna(),
	}
	DPrintf("[%d] Starting election, term=%d\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	var cnt atomic.Int32
	cnt.Store(1) // 给自己投票
	WinNeed := int32(len(rf.peers) / 2)
	timeoutCh := time.After(time.Duration(200) * time.Millisecond) // 减少选举超时时间
	resultCh := make(chan bool, 1)

	// 使用WaitGroup确保所有goroutine完成
	var wg sync.WaitGroup

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if rf.killed() {
				return
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &req, &reply)
			if ok && !rf.killed() {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.ToFollower(reply.Term)
					rf.mu.Unlock()
					select {
					case resultCh <- false:
					default:
					}
					return
				}
				if rf.State != 0 || rf.currentTerm != currentElectionTerm {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				if reply.VoteGranted {
					newCount := cnt.Add(1)
					if newCount > WinNeed {
						select {
						case resultCh <- true:
						default:
						}
					}
				}
			}
		}(i)
	}

	// 启动一个goroutine等待所有RPC完成
	go func() {
		wg.Wait()
		// 如果超时前所有RPC都完成了，但没有获得足够票数，发送失败信号
		select {
		case resultCh <- false:
		default:
		}
	}()

	select {
	case won := <-resultCh:
		if won {
			rf.mu.Lock()
			if rf.State == 0 && rf.currentTerm == currentElectionTerm {
				rf.ToLeader()
			}
			rf.mu.Unlock()
		}
	case <-timeoutCh:
		DPrintf("[%d] Election timeout, term=%d\n", rf.me, currentElectionTerm)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.mu = sync.Mutex{}
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.State = -1
	rf.dead = 0
	rf.LastApplied = 0
	rf.Logs = make([]LogEntry, 1)
	rf.Logs[0] = LogEntry{-1, nil, 0} // dummy entry at index 0
	rf.CommitIndex = 0
	rf.MatchIndex = make([]int, len(rf.peers))
	rf.NextIndex = make([]int, len(rf.peers))
	rf.ApplyCh = applyCh
	rf.lastKzIndex = 0
	rf.lastKzTerm = -1 // 修正：应该是 -1 匹配虚拟节点
	rf.data = nil

	// 初始化条件变量
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.Heartbeat = time.Now()
	rf.ElectionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 关键：如果从快照恢复（lastKzIndex > 0），需要更新LastApplied和CommitIndex
	// 否则applier会尝试应用已经在快照中的日志
	if rf.lastKzIndex > 0 {
		rf.LastApplied = rf.lastKzIndex
		rf.CommitIndex = rf.lastKzIndex
	}

	// 如果有持久化的快照数据，发送到服务层恢复状态
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 && rf.lastKzIndex > 0 {
		go func() {
			rf.ApplyCh <- raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot,
				SnapshotTerm:  rf.lastKzTerm,
				SnapshotIndex: rf.lastKzIndex,
			}
		}()
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	DPrintf("create raft %d\n", me)
	return rf
}
