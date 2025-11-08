package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	key      string // 锁的键名
	clientId string // 客户端唯一标识
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		key:      l,
		clientId: kvtest.RandValue(8), // 生成唯一客户端ID
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		// 尝试获取锁：如果锁不存在（version=0），则创建并设置为自己的clientId
		err := lk.ck.Put(lk.key, lk.clientId, 0)
		if err == rpc.OK {
			// 成功获取锁
			return
		}

		// 锁已存在，检查当前状态
		value, version, getErr := lk.ck.Get(lk.key)
		if getErr != rpc.OK {
			// 网络错误，重试
			time.Sleep(1 * time.Millisecond)
			continue
		}

		if value == lk.clientId {
			// 锁已经是自己的
			return
		}

		if value == "RELEASED" {
			// 锁已释放，尝试获取
			putErr := lk.ck.Put(lk.key, lk.clientId, version)
			if putErr == rpc.OK {
				// 成功获取锁
				return
			}
			// 版本冲突，继续重试
		}

		// 锁被其他客户端持有，等待一段时间后重试
		time.Sleep(1 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		// 获取当前锁状态
		value, version, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			// 锁不存在，已经释放了
			return
		}
		if err != rpc.OK {
			// 网络错误，重试
			continue
		}

		if value != lk.clientId {
			// 锁不是自己的，不能释放
			return
		}

		// 尝试释放锁：通过Put一个特殊值表示锁可用
		// 使用"RELEASED"表示锁已释放，其他客户端看到这个值就知道可以获取锁
		putErr := lk.ck.Put(lk.key, "RELEASED", version)
		if putErr == rpc.OK {
			// 成功释放锁
			return
		}

		// 版本冲突或其他错误，重试
	}
}
