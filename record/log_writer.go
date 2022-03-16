// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"context"
	"encoding/binary"
	"io"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/crc"
)

var walSyncLabels = pprof.Labels("pebble", "wal-sync")

//
type block struct {

	// buf[:written] has already been filled with fragments. Updated atomically.
	// 在 written 之前的数据已经拷贝到缓存中了，准备被 flush 。
	written int32

	// buf[:flushed] has already been flushed to w.
	// 在 flushed 之前的数据已经被 flush 了。
	flushed int32

	buf [blockSize]byte
}

type flusher interface {
	Flush() error
}

type syncer interface {
	Sync() error
}

const (
	syncConcurrencyBits = 9

	// SyncConcurrency is the maximum number of concurrent sync operations that
	// can be performed. Note that a sync operation is initiated either by a call
	// to SyncRecord or by a call to Close. Exported as this value also limits
	// the commit concurrency in commitPipeline.
	SyncConcurrency = 1 << syncConcurrencyBits
)

type syncSlot struct {
	wg  *sync.WaitGroup
	err *error
}

// syncQueue is a lock-free fixed-size single-producer, single-consumer
// queue. The single-producer can push to the head, and the single-consumer can
// pop multiple values from the tail. Popping calls Done() on each of the
// available *sync.WaitGroup elements.
//
// syncQueue 是一个无锁的固定大小的单生产者、单消费者队列。
// 单生产者可以向头部推送，而单消费者可以从尾部弹出多个值。。
type syncQueue struct {

	// headTail packs together a 32-bit head index and a 32-bit tail index.
	// Both are indexes into slots modulo len(slots)-1.
	//
	// tail = index of oldest data in queue
	// head = index of next slot to fill
	//
	// Slots in the range [tail, head) are owned by consumers.  A consumer
	// continues to own a slot outside this range until it nils the slot, at
	// which point ownership passes to the producer.
	//
	// The head index is stored in the most-significant bits so that we can
	// atomically add to it and the overflow is harmless.
	//
	//
	headTail uint64

	// slots is a ring buffer of values stored in this queue.
	// The size must be a power of 2.
	// A slot is in use until the tail index has moved beyond it.
	slots [SyncConcurrency]syncSlot

	// blocked is an atomic boolean which indicates whether syncing is currently
	// blocked or can proceed. It is used by the implementation of
	// min-sync-interval to block syncing until the min interval has passed.
	blocked uint32
}

const dequeueBits = 32

func (q *syncQueue) unpack(ptrs uint64) (head, tail uint32) {
	const mask = 1<<dequeueBits - 1
	head = uint32((ptrs >> dequeueBits) & mask)
	tail = uint32(ptrs & mask)
	return
}

func (q *syncQueue) push(wg *sync.WaitGroup, err *error) {
	// 头尾指针
	ptrs := atomic.LoadUint64(&q.headTail)
	head, tail := q.unpack(ptrs)

	// 已满？
	if (tail+uint32(len(q.slots)))&(1<<dequeueBits-1) == head {
		panic("pebble: queue is full")
	}

	// 根据 head 定位到 slot ，将数据保存到该 slot
	slot := &q.slots[head&uint32(len(q.slots)-1)]
	slot.wg = wg
	slot.err = err

	// Increment head.
	// This passes ownership of slot to dequeue and acts as a store barrier for writing the slot.
	//
	// 增加头指针
	atomic.AddUint64(&q.headTail, 1<<dequeueBits)
}

func (q *syncQueue) setBlocked() {
	atomic.StoreUint32(&q.blocked, 1)
}

func (q *syncQueue) clearBlocked() {
	atomic.StoreUint32(&q.blocked, 0)
}

func (q *syncQueue) empty() bool {
	head, tail := q.load()
	return head == tail
}

func (q *syncQueue) load() (head, tail uint32) {
	if atomic.LoadUint32(&q.blocked) == 1 {
		return 0, 0
	}

	ptrs := atomic.LoadUint64(&q.headTail)
	head, tail = q.unpack(ptrs)
	return head, tail
}

func (q *syncQueue) pop(head, tail uint32, err error) error {
	if tail == head {
		// Queue is empty.
		return nil
	}

	for ; tail != head; tail++ {
		slot := &q.slots[tail&uint32(len(q.slots)-1)]
		wg := slot.wg
		if wg == nil {
			return errors.Errorf("nil waiter at %d", errors.Safe(tail&uint32(len(q.slots)-1)))
		}
		*slot.err = err
		slot.wg = nil
		slot.err = nil
		// We need to bump the tail count before signalling the wait group as
		// signalling the wait group can trigger release a blocked goroutine which
		// will try to enqueue before we've "freed" space in the queue.
		atomic.AddUint64(&q.headTail, 1)
		wg.Done()
	}

	return nil
}

// flusherCond is a specialized condition variable that allows its condition to
// change and readiness be signalled without holding its associated mutex. In
// particular, when a waiter is added to syncQueue atomically, this condition
// variable can be signalled without holding flusher.Mutex.
//
// flusherCond是一个专门的条件变量，它允许其条件变化和准备就绪的信号，而不需要持有其相关的mutex。
// 特别是，当一个等待者被原子化地添加到 syncQueue 时，这个条件变量可以在不持有 flusher.Mutex 的情况下被发出信号。
type flusherCond struct {
	mu   *sync.Mutex	// 互斥锁
	q    *syncQueue		// 无锁队列
	cond sync.Cond		// 条件变量
}

func (c *flusherCond) init(mu *sync.Mutex, q *syncQueue) {
	c.mu = mu
	c.q = q

	// Yes, this is a bit circular, but that is intentional. flusherCond.cond.L
	// points flusherCond so that when cond.L.Unlock is called flusherCond.Unlock
	// will be called and we can check the !syncQueue.empty() condition.
	//
	// 是的，这里出现了循环引用，但这是故意的。
	// 当 c.cond.L.Unlock 被调用时，c.Unlock 将被调用，我们可以检查 !syncQueue.empty() 条件。
	c.cond.L = c
}

func (c *flusherCond) Signal() {
	// Pass-through to the cond var.
	//
	// 触发信号
	c.cond.Signal()
}

func (c *flusherCond) Wait() {
	// Pass-through to the cond var. Note that internally the cond var implements
	// Wait as:
	//
	//   t := notifyListAdd()
	//   L.Unlock()
	//   notifyListWait(t)
	//   L.Lock()
	//
	// We've configured the cond var to call flusherReady.Unlock() which allows
	// us to check the !syncQueue.empty() condition without a danger of missing a
	// notification. Any call to flusherReady.Signal() after notifyListAdd() is
	// called will cause the subsequent notifyListWait() to return immediately.
	//
	// 等待信号
	c.cond.Wait()
}

func (c *flusherCond) Lock() {
	c.mu.Lock()
}

func (c *flusherCond) Unlock() {
	c.mu.Unlock()
	if !c.q.empty() {
		// If the current goroutine is about to block on sync.Cond.Wait, this call
		// to Signal will prevent that. The comment in Wait above explains a bit
		// about what is going on here, but it is worth reiterating:
		//
		//   flusherCond.Wait()
		//     sync.Cond.Wait()
		//       t := notifyListAdd()
		//       flusherCond.Unlock()    <-- we are here
		//       notifyListWait(t)
		//       flusherCond.Lock()
		//
		// The call to Signal here results in:
		//
		//     sync.Cond.Signal()
		//       notifyListNotifyOne()
		//
		// The call to notifyListNotifyOne() will prevent the call to
		// notifyListWait(t) from blocking.
		c.cond.Signal()
	}
}

type durationFunc func() time.Duration

// syncTimer is an interface for timers, modeled on the closure callback mode
// of time.Timer. See time.AfterFunc and LogWriter.afterFunc. syncTimer is used
// by tests to mock out the timer functionality used to implement
// min-sync-interval.
type syncTimer interface {
	Reset(time.Duration) bool
	Stop() bool
}

// WAL 会用最大不超过 32 KiB 的多个 Block 进行保存，目前 Pebble 默认使用的是 recyclable 格式的 Block，
// 即 WAL 文件在不需要时可以被回收复用，以减少 sync 文件元信息。
//
// +----------+-----------+-----------+----------------+--- ... ---+
// | CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
// +----------+-----------+-----------+----------------+--- ... ---+
//
// CRC 和 Size 之后的 Type 用来标识当前 Block 是多个 Block 中的 Full / First / Middle / Last，
// 而 Log Number 则是帮助复用 WAL 的递增 Log Number。
//
//
// 根据要写入的格式我们知道 SyncRecord 和 LogWriter 需要做的本质上即将 Batch 的数据根据切分
// 为不超过 BlockSize(32 KiB) 的多个 Block，每个 Block 附上 CRC / Size / Type 以及当前
// 的 Log Number 并写入日志文件即可，而 emitFragment 方法主要也处理切 block 的逻辑，
// 不过 LogWriter 为了实现写入的高效做了一些事情，我们来细看下~
//
// 首先 LogWriter 为了避免 Go 分配内存开销，对写入的 Block 在 LogWriter 初始化时预分配一块作为当前 Block，
// 之后调用 emitFragment 只是在预留的 Block 处理为 WAL 的格式, 在 Block 写满或写入完成时将当前 Block 放
// 到 flusher.pending中异步 Flush 并获取新的 Block 来继续写入, 而当 Block 被 Flush 后会放回 LogWriter.free
// 的 blocks 数组中，每当需要新 Block 时会首先尝试从 free.blocks 中复用，如果没有且当前已分配的少于 16 (硬编码)
// 会先直接 new， 如果已经分配过 16 个则会等待 sync.Cond。
//
//
//

//
//
//
// LogWriter writes records to an underlying io.Writer. In order to support WAL
// file reuse, a LogWriter's records are tagged with the WAL's file
// number. When reading a log file a record from a previous incarnation of the
// file will return the error ErrInvalidLogNum.
type LogWriter struct {

	// w is the underlying writer.
	w io.Writer

	// c is w as a closer.
	c io.Closer

	// s is w as a syncer.
	s syncer

	// logNum is the low 32-bits of the log's file number.
	logNum uint32

	// blockNum is the zero based block number for the current block.
	// 当前块号，从 0 开始计数。
	blockNum int64

	// err is any accumulated error. TODO(peter): This needs to be protected in
	// some fashion. Perhaps using atomic.Value.
	err error

	// block is the current block being written. Protected by flusher.Mutex.
	//
	// 当前块
	block *block

	free struct {
		sync.Mutex

		// Condition variable used to signal a block is freed.
		cond      sync.Cond
		blocks    []*block
		allocated int
	}

	//
	flusher struct {
		sync.Mutex

		// Flusher ready is a condition variable that is signalled when there are
		// blocks to flush, syncing has been requested, or the LogWriter has been
		// closed. For signalling of a sync, it is safe to call without holding
		// flusher.Mutex.
		//
		// Flusher ready 是一个条件变量，当
		//	1. 有 block 需要被 flush
		//  2. 请求 sync
		//  3. LogWriter 被关闭
		// 就会触发通知信号，此变量可以在不加锁的情况下，安全的触发通知。
		ready flusherCond

		// Set to true when the flush loop should be closed.
		close bool

		// Closed when the flush loop has terminated.
		closed chan struct{}

		// Accumulated flush error.
		err error

		// minSyncInterval is the minimum duration between syncs.
		minSyncInterval durationFunc

		//
		pending []*block

		// 无锁队列
		syncQ syncQueue
	}

	// afterFunc is a hook to allow tests to mock out the timer functionality
	// used for min-sync-interval. In normal operation this points to
	// time.AfterFunc.
	afterFunc func(d time.Duration, f func()) syncTimer
}

// NewLogWriter returns a new LogWriter.
func NewLogWriter(w io.Writer, logNum base.FileNum) *LogWriter {
	c, _ := w.(io.Closer)
	s, _ := w.(syncer)

	r := &LogWriter{
		w: w,
		c: c,
		s: s,
		// NB: we truncate the 64-bit log number to 32-bits. This is ok because a)
		// we are very unlikely to reach a file number of 4 billion and b) the log
		// number is used as a validation check and using only the low 32-bits is
		// sufficient for that purpose.
		logNum: uint32(logNum),
		afterFunc: func(d time.Duration, f func()) syncTimer {
			return time.AfterFunc(d, f)
		},
	}

	r.free.cond.L = &r.free.Mutex
	r.free.blocks = make([]*block, 0, 16)
	r.free.allocated = 1
	r.block = &block{}

	r.flusher.ready.init(&r.flusher.Mutex, &r.flusher.syncQ)
	r.flusher.closed = make(chan struct{})
	r.flusher.pending = make([]*block, 0, cap(r.free.blocks))

	go func() {
		pprof.Do(context.Background(), walSyncLabels, r.flushLoop)
	}()

	return r
}

// SetMinSyncInterval sets the closure to invoke for retrieving the minimum
// sync duration between syncs.
func (w *LogWriter) SetMinSyncInterval(minSyncInterval durationFunc) {
	f := &w.flusher
	f.Lock()
	f.minSyncInterval = minSyncInterval
	f.Unlock()
}

// flushLoop 是一个单独的 Goroutine，它启动后将 Loop 等待 flusher.ready 的通知，
// 被唤醒后检查满足一下三个条件之一时开始工作：
//	- 有在 flusher.pending 中待 flush 的 block
//	- 有当前 flusher.block 有未 flush 的数据
//	- 距离上次 sync 满足了 minSyncInterval，且有 sync 请求在 flusher.syncQ 中
//
// flusher.ready 是一个包装了 sync.Cond 的 flusherCond,
// 该类型主要对 mutex 进行包装, 实现在 cond 进行 wait 的 Unlock mutex 后额外检查 syncQ 是否不为空,
// 如果不为空即便外面没有 signal 也主动 signal 让 wait 立刻唤醒, 即可以通过 syncQ change 来唤醒不
// 仅仅是等 signal，避免 miss.
//
//
// flushLoop 开始工作后主要从事两件事情：
//	- 首先是 Flush 即将当前 block 和当前 flusher.pending 中的 block 写入Write()到日志文件中(对应于操作系统的写入 PageCache),
//	- 此外是 Sync 即根据 syncQ 中是否有 sync 请求来决定是否对当前文件进行 Sync() 操作(刷到磁盘),
//		在 Sync 成功后会通过 WaitGroup Done 来通知等待 Sync 的逻辑继续(Publish)
//
// flushLoop 每次运行都可以保证将当前可看到的当前 block 数据和 Pending block 数据 Flush 到操作系统，
// 保证看的到的 Sync 请求都被处理，并且处理完后通过 WaitGroup Done 来通知发起者。
//
// Pebble 和 RocksDB 一样同样支持 minSyncInterval 可以在即便有 sync 请求也等一个间隔再 sync，
// 不过 CRDB 该功能配置 0 避免丢数据更重要, 实现上是通过对 syncQueue 设置 blocked 标志位时从 syncQueue 中获取不到新请求来实现。
//
//
//
func (w *LogWriter) flushLoop(context.Context) {

	f := &w.flusher

	// 加锁
	f.Lock()

	// 定时器
	var syncTimer syncTimer
	defer func() {
		// 停止定时器
		if syncTimer != nil {
			syncTimer.Stop()
		}
		// 退出信号
		close(f.closed)
		// 解锁
		f.Unlock()
	}()

	// The flush loop performs flushing of full and partial data blocks to the
	// underlying writer (LogWriter.w), syncing of the writer, and notification
	// to sync requests that they have completed.
	//
	// `flushLoop` 将 Full/Partial blocks 刷到底层存储上、执行 sync、通知上游完成 sync 。
	//
	// - flusher.ready is a condition variable that is signalled when there is
	//   work to do. Full blocks are contained in flusher.pending. The current
	//   partial block is in LogWriter.block. And sync operations are held in
	//   flusher.syncQ.
	//
	//   `flusher.ready` 是用于通知新任务到达的条件变量。
	//
	// - The decision to sync is determined by whether there are any sync
	//   requests present in flusher.syncQ and whether enough time has elapsed
	//   since the last sync. If not enough time has elapsed since the last sync,
	//   flusher.syncQ.blocked will be set to 1. If syncing is blocked,
	//   syncQueue.empty() will return true and syncQueue.load() will return 0,0
	//   (i.e. an empty list).
	//
	//   同步决定是由 flusher.syncQ 中是否有任何同步请求以及距离上一次同步是否有足够的时间来决定的。
	//   如果距离上次同步没有足够的时间，flusher.syncQ.blocked 将被设置为 1 。
	//   如果同步被阻止，syncQueue.empty() 将返回true，syncQueue.load() 将返回 0,0 (即一个空列表)。
	//
	// - flusher.syncQ.blocked is cleared by a timer that is initialized when
	//   blocked is set to 1. When blocked is 1, no syncing will take place, but
	//   flushing will continue to be performed. The on/off toggle for syncing
	//   does not need to be carefully synchronized with the rest of processing
	//   -- all we need to ensure is that after any transition to blocked=1 there
	//   is eventually a transition to blocked=0. syncTimer performs this
	//   transition. Note that any change to min-sync-interval will not take
	//   effect until the previous timer elapses.
	//
	//   flushher.syncQ.blocked 是由一个计时器清除的，当 blocked 被设置为 1 时，该计时器被初始化。
	//   当 blocked 为 1 时，不会发生 syncing 同步，但是 flushing 会继续执行。
	//
	//   syncing 的 on/off 开关切换不需要与其他处理过程仔细同步，我们只需要确保在任何 blocked=1 的过渡之后，
	//   最终有一个过渡到 blocked=0 。
	//
	// - Picking up the syncing work to perform requires coordination with
	//   picking up the flushing work. Specifically, flushing work is queued
	//   before syncing work. The guarantee of this code is that when a sync is
	//   requested, any previously queued flush work will be synced. This
	//   motivates reading the syncing work (f.syncQ.load()) before picking up
	//   the flush work (atomic.LoadInt32(&w.block.written)).
	//
	//   挑选同步工作的执行需要与挑选冲洗工作相协调。具体来说，冲洗工作要排在同步工作之前。
	//   这段代码的保证是，当请求同步时，任何先前排队的冲洗工作都会被同步。
	//   这就促使我们在读取同步工作（f.syncQ.load()）之前，要先读取  读取同步工作(f.syncQ.load())，
	//   然后再读取冲洗工作(atomic.LoadInt32(&w.block.writed))。


	// The list of full blocks that need to be written. This is copied from
	// f.pending on every loop iteration, though the number of elements is small
	// (usually 1, max 16).
	//
	// 需要执行写入的完整 blocks 列表。
	// 这个列表在每次循环迭代时从 f.pending 中拷贝出来，即使元素的数量很少 (通常是1，最多16)。
	pending := make([]*block, 0, cap(f.pending))

	for {
		for {
			// Grab the portion of the current block that requires flushing. Note that
			// the current block can be added to the pending blocks list after we release
			// the flusher lock, but it won't be part of pending.
			//
			// 抓取当前 block 中需要被 flush 的部分。
			//
			// 注意，在我们释放了 flusher 锁后，当前块可以被添加到待处理块列表中。
			// 之后，当前块可以被添加到待处理块列表中，但它不会成为待处理块的一部分。

			// 当前块的写入偏移
			written := atomic.LoadInt32(&w.block.written)

			// 条件判断:
			// 	1. 如果 f.pending 非空，意味着有若干 block 需要 flush ；
			// 	2. 如果 w.block.written > w.block.flushed ，意味着当前 block 中有新增数据；
			// 	3. 如果 f.syncQ 非空，意味着有 waiter 等待 flush 通知；
			if len(f.pending) > 0 || written > w.block.flushed || !f.syncQ.empty() {
				break
			}

			// 如果 writer 被关闭，假装 sync timer 立即启动，这样我们就可以处理任何排队的同步请求。
			if f.close {

				// If the writer is closed, pretend the sync timer fired immediately so
				// that we can process any queued sync requests.
				//
				//
				f.syncQ.clearBlocked()

				// 如果队列非空，退出内层循环，走到下面 block 处理逻辑。
				if !f.syncQ.empty() {
					break
				}

				// 如果队列为空，直接退出
				return
			}

			// 等待新的刷盘请求到达
			f.ready.Wait()
			continue
		}


		// 把 f.pending 拷贝到本地 pending 中，然后清空 f.pending 。
		pending = pending[:len(f.pending)]
		copy(pending, f.pending)
		f.pending = f.pending[:0]


		// Grab the list of sync waiters.
		//
		// Note that syncQueue.load() will return 0,0 while we're waiting for
		// the min-sync-interval to expire. This allows flushing to proceed
		// even if we're not ready to sync.
		//
		// 获取 sync waiters 列表。
		// 注意，当我们在等待 min-sync-interval 到期时，f.syncQ.load() 将返回 0,0 。
		// 这样，即使我们还没有准备好同步，也可以进行 sync 。
		head, tail := f.syncQ.load()


		// Grab the portion of the current block that requires flushing. Note that
		// the current block can be added to the pending blocks list after we
		// release the flusher lock, but it won't be part of pending. This has to
		// be ordered after we get the list of sync waiters from syncQ in order to
		// prevent a race where a waiter adds itself to syncQ, but this thread
		// picks up the entry in syncQ and not the buffered data.
		//
		// 获取 current block 中需要 flush 的部分。
		//
		// 注意，在我们释放了 flusher 的锁之后，当前块可以被添加到待处理块列表中，但它不会成为待处理的一部分。
		// 这必须在我们从 syncQ 获得同步等待者列表后进行，以防止发生竞争，即一个等待者将自己添加到 syncQ 中，
		// 但这个线程获取的是 syncQ 中的条目而不是缓冲数据。
		//
		written := atomic.LoadInt32(&w.block.written)
		data := w.block.buf[w.block.flushed:written]
		w.block.flushed = written

		// If flusher has an error, we propagate it to waiters.
		// Note in spite of error we consume the pending list above to free blocks for writers.
		//
		// 如果 flusher 报错，我们将其传播给 waiters 。
		// 请注意，尽管有错误，我们还是会消耗上面的待处理列表，为 writers 释放块。
		if f.err != nil {
			f.syncQ.pop(head, tail, f.err)
			continue
		}

		// 解锁
		f.Unlock()

		//
		synced, err := w.flushPending(data, pending, head, tail)

		f.Lock()
		f.err = err
		if f.err != nil {
			f.syncQ.clearBlocked()
			continue
		}

		//
		if synced && f.minSyncInterval != nil {
			// A sync was performed.
			// Make sure we've waited for the min sync interval before syncing again.
			if min := f.minSyncInterval(); min > 0 {
				//
				f.syncQ.setBlocked()
				// 为空则初始化
				if syncTimer == nil {
					// 在 min 之后触发 func()
					syncTimer = w.afterFunc(min, func() {
						f.syncQ.clearBlocked()
						f.ready.Signal()
					})
					// 非空则 Reset
				} else {
					syncTimer.Reset(min)
				}
			}
		}
	}
}

func (w *LogWriter) flushPending(
	data []byte, pending []*block, head, tail uint32,
) (synced bool, err error) {

	defer func() {
		// Translate panics into errors. The errors will cause flushLoop to shut
		// down, but allows us to do so in a controlled way and avoid swallowing
		// the stack that created the panic if panic'ing itself hits a panic
		// (e.g. unlock of unlocked mutex).
		if r := recover(); r != nil {
			err = errors.Newf("%v", r)
		}
	}()


	// 遍历 pending blocks ，逐个 flush 。
	for _, b := range pending {
		if err = w.flushBlock(b); err != nil {
			break
		}
	}

	// 把当前 block 中的 data flush 到存储
	if err == nil && len(data) > 0 {
		_, err = w.w.Write(data)
	}

	synced = head != tail
	if synced {
		if err == nil && w.s != nil {
			err = w.s.Sync()
		}
		f := &w.flusher
		if popErr := f.syncQ.pop(head, tail, err); popErr != nil {
			return synced, popErr
		}
	}

	return synced, err
}

func (w *LogWriter) flushBlock(b *block) error {
	// 将 block 写入到 w
	if _, err := w.w.Write(b.buf[b.flushed:]); err != nil {
		return err
	}
	// 重置
	b.written = 0
	b.flushed = 0
	// 将 block 归还到 free 列表中
	w.free.Lock()
	w.free.blocks = append(w.free.blocks, b)
	w.free.cond.Signal()	// 通知 free 有资源可用
	w.free.Unlock()
	return nil
}

// queueBlock queues the current block for writing to the underlying writer,
// allocates a new block and reserves space for the next header.
//
//
func (w *LogWriter) queueBlock() {

	// Allocate a new block, blocking until one is available.
	// We do this first because w.block is protected by w.flusher.Mutex.
	//
	// 获取空闲块
	w.free.Lock()
	if len(w.free.blocks) == 0 {
		if w.free.allocated < cap(w.free.blocks) {
			w.free.allocated++
			w.free.blocks = append(w.free.blocks, &block{})
		} else {
			for len(w.free.blocks) == 0 {
				w.free.cond.Wait()
			}
		}
	}
	nextBlock := w.free.blocks[len(w.free.blocks)-1]
	w.free.blocks = w.free.blocks[:len(w.free.blocks)-1]
	w.free.Unlock()


	f := &w.flusher
	f.Lock()
	// 保存当前 block 到 pending 列表中
	f.pending = append(f.pending, w.block)
	// 获取空闲 block 作为当前 block
	w.block = nextBlock

	// 这里通过 flusher.ready 通知 flushLoop 开始 flush 工作
	f.ready.Signal()
	w.err = w.flusher.err
	f.Unlock()

	// 增加块数目
	w.blockNum++
}

// Close flushes and syncs any unwritten data and closes the writer.
// Where required, external synchronisation is provided by commitPipeline.mu.
func (w *LogWriter) Close() error {
	f := &w.flusher

	// Emit an EOF trailer signifying the end of this log. This helps readers
	// differentiate between a corrupted entry in the middle of a log from
	// garbage at the tail from a recycled log file.
	w.emitEOFTrailer()

	// Signal the flush loop to close.
	f.Lock()
	f.close = true
	f.ready.Signal()
	f.Unlock()

	// Wait for the flush loop to close. The flush loop will not close until all
	// pending data has been written or an error occurs.
	<-f.closed

	// Sync any flushed data to disk. NB: flushLoop will sync after flushing the
	// last buffered data only if it was requested via syncQ, so we need to sync
	// here to ensure that all the data is synced.
	err := w.flusher.err
	if err == nil && w.s != nil {
		err = w.s.Sync()
	}

	if w.c != nil {
		cerr := w.c.Close()
		w.c = nil
		if cerr != nil {
			return cerr
		}
	}
	w.err = errors.New("pebble/record: closed LogWriter")
	return err
}

// WriteRecord writes a complete record.
// Returns the offset just past the end of the record.
// External synchronisation provided by commitPipeline.mu.
//
func (w *LogWriter) WriteRecord(p []byte) (int64, error) {
	return w.SyncRecord(p, nil, nil)
}

// SyncRecord writes a complete record. If wg!= nil the record will be
// asynchronously persisted to the underlying writer and done will be called on
// the wait group upon completion. Returns the offset just past the end of the
// record.
// External synchronisation provided by commitPipeline.mu.
//
//
// SyncRecord 方法还是比较好理解，for 循环中便会将数据按照上面讲的方式切分片段，并将片段写入 block 中。
//
// 如果日志为同步落盘方式，还会通知 flusher 去刷盘，同时会将 wg 放到 sync 队列，flusher 会通过 wg 异步通知 Pipeline 刷盘完成。
//
// SyncRecord 需要做的本质上即将 Batch 的数据根据切分为不超过 BlockSize(32 KiB) 的多个 Block，
// 每个 Block 附上 CRC / Size / Type 以及当前的 Log Number 并写入日志文件即可。
//
//
// 通过 emitFragment 方法将 Batch 数据写入当前 Block 或分多 Block 推入 Pending，
// 每新产生新 Block 时让 flushLoop 异步开始进行 Flush Write 到操作系统 PageCache，
// 在整个记录都写入完成后追加 WaitGroup 到 syncQ 触发 flushLoop 对前面写入的数据异步 Sync 到磁盘，
// SyncRecord 不用等待 Sync 完成既可以返回，后面需要“确保 Sync 完成”的逻辑可以通过 WaitGroup Wait 来保证。
func (w *LogWriter) SyncRecord(p []byte, wg *sync.WaitGroup, err *error) (int64, error) {

	if w.err != nil {
		return -1, w.err
	}

	// The `i == 0` condition ensures we handle empty records. Such records can
	// possibly be generated for VersionEdits stored in the MANIFEST. While the
	// MANIFEST is currently written using Writer, it is good to support the same
	// semantics with LogWriter.
	//
	// 切分数据放到片段中
	for i := 0; i == 0 || len(p) > 0; i++ {
		p = w.emitFragment(i, p)
	}

	// wg 不为空，则表示 wal 是同步落盘，因此需要通知 flusher 去刷盘
	if wg != nil {
		// If we've been asked to persist the record, add the WaitGroup to the sync
		// queue and signal the flushLoop. Note that flushLoop will write partial
		// blocks to the file if syncing has been requested. The contract is that
		// any record written to the LogWriter to this point will be flushed to the
		// OS and synced to disk.
		//
		// 若要持久化，需把 wg 添加到同步队列 syncQ ，并向 flushLoop 发出信号。
		f := &w.flusher
		// 将在整个 Batch 写入完成后将 WaitGroup 添加到 flusher.syncQ
		f.syncQ.push(wg, err)
		// 通过 flusher.ready 通知 flushLoop 进行 sync 工作
		f.ready.Signal()
	}

	// 根据当前块号和块内偏移，得到 offset
	offset := w.blockNum*blockSize + int64(w.block.written)

	// Note that we don't return w.err here as a concurrent call to Close would
	// race with our read. That's ok because the only error we could be seeing is
	// one to syncing for which the caller can receive notification of by passing
	// in a non-nil err argument.
	//
	// 请注意，我们在这里没有返回 w.err ，因为同时调用 Close 会导致与我们的读取相竞争。
	// 这没关系，因为我们可能看到的唯一错误是同步的错误，调用者可以通过传入一个非零的err参数来接收通知。
	return offset, nil
}

// Size returns the current size of the file.
// External synchronisation provided by commitPipeline.mu.
func (w *LogWriter) Size() int64 {
	return w.blockNum*blockSize + int64(w.block.written)
}

func (w *LogWriter) emitEOFTrailer() {
	// Write a recyclable chunk header with a different log number.  Readers
	// will treat the header as EOF when the log number does not match.
	b := w.block
	i := b.written
	binary.LittleEndian.PutUint32(b.buf[i+0:i+4], 0) // CRC
	binary.LittleEndian.PutUint16(b.buf[i+4:i+6], 0) // Size
	b.buf[i+6] = recyclableFullChunkType
	binary.LittleEndian.PutUint32(b.buf[i+7:i+11], w.logNum+1) // Log number
	atomic.StoreInt32(&b.written, i+int32(recyclableHeaderSize))
}

// 将 Batch 数据写入当前 Block 或分多 Block 推入到 w.flusher.pending 中，并通知后台 flush 。
func (w *LogWriter) emitFragment(n int, p []byte) []byte {
	// 当前 block
	b := w.block
	// 当前 block 的有效字节数
	i := b.written

	// 首个 fragment
	first := n == 0
	// 最后 fragment
	last := blockSize-i-recyclableHeaderSize >= int32(len(p))


	///// 向 b.buf[] 中写入数据

	// 写入 Type
	if last {
		if first {
			b.buf[i+6] = recyclableFullChunkType
		} else {
			b.buf[i+6] = recyclableLastChunkType
		}
	} else {
		if first {
			b.buf[i+6] = recyclableFirstChunkType
		} else {
			b.buf[i+6] = recyclableMiddleChunkType
		}
	}

	// 写入 LogNum
	binary.LittleEndian.PutUint32(b.buf[i+7:i+11], w.logNum)

	// 从 p 中拷贝数据到 b.buf[] 中，返回字节数 r
	r := copy(b.buf[i+recyclableHeaderSize:], p)
	j := i + int32(recyclableHeaderSize+r)

	// 写入 Crc
	binary.LittleEndian.PutUint32(b.buf[i+0:i+4], crc.New(b.buf[i+6:j]).Value())
	// 写入 Size
	binary.LittleEndian.PutUint16(b.buf[i+4:i+6], uint16(r))

	// 更新 b.written
	atomic.StoreInt32(&b.written, j)

	// 如果当前 block 中剩余的空间不足以容纳新的 Fragment 了，就新起一个 block 。
	if blockSize-b.written < recyclableHeaderSize {
		// There is no room for another fragment in the block, so fill the
		// remaining bytes with zeros and queue the block for flushing.
		//
		// 块中没有空间容纳另一个片段，所以将剩余的字节为零，并将该块排队刷新。
		for i := b.written; i < blockSize; i++ {
			b.buf[i] = 0
		}

		// 在写完一个 Block 后会调用 queueBlock 切换当前 Block 并将原来的 Block 加入 flusher.pending 队列中
		w.queueBlock()
	}

	// 返回尚未写入到 b.buf 中的剩余字节
	return p[r:]
}
