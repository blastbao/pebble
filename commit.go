// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/pebble/record"
)

// commitQueue is a lock-free fixed-size single-producer, multi-consumer
// queue. The single producer can enqueue (push) to the head, and consumers can
// dequeue (pop) from the tail.
//
// It has the added feature that it nils out unused slots to avoid unnecessary
// retention of objects.
type commitQueue struct {
	// headTail packs together a 32-bit head index and a 32-bit tail index. Both
	// are indexes into slots modulo len(slots)-1.
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
	headTail uint64

	// slots is a ring buffer of values stored in this queue. The size must be a
	// power of 2. A slot is in use until *both* the tail index has moved beyond
	// it and the slot value has been set to nil. The slot value is set to nil
	// atomically by the consumer and read atomically by the producer.
	slots [record.SyncConcurrency]unsafe.Pointer
}

const dequeueBits = 32

func (q *commitQueue) unpack(ptrs uint64) (head, tail uint32) {
	const mask = 1<<dequeueBits - 1
	head = uint32((ptrs >> dequeueBits) & mask)
	tail = uint32(ptrs & mask)
	return
}

func (q *commitQueue) pack(head, tail uint32) uint64 {
	const mask = 1<<dequeueBits - 1
	return (uint64(head) << dequeueBits) |
		uint64(tail&mask)
}

func (q *commitQueue) enqueue(b *Batch) {
	ptrs := atomic.LoadUint64(&q.headTail)
	head, tail := q.unpack(ptrs)
	if (tail+uint32(len(q.slots)))&(1<<dequeueBits-1) == head {
		// Queue is full. This should never be reached because commitPipeline.sem
		// limits the number of concurrent operations.
		panic("pebble: not reached")
	}
	slot := &q.slots[head&uint32(len(q.slots)-1)]

	// Check if the head slot has been released by dequeue.
	for atomic.LoadPointer(slot) != nil {
		// Another goroutine is still cleaning up the tail, so the queue is
		// actually still full. We spin because this should resolve itself
		// momentarily.
		runtime.Gosched()
	}

	// The head slot is free, so we own it.
	atomic.StorePointer(slot, unsafe.Pointer(b))

	// Increment head. This passes ownership of slot to dequeue and acts as a
	// store barrier for writing the slot.
	atomic.AddUint64(&q.headTail, 1<<dequeueBits)
}

func (q *commitQueue) dequeue() *Batch {
	for {
		ptrs := atomic.LoadUint64(&q.headTail)
		head, tail := q.unpack(ptrs)
		if tail == head {
			// Queue is empty.
			return nil
		}

		slot := &q.slots[tail&uint32(len(q.slots)-1)]
		b := (*Batch)(atomic.LoadPointer(slot))
		if b == nil || atomic.LoadUint32(&b.applied) == 0 {
			// The batch is not ready to be dequeued, or another goroutine has
			// already dequeued it.
			return nil
		}

		// Confirm head and tail (for our speculative check above) and increment
		// tail. If this succeeds, then we own the slot at tail.
		ptrs2 := q.pack(head, tail+1)
		if atomic.CompareAndSwapUint64(&q.headTail, ptrs, ptrs2) {
			// We now own slot.
			//
			// Tell enqueue that we're done with this slot. Zeroing the slot is also
			// important so we don't leave behind references that could keep this object
			// live longer than necessary.
			atomic.StorePointer(slot, nil)
			// At this point enqueue owns the slot.
			return b
		}
	}
}

// commitEnv contains the environment that a commitPipeline interacts with.
// This allows fine-grained testing of commitPipeline behavior without
// construction of an entire DB.
//
type commitEnv struct {
	// The next sequence number to give to a batch. Protected by commitPipeline.mu.
	//
	// 日志序号
	logSeqNum *uint64

	// The visible sequence number at which reads should be performed. Ratcheted
	// upwards atomically as batches are applied to the memtable.
	//
	// 可见日志序号
	visibleSeqNum *uint64

	// Apply the batch to the specified memtable. Called concurrently.
	//
	// 将 batch 应用到 MemTable ，并发调用。
	apply func(b *Batch, mem *memTable) error

	// Write the batch to the WAL. If wg != nil, the data will be persisted
	// asynchronously and done will be called on wg upon completion. If wg != nil
	// and err != nil, a failure to persist the WAL will populate *err. Returns
	// the memtable the batch should be applied to. Serial execution enforced by
	// commitPipeline.mu.
	//
	// 将 batch 写入 WAL 并返回 MemTable
	write func(b *Batch, wg *sync.WaitGroup, err *error) (*memTable, error)
}

// A commitPipeline manages the stages of committing a set of mutations
// (contained in a single Batch) atomically to the DB. The steps are
// conceptually:
//
//   1. Write the batch to the WAL and optionally sync the WAL
//   2. Apply the mutations in the batch to the memtable
//
// These two simple steps are made complicated by the desire for high
// performance. In the absence of concurrency, performance is limited by how
// fast a batch can be written (and synced) to the WAL and then added to the
// memtable, both of which are outside the purview of the commit
// pipeline. Performance under concurrency is the primary concern of the commit
// pipeline, though it also needs to maintain two invariants:
//
//   1. Batches need to be written to the WAL in sequence number order.
//   2. Batches need to be made visible for reads in sequence number order. This
//      invariant arises from the use of a single sequence number which
//      indicates which mutations are visible.
//
// Taking these invariants into account, let's revisit the work the commit
// pipeline needs to perform. Writing the batch to the WAL is necessarily
// serialized as there is a single WAL object. The order of the entries in the
// WAL defines the sequence number order. Note that writing to the WAL is
// extremely fast, usually just a memory copy. Applying the mutations in a
// batch to the memtable can occur concurrently as the underlying skiplist
// supports concurrent insertions. Publishing the visible sequence number is
// another serialization point, but one with a twist: the visible sequence
// number cannot be bumped until the mutations for earlier batches have
// finished applying to the memtable (the visible sequence number only ratchets
// up). Lastly, if requested, the commit waits for the WAL to sync. Note that
// waiting for the WAL sync after ratcheting the visible sequence number allows
// another goroutine to read committed data before the WAL has synced. This is
// similar behavior to RocksDB's manual WAL flush functionality. Application
// code needs to protect against this if necessary.
//
// The full outline of the commit pipeline operation is as follows:
//
//   with commitPipeline mutex locked:
//     assign batch sequence number
//     write batch to WAL
//   (optionally) add batch to WAL sync list
//   apply batch to memtable (concurrently)
//   wait for earlier batches to apply
//   ratchet read sequence number
//   (optionally) wait for the WAL to sync
//
// As soon as a batch has been written to the WAL, the commitPipeline mutex is
// released allowing another batch to write to the WAL. Each commit operation
// individually applies its batch to the memtable providing concurrency. The
// WAL sync happens concurrently with applying to the memtable (see
// commitPipeline.syncLoop).
//
// The "waits for earlier batches to apply" work is more complicated than might
// be expected. The obvious approach would be to keep a queue of pending
// batches and for each batch to wait for the previous batch to finish
// committing. This approach was tried initially and turned out to be too
// slow. The problem is that it causes excessive goroutine activity as each
// committing goroutine needs to wake up in order for the next goroutine to be
// unblocked. The approach taken in the current code is conceptually similar,
// though it avoids waking a goroutine to perform work that another goroutine
// can perform. A commitQueue (a single-producer, multiple-consumer queue)
// holds the ordered list of committing batches. Addition to the queue is done
// while holding commitPipeline.mutex ensuring the same ordering of batches in
// the queue as the ordering in the WAL. When a batch finishes applying to the
// memtable, it atomically updates its Batch.applied field. Ratcheting of the
// visible sequence number is done by commitPipeline.publish which loops
// dequeueing "applied" batches and ratcheting the visible sequence number. If
// we hit an unapplied batch at the head of the queue we can block as we know
// that committing of that unapplied batch will eventually find our (applied)
// batch in the queue. See commitPipeline.publish for additional commentary.
//
//
///
//
//	提交管道用于管理一系列原子提交变更（在单批中包含）到DB的阶段。
//
//	主要包括
//		将批写入到WAL，并且可选是否将WAL刷新到磁盘。
//		将变更应用到memtable。
//	这两个步骤在期望高性能的场景下变得很复杂。
//	在没有并发的情况下，性能被限制为一批数据写入（刷新）到WAL以及添加到memtable中的速度，
//	这两个点都在提交管道的范围之外。
//
//	提交管道主要关注并发场景的性能，同时还要维护下面的两个不变性：
//		所有batch需要以序列号的顺序写出到WAL。
//		所有batch需要以序列号的顺序对读操作可见。
//	这种不变性源于使用单个序列号来指示哪些变更是可见的。
//	考虑这两种不变性，让我们重新考虑提交管道需要做的工作。
//	将批量操作以串行化的方式写入WAL是必须的，因为只有一个WAL对象。
//	注意，写WAL非常快，通常都是内存拷贝。
//	将批量变更应用到内存表可以并发处理，因为底层跳跃表支持并发插入。
//	发布可见序列是另一个串行化点，但是有一个变化：可见序列号不能被碰撞，
//	直到早期批次的变更已经完成应用于内存表（可见序列号只是棘轮）。
//
//	最后，如果发出请求，则提交等待WAL同步。
//	请注意，在棘轮显示可见序列号后等待WAL同步，允许另一个goroutine在WAL同步之前读取已提交的数据。
//	这与RocksDB的手动WAL刷新功能类似。
//	如有必要，应用程序代码需要对此进行保护。
//
// 提交管道操作的完整操作如下：
// 	加锁：
//    分配批操作序列号
//    将批写到WAL
//	将批刷新到WAL同步列表中（可选）
//	将批操作应用到memtable中（并发地）
//	等待更早的批应用完成
//	棘轮读序列号
//	等待WAL刷新（可选）
//
// 	一旦批处理被写入WAL，就会释放提交管道的互斥锁，允许另一个批处理写入WAL。
// 	每个提交操作单独将其批处理应用于提供并发的内存表。
//	WAL同步与应用内存表同时发生（请参阅commitPipeline.syncLoop）。
//
//	“等待早期批次应用”的工作比预期要复杂得多。
//	显而易见的方法是保留待处理批次的队列，并为每个批次等待上一批次完成提交。
//	这种方法最初尝试过，结果太慢了。
//	问题在于它会导致过多的Go例程活动，因为每个提交的Go例程都需要唤醒，以便下一个Go例程被解除阻塞。
//	当前代码中采用的方法在概念上是相似的，但它避免唤醒Go例程来执行另一个Go例程可以执行的工作。
//
//	commitQueue（单生产者，多消费者队列）包含提交批次的有序列表。
//	在保存commitPipeline.mutex的同时完成对队列的添加，确保队列中批次的顺序与WAL中的排序相同。
//	当批处理完成对内存表的应用时，它会自动更新其Batch.applied字段。
//	通过commitPipeline.publish完成可见序列号的棘轮操作，该循环使“应用”批次出队列并使棘轮显示可见序列号。
//	如果我们在队列的头部遇到未应用的批处理，我们可以阻塞，因为我们知道提交该未应用的批处理最终将在队列中找到我们的（应用的）批。
//
//	请参阅commitPipeline.publish以获取其他评论。

type commitPipeline struct {
	// WARNING: The following struct `commitQueue` contains fields which will
	// be accessed atomically.
	//
	// Go allocations are guaranteed to be 64-bit aligned which we take advantage
	// of by placing the 64-bit fields which we access atomically at the beginning
	// of the commitPipeline struct.
	// For more information, see https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
	// Queue of pending batches to commit.
	//
	// pending 类型是 commitQueue，他是一个固定大小 lock-free 的队列实现,
	// 可以固定大小是因 sem 已经限制了最大并发(queue 实现中有 head tail index 共用一个 uint64 等技巧)，
	// pending 这里主要用用于对到达的写入 Batch 进行排序，而 seqNum 因为和 enqueue 是在持有锁的同时进行的，
	// 所以可以保证 seq 和入队列顺序一致, 而后面 Publish 部分将依赖 pending 和 seq 来帮助确定能否 Publish.
	pending commitQueue
	env     commitEnv
	sem     chan struct{}
	// The mutex to use for synchronizing access to logSeqNum and serializing
	// calls to commitEnv.write().
	mu sync.Mutex
}

func newCommitPipeline(env commitEnv) *commitPipeline {
	p := &commitPipeline{
		env: env,
		// NB: the commit concurrency is one less than SyncConcurrency because we
		// have to allow one "slot" for a concurrent WAL rotation which will close
		// and sync the WAL.
		sem: make(chan struct{}, record.SyncConcurrency-1),
	}
	return p
}

// Commit the specified batch, writing it to the WAL, optionally syncing the
// WAL, and applying the batch to the memtable. Upon successful return the
// batch's mutations will be visible for reading.
//
// Commit 方法是写入流程中非常核心的方法，里面体现了 Pebble 高性能设计之道。
//
// Commit 方法包含三个核心步骤，
//	(1) 首先是通过 prepare 方法准备好可用的 Memtable，并将数据异步写入 Wal，
//		整个 prepare 方法会加锁，因此该方法只能串行执行，但是该方法执行较快，时间复杂度为 O(1) ；
//	(2) 然后调用 apply 方法将 batch 写入 Memtable，由于 Memtable 采用无锁 Skiplist 实现，
//		可以并发执行，但是该流程相对更耗，时间复杂度为 nO(logm)（n 为 Batch 记录条数，m 为 Memtable 中 key 数目）；
//	(3) 最后调用 publish 方法将 batch 的 SeqNum 发布出去使其可见，换句话说，就是让提交的数据可读，
//		该函数可并发执行，如果 Wal 为异步落盘，该方法会比较快。
//
// 在多线程场景下，这三个阶段会被组织成 Pipeline 方式处理，我们先根据下图来直观感受下 Pipeline 的执行过程：
//
// 如上图，假设有多个线程并发执行 Commit，只有 prepare 阶段间多个线程是串行执行，其他阶段是可以并发执行的，
// 这种思想和处理器指令流水线如出一辙，这种模型可以充分发挥现代 CPU 多核的优势。
//
// 我们可以通过一个公式来计算下每个线程的平均耗时。
// 假设每个线程的 prepare 阶段耗时为 x，apply 耗时为 y，publish 耗时为 z。
// 则从第 1 个线程到第 n 个线程执行完毕，时间轴上总耗时为 n * x+y+z，每个线程的平均耗时为 (n * x+y+z)/n = x+(y+z)/n。
// 可以看出在理想情况下，并发量无穷大时，线程的平均执行时间趋近于 x，当然实际上线程数过多并不一定更优，因为线程切换也是有开销的，
// 总的来说，在合理范围内，并发量越大系统吞吐也更大。
//
// 到这里，Pipeline 的设计思想就分析完成了。
func (p *commitPipeline) Commit(b *Batch, syncWAL bool) error {
	// 参数检查
	if b.Empty() {
		return nil
	}

	// 并发度控制
	p.sem <- struct{}{}

	// Prepare the batch for committing: enqueuing the batch in the pending
	// queue, determining the batch sequence number and writing the data to the
	// WAL.
	//
	// NB: We set Batch.commitErr on error so that the batch won't be a candidate
	// for reuse. See Batch.release().
	//
	//
	// prepare 主要做的事情：
	//	1. 准备好可用的 memTable
	//	2. 写 wal(可以是异步的，将 wal 塞入 queue, 再异步写，提高并发性能)
	//
	// prepare 中会对 pipeline 加锁，因此整个过程是串行执行，不过该函数通常很快。
	mem, err := p.prepare(b, syncWAL)
	if err != nil {
		b.db = nil // prevent batch reuse on error
		return err
	}

	// Apply the batch to the memTable.
	//
	// 将 batch 写入 memTable ，这里可以是并发执行，该流程是 pipeline 中最耗的
	if err := p.env.apply(b, mem); err != nil {
		b.db = nil // prevent batch reuse on error
		return err
	}

	// Publish the batch sequence number.
	//
	// 将 batch 的 SeqNum 发布出去使其可见，换句话说，就是让提交的数据可读
	p.publish(b)

	<-p.sem


	if b.commitErr != nil {
		b.db = nil // prevent batch reuse on error
	}

	return b.commitErr
}

// AllocateSeqNum allocates count sequence numbers, invokes the prepare
// callback, then the apply callback, and then publishes the sequence
// numbers. AllocateSeqNum does not write to the WAL or add entries to the
// memtable. AllocateSeqNum can be used to sequence an operation such as
// sstable ingestion within the commit pipeline. The prepare callback is
// invoked with commitPipeline.mu held, but note that DB.mu is not held and
// must be locked if necessary.
//
//
//
func (p *commitPipeline) AllocateSeqNum(count int, prepare func(), apply func(seqNum uint64)) {

	// This method is similar to Commit and prepare. Be careful about trying to
	// share additional code with those methods because Commit and prepare are
	// performance critical code paths.

	//
	b := newBatch(nil)
	defer b.release()

	// Give the batch a count of 1 so that the log and visible sequence number
	// are incremented correctly.
	b.data = make([]byte, batchHeaderLen)
	b.setCount(uint32(count))
	b.commit.Add(1)

	// 并发度控制
	p.sem <- struct{}{}

	// 加锁
	p.mu.Lock()

	// Enqueue the batch in the pending queue. Note that while the pending queue
	// is lock-free, we want the order of batches to be the same as the sequence
	// number order.
	//
	// 将 batch 入队，等待被 commit 。
	p.pending.enqueue(b)

	// Assign the batch a sequence number. Note that we use atomic operations
	// here to handle concurrent reads of logSeqNum. commitPipeline.mu provides
	// mutual exclusion for other goroutines writing to logSeqNum.
	//
	// 给 Batch 分配一个序列号。
	// 注意，我们在这里使用原子操作来处理 logSeqNum 的并发读取。
	// commitPipeline.mu 提供了为其他写入 logSeqNum 的 goroutine 提供相互排斥。

	// 获取日志序号
	logSeqNum := atomic.AddUint64(p.env.logSeqNum, uint64(count)) - uint64(count)

	seqNum := logSeqNum
	if seqNum == 0 {
		// We can't use the value 0 for the global seqnum during ingestion, because
		// 0 indicates no global seqnum. So allocate one more seqnum.
		//
		// 不能使用 0 作为序号
		atomic.AddUint64(p.env.logSeqNum, 1)
		seqNum++
	}

	// 设置 Batch 的日志序号
	b.setSeqNum(seqNum)

	// Wait for any outstanding writes to the memtable to complete. This is
	// necessary for ingestion so that the check for memtable overlap can see any
	// writes that were sequenced before the ingestion. The spin loop is
	// unfortunate, but obviates the need for additional synchronization.
	//
	for {
		// 获取当前可见的序号
		visibleSeqNum := atomic.LoadUint64(p.env.visibleSeqNum)
		// 如果可见序号
		if visibleSeqNum == logSeqNum {
			break
		}
		// 让出运行权
		runtime.Gosched()
	}

	// Invoke the prepare callback. Note the lack of error reporting. Even if the
	// callback internally fails, the sequence number needs to be published in
	// order to allow the commit pipeline to proceed.
	//
	//
	prepare()

	// 解锁
	p.mu.Unlock()

	// Invoke the apply callback.
	apply(b.SeqNum())

	// Publish the sequence number.
	p.publish(b)

	<-p.sem
}

// prepare 方法主要是准备 batch 写入的 MemTable 及异步写 wal。
//
// 首先会根据 Wal 是否为同步模式来决定 commit 的等待计数，初始计数为 1，
// 是因为必须等待 batch 的发布（后面 publish 方法中会看到），
// 如果 Wal 为同步模式，还必须等待 Wal 刷盘完成。
// 注意，这里只是计数，commit.Wait 会在 publish 中调用。
//
// 然后加锁进入临界区，在临界区内：
//	将 batch enqueue 到 commitPipeline.pending；
// 	为 batch 生成同 enqueue 顺序的 seqNum 递增序号，由于外面有上锁，因此在并发环境下，batch 在队列中的顺序和 SeqNum 的顺序关系一致，即先入队列的 SeqNum 越小；
//	调用 db.commitWrite 发起 WAL 写入(可不等待完成)。
//
// 这里的 write 方法其实是 DB.commitWrite，我们来看下写入的逻辑。
//
//
//
//
func (p *commitPipeline) prepare(b *Batch, syncWAL bool) (*memTable, error) {

	n := uint64(b.Count())
	if n == invalidBatchCount {
		return nil, ErrInvalidBatch
	}

	// 因为 prepare 和 publish 是成对的，所以 wg 至少为 1 ；
	// 如果开启了 sync 标记，则还要等待后台 flush 协程完成落盘，需额外 +1 。
	count := 1
	if syncWAL {
		count++
	}

	// count represents the waiting needed for publish, and optionally the
	// waiting needed for the WAL sync.
	//
	// commit 为 sync.Group，用于等待 batch publish，如果 wal 为同步模式，也会等待 wal 刷盘。
	b.commit.Add(count)

	var syncWG *sync.WaitGroup
	var syncErr *error
	if syncWAL {
		syncWG, syncErr = &b.commit, &b.commitErr
	}

	// 加锁
	p.mu.Lock()

	// Enqueue the batch in the pending queue. Note that while the pending queue
	// is lock-free, we want the order of batches to be the same as the sequence
	// number order.
	//
	// 将 batch 入队列，保证并发场景下 batch 的顺序
	p.pending.enqueue(b)

	// Assign the batch a sequence number. Note that we use atomic operations
	// here to handle concurrent reads of logSeqNum. commitPipeline.mu provides
	// mutual exclusion for other goroutines writing to logSeqNum.
	//
	// 设置 batch 的序列号，batch 的 n 条记录序列号递增
	b.setSeqNum(atomic.AddUint64(p.env.logSeqNum, n) - n)

	// Write the data to the WAL.
	//
	// 将数据写入 wal 并返回 MemTable
	mem, err := p.env.write(b, syncWG, syncErr)

	// 解锁
	p.mu.Unlock()

	return mem, err
}


// 在 commitPipeline.publish 中将从 commitPipeline.pending 中按顺序 dequeue batch 并
// 对 visibleSeqNum 进行更新来进行 publish， dequeue 出的 batch 可能是当前 batch 前置 batch 或
// 当前 batch， batch 可能被自己 publish 也可能被其他人 publish，每个 batch 处理都尝试 dequeue 并
// 处理前置 batch 直到自己，最后总会被某一个人处理，但当前 batch 的处理需通过 WaitGroup 等待 “被 Publish”
// 且 “WAL Sync” 完成才会返回，从而保证队列中必须将前置的 Batch Publish 完成才能 Publish 后续 Batch。
//
// 最后 Publish 操作最终将通过 versionSet.atomic.visibleSeqNum 的修改来控制读取侧的的可见性。
//
//
//
// 执行到 publish 方法，说明当前 batch b 已经被 apply 到 MemTable 中了，
// 这里先将其标记为 applied 状态，然后从队列取出队头 batch，注意：
//	1. 取出的 batch 有可能是当前线程对应的 batch，也有可能是其他线程的 batch；
//	2. 如果队头 batch 并未 apply，则其并不会出队列，同时返回 nil 。
//
// 如果返回的是 nil，则调用 commit.Wait 等待 SeqNum 发布和 Wal 落盘，
// 否则，通过循环 + CAS 的方式更新整个 DB 的 visibleSeqNum。
//
// 我们看下第二层 for 循环中更新 visibleSeqNum 的逻辑，
// 首先通过原子操作取出 visibleSeqNum，然后根据 t 计算新的 SeqNum，如果新的 SeqNum < visibleSeqNum，
// 说明有排在 t 后面的 batch 已经被其他线程 publish 了，那么 t 也就相当于 publish 了，直接退出循环，
// 否则更新 visibleSeqNum。
//
// t 成功 publish 后调用 t.commit 将计数减一，而后回到第一层循环继续消费 pengding 队列。
//
// 可以看到，publish 设计得比较有意思，支持多个消费者同时消费 pending 队列，而且每个线程可以消费其他线程的 batch。
// 这样做的好处是每个线程不必等着处理自己的 batch，多个线程可以接连不断地消费 pending 队列并独立 publish batch，充分利用多核优势提升性能。
//
// 到此，pipeline 的三个阶段便分析完成，整个写入流程也就结束了。
//
// 下一章将继续补充写入路径上对 large batch 的特殊处理，以及详细讲解 makeRoomForWrite 的执行逻辑。
func (p *commitPipeline) publish(b *Batch) {

	// Mark the batch as applied.
	// 标记当前 batch 已经 apply
	atomic.StoreUint32(&b.applied, 1)

	// Loop dequeuing applied batches from the pending queue. If our batch was
	// the head of the pending queue we are guaranteed that either we'll publish
	// it or someone else will dequeue and publish it. If our batch is not the
	// head of the queue then either we'll dequeue applied batches and reach our
	// batch or there is an unapplied batch blocking us. When that unapplied
	// batch applies it will go through the same process and publish our batch
	// for us.
	for {

		// 从队列取出 batch，该 batch 可能是当前 batch，也可能是其他线程提交的 batch
		t := p.pending.dequeue()
		if t == nil {
			// Wait for another goroutine to publish us. We might also be waiting for
			// the WAL sync to finish.
			//
			// 如果为 nil ，意味着 pending 中已无待 publish 的 batch ，所以当前 batch 已经被
			// 其它线程取出并处理，这里等待当前 batch 被别的线程处理完毕通过 b.commit 通知过来。
			b.commit.Wait()
			break
		}

		if atomic.LoadUint32(&t.applied) != 1 {
			panic("not reached")
		}

		// We're responsible for publishing the sequence number for batch t, but
		// another concurrent goroutine might sneak in and publish the sequence
		// number for a subsequent batch. That's ok as all we're guaranteeing is
		// that the sequence number ratchets up.
		//
		// 通过循环 + cas 的方式更新当前可见的 SeqNum
		for {

			// 首先通过原子操作取出 visibleSeqNum
			curSeqNum := atomic.LoadUint64(p.env.visibleSeqNum)

			// 计算新的 SeqNum
			newSeqNum := t.SeqNum() + uint64(t.Count())

			// 如果新的 SeqNum < visibleSeqNum ，说明有排在 t 后面的 batch 已经被其他线程 publish 了，
			// 那么 t 也就相当于 publish 了，直接退出循环，否则更新 visibleSeqNum。
			if newSeqNum <= curSeqNum {
				// t's sequence number has already been published.
				break
			}

			// 更新 visibleSeqNum
			if atomic.CompareAndSwapUint64(p.env.visibleSeqNum, curSeqNum, newSeqNum) {
				// We successfully published t's sequence number.
				break
			}

		}

		// t 成功 publish 后调用 t.commit.Done() 将计数减一，并通知 waiter
		t.commit.Done()

		// 回到第一层循环继续消费 pending 队列
	}
}

// ratchetSeqNum allocates and marks visible all sequence numbers less than
// but excluding `nextSeqNum`.
func (p *commitPipeline) ratchetSeqNum(nextSeqNum uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	logSeqNum := atomic.LoadUint64(p.env.logSeqNum)
	if logSeqNum >= nextSeqNum {
		return
	}

	count := nextSeqNum - logSeqNum
	_ = atomic.AddUint64(p.env.logSeqNum, uint64(count)) - uint64(count)
	atomic.StoreUint64(p.env.visibleSeqNum, nextSeqNum)
}
