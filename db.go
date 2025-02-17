// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package pebble provides an ordered key/value store.
package pebble // import "github.com/cockroachdb/pebble"

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/manual"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
)

const (
	// minTableCacheSize is the minimum size of the table cache, for a single db.
	minTableCacheSize = 64

	// numNonTableCacheFiles is an approximation for the number of files
	// that we don't use for table caches, for a given db.
	numNonTableCacheFiles = 10
)

var (
	// ErrNotFound is returned when a get operation does not find the requested
	// key.
	ErrNotFound = base.ErrNotFound
	// ErrClosed is panicked when an operation is performed on a closed snapshot or
	// DB. Use errors.Is(err, ErrClosed) to check for this error.
	ErrClosed = errors.New("pebble: closed")
	// ErrReadOnly is returned when a write operation is performed on a read-only
	// database.
	ErrReadOnly = errors.New("pebble: read-only")
	// errNoSplit indicates that the user is trying to perform a range key
	// operation but the configured Comparer does not provide a Split
	// implementation.
	errNoSplit = errors.New("pebble: Comparer.Split required for range key operations")
)

// Reader is a readable key/value store.
//
// It is safe to call Get and NewIter from concurrent goroutines.
type Reader interface {
	// Get gets the value for the given key. It returns ErrNotFound if the DB
	// does not contain the key.
	//
	// The caller should not modify the contents of the returned slice, but it is
	// safe to modify the contents of the argument after Get returns. The
	// returned slice will remain valid until the returned Closer is closed. On
	// success, the caller MUST call closer.Close() or a memory leak will occur.
	Get(key []byte) (value []byte, closer io.Closer, err error)

	// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
	// return false). The iterator can be positioned via a call to SeekGE,
	// SeekLT, First or Last.
	NewIter(o *IterOptions) *Iterator

	// Close closes the Reader. It may or may not close any underlying io.Reader
	// or io.Writer, depending on how the DB was created.
	//
	// It is not safe to close a DB until all outstanding iterators are closed.
	// It is valid to call Close multiple times. Other methods should not be
	// called after the DB has been closed.
	Close() error
}

// Writer is a writable key/value store.
//
// Goroutine safety is dependent on the specific implementation.
type Writer interface {
	// Apply the operations contained in the batch to the DB.
	//
	// It is safe to modify the contents of the arguments after Apply returns.
	Apply(batch *Batch, o *WriteOptions) error

	// Delete deletes the value for the given key. Deletes are blind all will
	// succeed even if the given key does not exist.
	//
	// It is safe to modify the contents of the arguments after Delete returns.
	Delete(key []byte, o *WriteOptions) error

	// SingleDelete is similar to Delete in that it deletes the value for the given key. Like Delete,
	// it is a blind operation that will succeed even if the given key does not exist.
	//
	// WARNING: Undefined (non-deterministic) behavior will result if a key is overwritten and
	// then deleted using SingleDelete. The record may appear deleted immediately, but be
	// resurrected at a later time after compactions have been performed. Or the record may
	// be deleted permanently. A Delete operation lays down a "tombstone" which shadows all
	// previous versions of a key. The SingleDelete operation is akin to "anti-matter" and will
	// only delete the most recently written version for a key. These different semantics allow
	// the DB to avoid propagating a SingleDelete operation during a compaction as soon as the
	// corresponding Set operation is encountered. These semantics require extreme care to handle
	// properly. Only use if you have a workload where the performance gain is critical and you
	// can guarantee that a record is written once and then deleted once.
	//
	// SingleDelete is internally transformed into a Delete if the most recent record for a key is either
	// a Merge or Delete record.
	//
	// It is safe to modify the contents of the arguments after SingleDelete returns.
	SingleDelete(key []byte, o *WriteOptions) error

	// DeleteRange deletes all of the point keys (and values) in the range
	// [start,end) (inclusive on start, exclusive on end). DeleteRange does NOT
	// delete overlapping range keys (eg, keys set via RangeKeySet).
	//
	// It is safe to modify the contents of the arguments after DeleteRange
	// returns.
	DeleteRange(start, end []byte, o *WriteOptions) error

	// LogData adds the specified to the batch. The data will be written to the
	// WAL, but not added to memtables or sstables. Log data is never indexed,
	// which makes it useful for testing WAL performance.
	//
	// It is safe to modify the contents of the argument after LogData returns.
	LogData(data []byte, opts *WriteOptions) error

	// Merge merges the value for the given key. The details of the merge are
	// dependent upon the configured merge operation.
	//
	// It is safe to modify the contents of the arguments after Merge returns.
	Merge(key, value []byte, o *WriteOptions) error

	// Set sets the value for the given key. It overwrites any previous value
	// for that key; a DB is not a multi-map.
	//
	// It is safe to modify the contents of the arguments after Set returns.
	Set(key, value []byte, o *WriteOptions) error

	// Experimental returns the experimental write API.
	Experimental() ExperimentalWriter
}

// ExperimentalWriter provides access to experimental features of a Batch.
type ExperimentalWriter interface {
	Writer

	// RangeKeySet sets a range key mapping the key range [start, end) at the MVCC
	// timestamp suffix to value. The suffix is optional. If any portion of the key
	// range [start, end) is already set by a range key with the same suffix value,
	// RangeKeySet overrides it.
	//
	// It is safe to modify the contents of the arguments after RangeKeySet returns.
	//
	// WARNING: This is an experimental feature with limited functionality.
	RangeKeySet(start, end, suffix, value []byte, opts *WriteOptions) error

	// RangeKeyUnset removes a range key mapping the key range [start, end) at the
	// MVCC timestamp suffix. The suffix may be omitted to remove an unsuffixed
	// range key. RangeKeyUnset only removes portions of range keys that fall within
	// the [start, end) key span, and only range keys with suffixes that exactly
	// match the unset suffix.
	//
	// It is safe to modify the contents of the arguments after RangeKeyUnset
	// returns.
	//
	// WARNING: This is an experimental feature with limited functionality.
	RangeKeyUnset(start, end, suffix []byte, opts *WriteOptions) error

	// RangeKeyDelete deletes all of the range keys in the range [start,end)
	// (inclusive on start, exclusive on end). It does not delete point keys (for
	// that use DeleteRange). RangeKeyDelete removes all range keys within the
	// bounds, including those with or without suffixes.
	//
	// It is safe to modify the contents of the arguments after RangeKeyDelete
	// returns.
	//
	// WARNING: This is an experimental feature with limited functionality.
	RangeKeyDelete(start, end []byte, opts *WriteOptions) error
}

// DB provides a concurrent, persistent ordered key/value store.
//
// A DB's basic operations (Get, Set, Delete) should be self-explanatory. Get
// and Delete will return ErrNotFound if the requested key is not in the store.
// Callers are free to ignore this error.
//
// A DB also allows for iterating over the key/value pairs in key order. If d
// is a DB, the code below prints all key/value pairs whose keys are 'greater
// than or equal to' k:
//
//	iter := d.NewIter(readOptions)
//	for iter.SeekGE(k); iter.Valid(); iter.Next() {
//		fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
//	}
//	return iter.Close()
//
// The Options struct holds the optional parameters for the DB, including a
// Comparer to define a 'less than' relationship over keys. It is always valid
// to pass a nil *Options, which means to use the default parameter values. Any
// zero field of a non-nil *Options also means to use the default value for
// that parameter. Thus, the code below uses a custom Comparer, but the default
// values for every other parameter:
//
//	db := pebble.Open(&Options{
//		Comparer: myComparer,
//	})
//
//
//
type DB struct {

	// WARNING: The following struct `atomic` contains fields which are accessed
	// atomically.
	//
	// Go allocations are guaranteed to be 64-bit aligned which we take advantage
	// of by placing the 64-bit fields which we access atomically at the beginning
	// of the DB struct. For more information, see https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
	//
	//
	atomic struct {
		// The count and size of referenced memtables. This includes memtables
		// present in DB.mu.mem.queue, as well as memtables that have been flushed
		// but are still referenced by an inuse readState.
		//
		//
		memTableCount    int64
		memTableReserved int64 // number of bytes reserved in the cache for memtables

		// bytesFlushed is the number of bytes flushed in the current flush. This
		// must be read/written atomically since it is accessed by both the flush
		// and compaction routines.
		bytesFlushed uint64

		// bytesCompacted is the number of bytes compacted in the current compaction.
		// This is used as a dummy variable to increment during compaction, and the
		// value is not used anywhere.
		bytesCompacted uint64

		// The size of the current log file (i.e. db.mu.log.queue[len(queue)-1].
		logSize uint64

		// The number of bytes available on disk.
		diskAvailBytes uint64
	}

	//
	cacheID        uint64
	dirname        string
	walDirname     string
	opts           *Options
	cmp            Compare
	equal          Equal
	merge          Merge
	split          Split
	abbreviatedKey AbbreviatedKey

	// The threshold for determining when a batch is "large" and
	// will skip being inserted into a memtable.
	//
	// 超过此阈值的 batch 不会插入到 memtable 。
	largeBatchThreshold int


	// The current OPTIONS file number.
	optionsFileNum FileNum
	// The on-disk size of the current OPTIONS file.
	optionsFileSize uint64

	fileLock io.Closer
	dataDir  vfs.File
	walDir   vfs.File

	tableCache *tableCacheContainer
	newIters   tableNewIters

	commit *commitPipeline

	// readState provides access to the state needed for reading without needing
	// to acquire DB.mu.
	readState struct {
		sync.RWMutex
		val *readState
	}
	// logRecycler holds a set of log file numbers that are available for
	// reuse. Writing to a recycled log file is faster than to a new log file on
	// some common filesystems (xfs, and ext3/4) due to avoiding metadata
	// updates.
	logRecycler logRecycler

	closed   *atomic.Value
	closedCh chan struct{}

	compactionLimiter limiter
	flushLimiter      limiter
	deletionLimiter   limiter

	// Async deletion jobs spawned by cleaners increment this WaitGroup, and
	// call Done when completed. Once `d.mu.cleaning` is false, the db.Close()
	// goroutine needs to call Wait on this WaitGroup to ensure all cleaning
	// and deleting goroutines have finished running. As deletion goroutines
	// could grab db.mu, it must *not* be held while deleters.Wait() is called.
	deleters sync.WaitGroup

	// During an iterator close, we may asynchronously schedule read compactions.
	// We want to wait for those goroutines to finish, before closing the DB.
	// compactionShedulers.Wait() should not be called while the DB.mu is held.
	compactionSchedulers sync.WaitGroup

	// The main mutex protecting internal DB state. This mutex encompasses many
	// fields because those fields need to be accessed and updated atomically. In
	// particular, the current version, log.*, mem.*, and snapshot list need to
	// be accessed and updated atomically during compaction.
	//
	// Care is taken to avoid holding DB.mu during IO operations. Accomplishing
	// this sometimes requires releasing DB.mu in a method that was called with
	// it held. See versionSet.logAndApply() and DB.makeRoomForWrite() for
	// examples. This is a common pattern, so be careful about expectations that
	// DB.mu will be held continuously across a set of calls.
	mu struct {

		sync.Mutex

		formatVers struct {
			// vers is the database's current format major version.
			// Backwards-incompatible features are gated behind new
			// format major versions and not enabled until a database's
			// version is ratcheted upwards.
			vers FormatMajorVersion
			// marker is the atomic marker for the format major version.
			// When a database's version is ratcheted upwards, the
			// marker is moved in order to atomically record the new
			// version.
			marker *atomicfs.Marker
		}

		// The ID of the next job. Job IDs are passed to event listener
		// notifications and act as a mechanism for tying together the events and
		// log messages for a single job such as a flush, compaction, or file
		// ingestion. Job IDs are not serialized to disk or used for correctness.
		nextJobID int

		// The collection of immutable versions and state about the log and visible
		// sequence numbers. Use the pointer here to ensure the atomic fields in
		// version set are aligned properly.
		versions *versionSet

		//
		log struct {

			// The queue of logs, containing both flushed and unflushed logs. The
			// flushed logs will be a prefix, the unflushed logs a suffix.
			// The delimeter between flushed and unflushed logs is versionSet.minUnflushedLogNum.
			//
			// 日志文件队列，包含 flushed 和 unflushed 的所有日志文件。
			// flushed 和 unflushed 的日志之间的界限是 versionSet.minUnflushedLogNum 。
			queue []fileInfo

			// The number of input bytes to the log. This is the raw size of the
			// batches written to the WAL, without the overhead of the record
			// envelopes.
			bytesIn uint64

			// The LogWriter is protected by commitPipeline.mu. This allows log
			// writes to be performed without holding DB.mu, but requires both
			// commitPipeline.mu and DB.mu to be held when rotating the WAL/memtable
			// (i.e. makeRoomForWrite).
			*record.LogWriter
		}


		//
		mem struct {
			// Condition variable used to serialize memtable switching.
			// See DB.makeRoomForWrite().
			cond sync.Cond

			// The current mutable memTable.
			mutable *memTable

			// Queue of flushables (the mutable memtable is at end). Elements are
			// added to the end of the slice and removed from the beginning. Once an
			// index is set it is never modified making a fixed slice immutable and
			// safe for concurrent reads.
			//
			//
			queue flushableList


			// True when the memtable is actively been switched.
			// Both mem.mutable and log.LogWriter are invalid while switching is true.
			switching bool


			// nextSize is the size of the next memtable. The memtable size starts at
			// min(256KB,Options.MemTableSize) and doubles each time a new memtable
			// is allocated up to Options.MemTableSize. This reduces the memory
			// footprint of memtables when lots of DB instances are used concurrently
			// in test environments.
			nextSize int
		}


		compact struct {
			// Condition variable used to signal when a flush or compaction has
			// completed. Used by the write-stall mechanism to wait for the stall
			// condition to clear. See DB.makeRoomForWrite().
			cond sync.Cond
			// True when a flush is in progress.
			flushing bool
			// The number of ongoing compactions.
			compactingCount int
			// The list of deletion hints, suggesting ranges for delete-only
			// compactions.
			deletionHints []deleteCompactionHint
			// The list of manual compactions. The next manual compaction to perform
			// is at the start of the list. New entries are added to the end.
			manual []*manualCompaction
			// inProgress is the set of in-progress flushes and compactions.
			inProgress map[*compaction]struct{}

			// rescheduleReadCompaction indicates to an iterator that a read compaction
			// should be scheduled.
			rescheduleReadCompaction bool

			// readCompactions is a readCompactionQueue which keeps track of the
			// compactions which we might have to perform.
			readCompactions readCompactionQueue
			// See DB.RegisterFlushCompletedCallback.
			flushCompletedCallback func()
		}

		cleaner struct {
			// Condition variable used to signal the completion of a file cleaning
			// operation or an increment to the value of disabled. File cleaning operations are
			// serialized, and a caller trying to do a file cleaning operation may wait
			// until the ongoing one is complete.
			cond sync.Cond
			// True when a file cleaning operation is in progress. False does not necessarily
			// mean all cleaning jobs have completed; see the comment on d.deleters.
			cleaning bool
			// Non-zero when file cleaning is disabled. The disabled count acts as a
			// reference count to prohibit file cleaning. See
			// DB.{disable,Enable}FileDeletions().
			disabled int
		}

		// The list of active snapshots.
		snapshots snapshotList

		tableStats struct {
			// Condition variable used to signal the completion of a
			// job to collect table stats.
			cond sync.Cond
			// True when a stat collection operation is in progress.
			loading bool
			// True if stat collection has loaded statistics for all tables
			// other than those listed explcitly in pending. This flag starts
			// as false when a database is opened and flips to true once stat
			// collection has caught up.
			loadedInitial bool
			// A slice of files for which stats have not been computed.
			// Compactions, ingests, flushes append files to be processed. An
			// active stat collection goroutine clears the list and processes
			// them.
			pending []manifest.NewFileEntry
		}

		tableValidation struct {
			// cond is a condition variable used to signal the completion of a
			// job to validate one or more sstables.
			cond sync.Cond
			// pending is a slice of metadata for sstables waiting to be
			// validated.
			pending []newFileEntry
			// validating is set to true when validation is running.
			validating bool
		}
	}

	// rangeKeys is a temporary field so that Pebble can provide a non-durable
	// implementation of range keys in advance of the real implementation.
	// TODO(jackson): Remove this.
	rangeKeys *RangeKeysArena

	// Normally equal to time.Now() but may be overridden in tests.
	timeNow func() time.Time
}

var _ Reader = (*DB)(nil)
var _ Writer = (*DB)(nil)

// Get gets the value for the given key. It returns ErrNotFound if the DB does
// not contain the key.
//
// The caller should not modify the contents of the returned slice, but it is
// safe to modify the contents of the argument after Get returns. The returned
// slice will remain valid until the returned Closer is closed. On success, the
// caller MUST call closer.Close() or a memory leak will occur.
func (d *DB) Get(key []byte) ([]byte, io.Closer, error) {
	return d.getInternal(key, nil /* batch */, nil /* snapshot */)
}

type getIterAlloc struct {
	dbi    Iterator
	keyBuf []byte
	get    getIter
}

var getIterAllocPool = sync.Pool{
	New: func() interface{} {
		return &getIterAlloc{}
	},
}

func (d *DB) getInternal(key []byte, b *Batch, s *Snapshot) ([]byte, io.Closer, error) {

	// 已关闭
	if err := d.closed.Load(); err != nil {
		panic(err)
	}

	// Grab and reference the current readState. This prevents the underlying
	// files in the associated version from being deleted if there is a current
	// compaction. The readState is unref'd by Iterator.Close().
	readState := d.loadReadState()

	// Determine the seqNum to read at after grabbing the read state (current and
	// memTables) above.
	var seqNum uint64
	if s != nil {
		seqNum = s.seqNum
	} else {
		seqNum = atomic.LoadUint64(&d.mu.versions.atomic.visibleSeqNum)
	}

	buf := getIterAllocPool.Get().(*getIterAlloc)

	get := &buf.get
	*get = getIter{
		logger:   d.opts.Logger,
		cmp:      d.cmp,
		equal:    d.equal,
		newIters: d.newIters,
		snapshot: seqNum,
		key:      key,
		batch:    b,
		mem:      readState.memtables,
		l0:       readState.current.L0SublevelFiles,
		version:  readState.current,
	}

	// Strip off memtables which cannot possibly contain the seqNum being read at.
	for len(get.mem) > 0 {
		n := len(get.mem)
		if logSeqNum := get.mem[n-1].logSeqNum; logSeqNum < seqNum {
			break
		}
		get.mem = get.mem[:n-1]
	}


	// 构造迭代器
	i := &buf.dbi
	*i = Iterator{
		getIterAlloc: buf,				//
		cmp:          d.cmp,			// 比较函数
		equal:        d.equal,		// 相等函数
		iter:         get,				//
		merge:        d.merge,
		split:        d.split,
		readState:    readState,
		keyBuf:       buf.keyBuf,
	}

	// 没有找到
	if !i.First() {
		// 关闭迭代器，失败报错
		if err := i.Close(); err != nil {
			return nil, nil, err
		}
		// 没有找到，报错 NotFound
		return nil, nil, ErrNotFound
	}

	// 找到了，返回 value
	return i.Value(), i, nil
}

// Set sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map.
//
// It is safe to modify the contents of the arguments after Set returns.
//
// 写数据
//
// Batch 格式：
// 	|-        header       -|-   body  -|
//	+-----------+-----------+---- ... --+
//	|SeqNum (8B)| Count (4B)|   Entries |
//	+-----------+-----------+-----------+
//
// Batch 由 header 和 body 组成：
// header 包含 8 字节 SeqNum 和 4 字节 Count，SeqNum 表示 batch 的序列号，Count 表示 Entry 个数。
// body 由多个 Entry 构成。
//
// Entry 格式：
// +----------+--------+-----+----------+-------+
// |Kind (1B) | KeyLen | Key | ValueLen | Value |
// +----------+--------+-----+----------+-------+
//
// Kind 表示 Entry 的类型，如 SET、DELETE、MERGE 等，这里写入的类型为 SET 。
// KeyLen 表示 Key 的大小，为 VInt 类型，最大 4 字节，Key 即为 KeyLen 字节序列，
// 同理，ValueLen 表示 Value 大小，为 VInt 类型，最大 4 字节，Value 为 ValueLen 字节序列。
//
func (d *DB) Set(key, value []byte, opts *WriteOptions) error {
	// 获取一个 batch
	b := newBatch(d)

	// 调用 batch.Set() 更新 kv
	_ = b.Set(key, value, opts)

	// 参数检验
	if err := d.Apply(b, opts); err != nil {
		return err
	}

	// Only release the batch on success.
	// 执行成功后，释放 batch
	b.release()
	return nil
}

// Delete deletes the value for the given key. Deletes are blind all will
// succeed even if the given key does not exist.
//
// It is safe to modify the contents of the arguments after Delete returns.
func (d *DB) Delete(key []byte, opts *WriteOptions) error {
	b := newBatch(d)
	_ = b.Delete(key, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// SingleDelete adds an action to the batch that single deletes the entry for key.
// See Writer.SingleDelete for more details on the semantics of SingleDelete.
//
// It is safe to modify the contents of the arguments after SingleDelete returns.
func (d *DB) SingleDelete(key []byte, opts *WriteOptions) error {
	b := newBatch(d)
	_ = b.SingleDelete(key, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// DeleteRange deletes all of the keys (and values) in the range [start,end)
// (inclusive on start, exclusive on end).
//
// It is safe to modify the contents of the arguments after DeleteRange
// returns.
func (d *DB) DeleteRange(start, end []byte, opts *WriteOptions) error {
	b := newBatch(d)
	_ = b.DeleteRange(start, end, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// Merge adds an action to the DB that merges the value at key with the new
// value. The details of the merge are dependent upon the configured merge
// operator.
//
// It is safe to modify the contents of the arguments after Merge returns.
func (d *DB) Merge(key, value []byte, opts *WriteOptions) error {
	b := newBatch(d)
	_ = b.Merge(key, value, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// LogData adds the specified to the batch. The data will be written to the
// WAL, but not added to memtables or sstables. Log data is never indexed,
// which makes it useful for testing WAL performance.
//
// It is safe to modify the contents of the argument after LogData returns.
func (d *DB) LogData(data []byte, opts *WriteOptions) error {
	b := newBatch(d)
	_ = b.LogData(data, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// Experimental returns the experimental write API.
func (d *DB) Experimental() ExperimentalWriter {
	return experimentalDB{d}
}

type experimentalDB struct {
	*DB
}

// RangeKeySet implements the ExperimentalWriter interface.
func (e experimentalDB) RangeKeySet(start, end, suffix, value []byte, opts *WriteOptions) error {
	b := newBatch(e.DB)
	eb := b.Experimental()
	_ = eb.RangeKeySet(start, end, suffix, value, opts)
	if err := e.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// RangeKeyUnset implements the ExperimentalWriter interface.
func (e experimentalDB) RangeKeyUnset(start, end, suffix []byte, opts *WriteOptions) error {
	b := newBatch(e.DB)
	eb := b.Experimental()
	_ = eb.RangeKeyUnset(start, end, suffix, opts)
	if err := e.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// RangeKeyDelete implements the ExperimentalWriter interface.
func (e experimentalDB) RangeKeyDelete(start, end []byte, opts *WriteOptions) error {
	b := newBatch(e.DB)
	eb := b.Experimental()
	_ = eb.RangeKeyDelete(start, end, opts)
	if err := e.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// Apply the operations contained in the batch to the DB. If the batch is large
// the contents of the batch may be retained by the database. If that occurs
// the batch contents will be cleared preventing the caller from attempting to
// reuse them.
//
// It is safe to modify the contents of the arguments after Apply returns.
//
//
// 应用 batch 到 DB 。
func (d *DB) Apply(batch *Batch, opts *WriteOptions) error {
	// 已关闭？
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	// 已提交？
	if atomic.LoadUint32(&batch.applied) != 0 {
		panic("pebble: batch already applied")
	}
	// 只读操作？
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	// 参数合法？
	if batch.db != nil && batch.db != d {
		panic(fmt.Sprintf("pebble: batch db mismatch: %p != %p", batch.db, d))
	}

	// 同步落盘？
	sync := opts.GetSync()

	// 如果开启同步落盘，但是禁用了 WAL ，则参数冲突，报错
	if sync && d.opts.DisableWAL {
		return errors.New("pebble: WAL disabled")
	}

	// ???
	if batch.countRangeKeys > 0 {

		//
		if d.split == nil {
			return errNoSplit
		}

		//
		if d.opts.Experimental.RangeKeys == nil {
			panic("pebble: range keys require the Experimental.RangeKeys option")
		}

		if d.FormatMajorVersion() < FormatRangeKeys {
			panic(fmt.Sprintf(
				"pebble: range keys require at least format major version %d (current: %d)",
				FormatRangeKeys, d.FormatMajorVersion(),
			))
		}

		// TODO(jackson): Assert that all range key operands are suffixless.
	}


	// ???
	if batch.db == nil {
		batch.refreshMemTableSize()
	}


	// 如果 batch 的大小超过阈值，则该 batch 被视为 large batch，
	// 这里会调用 newFlushableBatch 方法，根据 batch 生成一个 flushable（可以理解成 immutable memtable）。
	if int(batch.memTableSize) >= d.largeBatchThreshold {
		batch.flushable = newFlushableBatch(batch, d.opts.Comparer)
	}

	//
	if err := d.commit.Commit(batch, sync); err != nil {
		// There isn't much we can do on an error here. The commit pipeline will be
		// horked at this point.
		d.opts.Logger.Fatalf("%v", err)
	}


	// If this is a large batch, we need to clear the batch contents as the
	// flushable batch may still be present in the flushables queue.
	//
	// TODO(peter): Currently large batches are written to the WAL. We could
	// skip the WAL write and instead wait for the large batch to be flushed to
	// an sstable. For a 100 MB batch, this might actually be faster. For a 1
	// GB batch this is almost certainly faster.
	if batch.flushable != nil {
		batch.data = nil
	}


	return nil
}

// 在 Prepare 阶段已经准备好空间足够的 MemTable ，在 Apply 阶段便会将 batch 写入到 MemTable 中。
//
// commitApply 方法中，
//	(1) 首先通过 memTable.apply() 将 batch 写入 MemTable，MemTable 内部是无锁 SkipList，支持并发读写；
//  (2) 如果 Batch 有 DeleteRange 且有配置 Experimental.DeleteRangeFlushDelay(CRDB 默认 10s) 则
// 		  schedule 一个 DelayedFlush 来让 Delete Range 的空间即便一段时间没有写入也能能被释放。
// 	(3) 写入完毕后释放 MemTable 的写引用，减少引用计数并如果为 0, 尝试 schedule 一次 Flush;
//	(4) 最后调用 DB.maybeScheduleFlush 决定是否将 MemTable flush 到磁盘，该方法是异步执行的，
//		  因此临界区耗时较短，flush 的流程我们放到后面剖析 compaction 的章节去讲。
//
// commitApply 完毕后，数据就已经成功写入到 MemTable 中，这时候写入流程还并未结束，数据还不能读取到。
// 我们继续看下一阶段 publish。
//
func (d *DB) commitApply(b *Batch, mem *memTable) error {

		// 如果是 Large batch ，直接 return 。
		if b.flushable != nil {
			// This is a large batch which was already added to the immutable queue.
			return nil
		}

		// 写入 memTable 中的 SkipList ，无锁
		if err := mem.apply(b, b.SeqNum()); err != nil {
			return err
		}

		// If the batch contains range tombstones and the database is configured
		// to flush range deletions, schedule a delayed flush so that disk space
		// may be reclaimed without additional writes or an explicit flush.
		//
		//
		if b.countRangeDels > 0 && d.opts.Experimental.DeleteRangeFlushDelay > 0 {
			d.mu.Lock()
			d.maybeScheduleDelayedFlush(mem)
			d.mu.Unlock()
		}

		// 减引用，若为 0 返回 true
		if mem.writerUnref() {
			d.mu.Lock()
			d.maybeScheduleFlush()
			d.mu.Unlock()
		}

		return nil
}



// 这个方法里面会执行两个核心的操作：
//	1. 准备 batch 的 Memtable；
//	2. 如果开启了 Wal，将数据写入到日志的内存结构中。
//
// 我们来看看日志的格式：
//	|-                  header                        -|-    body     -|
//	+---------+-----------+-----------+----------------+---   ...   ---+
//	|CRC (4B) | Size (2B) | Type (1B) | Log number (4B)|    Payload    |
//	+---------+-----------+-----------+----------------+---   ...   ---+
//
// 日志由 header 和 body 构成。
//
// header 包含：
// 	4 字节 CRC 校验码，
// 	2 字节 Size 表示 body 的大小，
// 	1 字节 Type 表示日志处在 block 中的位置，后面详细解释，
//	4 字节 LogNum 表示日志文件的编号，可用于日志复用，这个后面章节再详解；
//
// body 包含：
//	payload 表示日志的内容，在这里即为 Batch 的字节数组数据。
//
// 日志是按照 32KB 的 Block 来存放的，如下图所示：
//
// 如果一条日志比较小，足以放入到 1 个 Block 中，此时 Type 即为 full，
// 如果一条日志比较大，那么 1 个 Block 无法放入，那么一条日志便会切分成多个片段跨多个 Block 存放，
// 第一个片段的 Type 为 first，中间片段的 Type 为 middle，最后一个片段的 Type 为 last。
// 读取日志时，便可根据 Type 将日志组装还原。
//
// 有了对日志格式的介绍，我们再看 SyncRecord 方法就比较容易了。
func (d *DB) commitWrite(b *Batch, syncWG *sync.WaitGroup, syncErr *error) (*memTable, error) {
	var size int64

	// 获取 batch 的底层字节数组
	repr := b.Repr()

	//
	if b.flushable != nil {
		// We have a large batch. Such batches are special in that they don't get
		// added to the memtable, and are instead inserted into the queue of
		// memtables. The call to makeRoomForWrite with this batch will force the
		// current memtable to be flushed. We want the large batch to be part of
		// the same log, so we add it to the WAL here, rather than after the call
		// to makeRoomForWrite().
		//
		// Set the sequence number since it was not set to the correct value earlier
		// (see comment in newFlushableBatch()).
		b.flushable.setSeqNum(b.SeqNum())
		if !d.opts.DisableWAL {
			var err error
			//
			size, err = d.mu.log.SyncRecord(repr, syncWG, syncErr)
			if err != nil {
				panic(err)
			}
		}
	}

	// 上锁，操作 memtable
	d.mu.Lock()

	// Switch out the memtable if there was not enough room to store the batch.
	//
	// 确保当前 MemTable 是否足以容纳 batch 的数据
	err := d.makeRoomForWrite(b)
	if err == nil && !d.opts.DisableWAL {
		d.mu.log.bytesIn += uint64(len(repr))
	}

	// Grab a reference to the memtable while holding DB.mu. Note that for
	// non-flushable batches (b.flushable == nil) makeRoomForWrite() added a
	// reference to the memtable which will prevent it from being flushed until
	// we unreference it. This reference is dropped in DB.commitApply().
	mem := d.mu.mem.mutable

	d.mu.Unlock()
	if err != nil {
		return nil, err
	}

	// 如果 wal 未开启，直接返回 memtable
	if d.opts.DisableWAL {
		return mem, nil
	}

	// 将数据 repr 写入 wal 日志
	if b.flushable == nil {
		size, err = d.mu.log.SyncRecord(repr, syncWG, syncErr)
		if err != nil {
			panic(err)
		}
	}

	// 更新日志总大小
	atomic.StoreUint64(&d.atomic.logSize, uint64(size))
	return mem, err
}

type iterAlloc struct {
	dbi                 Iterator
	keyBuf              []byte
	prefixOrFullSeekKey []byte
	merging             mergingIter
	mlevels             [3 + numLevels]mergingIterLevel
	levels              [3 + numLevels]levelIter
}

var iterAllocPool = sync.Pool{
	New: func() interface{} {
		return &iterAlloc{}
	},
}

// newIterInternal constructs a new iterator, merging in batch iterators as an extra
// level.
func (d *DB) newIterInternal(batch *Batch, s *Snapshot, o *IterOptions) *Iterator {

	if err := d.closed.Load(); err != nil {
		panic(err)
	}

	if o.rangeKeys() {
		if d.opts.Experimental.RangeKeys == nil {
			panic("pebble: range keys require the Experimental.RangeKeys option")
		}
		if d.FormatMajorVersion() < FormatRangeKeys {
			panic(fmt.Sprintf(
				"pebble: range keys require at least format major version %d (current: %d)",
				FormatRangeKeys, d.FormatMajorVersion(),
			))
		}
	}

	if o != nil && o.RangeKeyMasking.Suffix != nil && o.KeyTypes != IterKeyTypePointsAndRanges {
		panic("pebble: range key masking requires IterKeyTypePointsAndRanges")
	}

	if (batch != nil || s != nil) && (o != nil && o.OnlyReadGuaranteedDurable) {
		// We could add support for OnlyReadGuaranteedDurable on snapshots if
		// there was a need: this would require checking that the sequence number
		// of the snapshot has been flushed, by comparing with
		// DB.mem.queue[0].logSeqNum.
		panic("OnlyReadGuaranteedDurable is not supported for batches or snapshots")
	}

	// Grab and reference the current readState. This prevents the underlying
	// files in the associated version from being deleted if there is a current
	// compaction. The readState is unref'd by Iterator.Close().
	readState := d.loadReadState()

	// Determine the seqnum to read at after grabbing the read state (current and
	// memtables) above.
	var seqNum uint64
	if s != nil {
		seqNum = s.seqNum
	} else {
		seqNum = atomic.LoadUint64(&d.mu.versions.atomic.visibleSeqNum)
	}

	// Bundle various structures under a single umbrella in order to allocate
	// them together.
	buf := iterAllocPool.Get().(*iterAlloc)
	dbi := &buf.dbi
	*dbi = Iterator{
		alloc:               buf,
		cmp:                 d.cmp,
		equal:               d.equal,
		iter:                &buf.merging,
		merge:               d.merge,
		split:               d.split,
		readState:           readState,
		keyBuf:              buf.keyBuf,
		prefixOrFullSeekKey: buf.prefixOrFullSeekKey,
		batch:               batch,
		newIters:            d.newIters,
		seqNum:              seqNum,
	}

	if o.rangeKeys() {
		// TODO(jackson): Pool range-key iterator objects.
		dbi.rangeKey = &iteratorRangeKeyState{
			rangeKeyIter: d.newRangeKeyIter(seqNum, batch, readState, o),
		}
	}

	if o != nil {
		dbi.opts = *o
	}

	dbi.opts.logger = d.opts.Logger
	return finishInitializingIter(buf)
}

// finishInitializingIter is a helper for doing the non-trivial initialization
// of an Iterator.
func finishInitializingIter(buf *iterAlloc) *Iterator {
	// Short-hand.
	dbi := &buf.dbi
	readState := dbi.readState
	batch := dbi.batch
	seqNum := dbi.seqNum
	memtables := readState.memtables
	if dbi.opts.OnlyReadGuaranteedDurable {
		memtables = nil
	}
	current := readState.current

	// Merging levels and levels from iterAlloc.
	mlevels := buf.mlevels[:0]
	levels := buf.levels[:0]

	// We compute the number of levels needed ahead of time and reallocate a slice if
	// the array from the iterAlloc isn't large enough. Doing this allocation once
	// should improve the performance.
	numMergingLevels := 0
	numLevelIters := 0
	if batch != nil {
		numMergingLevels++
	}
	for i := len(memtables) - 1; i >= 0; i-- {
		mem := memtables[i]
		// We only need to read from memtables which contain sequence numbers older
		// than seqNum.
		if logSeqNum := mem.logSeqNum; logSeqNum >= seqNum {
			continue
		}
		numMergingLevels++
	}
	numMergingLevels += len(current.L0SublevelFiles)
	numLevelIters += len(current.L0SublevelFiles)
	for level := 1; level < len(current.Levels); level++ {
		if current.Levels[level].Empty() {
			continue
		}
		numMergingLevels++
		numLevelIters++
	}

	if dbi.opts.pointKeys() {

		if numMergingLevels > cap(mlevels) {
			mlevels = make([]mergingIterLevel, 0, numMergingLevels)
		}

		if numLevelIters > cap(levels) {
			levels = make([]levelIter, 0, numLevelIters)
		}

		// Top-level is the batch, if any.
		if batch != nil {
			mlevels = append(mlevels, mergingIterLevel{
				iter:         batch.newInternalIter(&dbi.opts),
				rangeDelIter: batch.newRangeDelIter(&dbi.opts),
			})
		}

		// Next are the memtables.
		for i := len(memtables) - 1; i >= 0; i-- {
			mem := memtables[i]
			// We only need to read from memtables which contain sequence numbers older
			// than seqNum.
			if logSeqNum := mem.logSeqNum; logSeqNum >= seqNum {
				continue
			}
			mlevels = append(mlevels, mergingIterLevel{
				iter:         mem.newIter(&dbi.opts),
				rangeDelIter: mem.newRangeDelIter(&dbi.opts),
			})
		}

		// Next are the file levels: L0 sub-levels followed by lower levels.
		mlevelsIndex := len(mlevels)
		levelsIndex := len(levels)
		mlevels = mlevels[:numMergingLevels]
		levels = levels[:numLevelIters]

		addLevelIterForFiles := func(files manifest.LevelIterator, level manifest.Level) {
			li := &levels[levelsIndex]

			li.init(
				dbi.opts,
				dbi.cmp,
				dbi.split,
				dbi.newIters,
				files,
				level,
				nil,
			)

			li.initRangeDel(&mlevels[mlevelsIndex].rangeDelIter)

			li.initSmallestLargestUserKey(
				&mlevels[mlevelsIndex].smallestUserKey,
				&mlevels[mlevelsIndex].largestUserKey,
				&mlevels[mlevelsIndex].isLargestUserKeyRangeDelSentinel,
			)

			li.initIsSyntheticIterBoundsKey(
				&mlevels[mlevelsIndex].isSyntheticIterBoundsKey,
			)

			mlevels[mlevelsIndex].iter = li

			levelsIndex++
			mlevelsIndex++
		}

		// Add level iterators for the L0 sublevels, iterating from newest to
		// oldest.
		for i := len(current.L0SublevelFiles) - 1; i >= 0; i-- {
			addLevelIterForFiles(current.L0SublevelFiles[i].Iter(), manifest.L0Sublevel(i))
		}

		// Add level iterators for the non-empty non-L0 levels.
		for level := 1; level < len(current.Levels); level++ {
			if current.Levels[level].Empty() {
				continue
			}
			addLevelIterForFiles(current.Levels[level].Iter(), manifest.Level(level))
		}
		buf.merging.init(&dbi.opts, dbi.cmp, dbi.split, mlevels...)
		buf.merging.snapshot = seqNum
		buf.merging.elideRangeTombstones = true
	} else {
		// This is a merging iterator with no levels, that produces nothing.
		buf.merging.init(&dbi.opts, dbi.cmp, dbi.split)
	}

	// For the in-memory prototype of range keys, wrap the merging iterator with
	// an interleaving iterator. The dbi.rangeKeysIter is an iterator into
	// fragmented range keys read from the global range key arena.
	if dbi.rangeKey != nil {
		dbi.rangeKey.iter.Init(
			dbi.cmp,
			dbi.split,
			&buf.merging,
			dbi.rangeKey.rangeKeyIter,
			dbi.opts.RangeKeyMasking.Suffix,
		)
		dbi.iter = &dbi.rangeKey.iter
		dbi.iter.SetBounds(
			dbi.opts.LowerBound,
			dbi.opts.UpperBound,
		)
	}
	return dbi
}

// NewBatch returns a new empty write-only batch. Any reads on the batch will
// return an error. If the batch is committed it will be applied to the DB.
func (d *DB) NewBatch() *Batch {
	return newBatch(d)
}

// NewIndexedBatch returns a new empty read-write batch. Any reads on the batch
// will read from both the batch and the DB. If the batch is committed it will
// be applied to the DB. An indexed batch is slower that a non-indexed batch
// for insert operations. If you do not need to perform reads on the batch, use
// NewBatch instead.
func (d *DB) NewIndexedBatch() *Batch {
	return newIndexedBatch(d, d.opts.Comparer)
}

// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE, SeekLT,
// First or Last. The iterator provides a point-in-time view of the current DB
// state. This view is maintained by preventing file deletions and preventing
// memtables referenced by the iterator from being deleted. Using an iterator
// to maintain a long-lived point-in-time view of the DB state can lead to an
// apparent memory and disk usage leak. Use snapshots (see NewSnapshot) for
// point-in-time snapshots which avoids these problems.
func (d *DB) NewIter(o *IterOptions) *Iterator {
	return d.newIterInternal(nil /* batch */, nil /* snapshot */, o)
}

// NewSnapshot returns a point-in-time view of the current DB state. Iterators
// created with this handle will all observe a stable snapshot of the current
// DB state. The caller must call Snapshot.Close() when the snapshot is no
// longer needed. Snapshots are not persisted across DB restarts (close ->
// open). Unlike the implicit snapshot maintained by an iterator, a snapshot
// will not prevent memtables from being released or sstables from being
// deleted. Instead, a snapshot prevents deletion of sequence numbers
// referenced by the snapshot.
func (d *DB) NewSnapshot() *Snapshot {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}

	d.mu.Lock()
	s := &Snapshot{
		db:     d,
		seqNum: atomic.LoadUint64(&d.mu.versions.atomic.visibleSeqNum),
	}
	d.mu.snapshots.pushBack(s)
	d.mu.Unlock()
	return s
}

// Close closes the DB.
//
// It is not safe to close a DB until all outstanding iterators are closed
// or to call Close concurrently with any other DB method. It is not valid
// to call any of a DB's methods after the DB has been closed.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.closed.Load(); err != nil {
		panic(err)
	}

	// Clear the finalizer that is used to check that an unreferenced DB has been
	// closed. We're closing the DB here, so the check performed by that
	// finalizer isn't necessary.
	//
	// Note: this is a no-op if invariants are disabled or race is enabled.
	invariants.SetFinalizer(d.closed, nil)

	d.closed.Store(errors.WithStack(ErrClosed))
	close(d.closedCh)

	defer d.opts.Cache.Unref()

	for d.mu.compact.compactingCount > 0 || d.mu.compact.flushing {
		d.mu.compact.cond.Wait()
	}
	for d.mu.tableStats.loading {
		d.mu.tableStats.cond.Wait()
	}
	for d.mu.tableValidation.validating {
		d.mu.tableValidation.cond.Wait()
	}

	var err error
	if n := len(d.mu.compact.inProgress); n > 0 {
		err = errors.Errorf("pebble: %d unexpected in-progress compactions", errors.Safe(n))
	}
	err = firstError(err, d.mu.formatVers.marker.Close())
	err = firstError(err, d.tableCache.close())
	if !d.opts.ReadOnly {
		err = firstError(err, d.mu.log.Close())
	} else if d.mu.log.LogWriter != nil {
		panic("pebble: log-writer should be nil in read-only mode")
	}
	err = firstError(err, d.fileLock.Close())

	// Note that versionSet.close() only closes the MANIFEST. The versions list
	// is still valid for the checks below.
	err = firstError(err, d.mu.versions.close())

	err = firstError(err, d.dataDir.Close())
	if d.dataDir != d.walDir {
		err = firstError(err, d.walDir.Close())
	}

	d.readState.val.unrefLocked()

	current := d.mu.versions.currentVersion()
	for v := d.mu.versions.versions.Front(); true; v = v.Next() {
		refs := v.Refs()
		if v == current {
			if refs != 1 {
				err = firstError(err, errors.Errorf("leaked iterators: current\n%s", v))
			}
			break
		}
		if refs != 0 {
			err = firstError(err, errors.Errorf("leaked iterators:\n%s", v))
		}
	}

	for _, mem := range d.mu.mem.queue {
		mem.readerUnref()
	}
	if reserved := atomic.LoadInt64(&d.atomic.memTableReserved); reserved != 0 {
		err = firstError(err, errors.Errorf("leaked memtable reservation: %d", errors.Safe(reserved)))
	}

	// No more cleaning can start. Wait for any async cleaning to complete.
	for d.mu.cleaner.cleaning {
		d.mu.cleaner.cond.Wait()
	}
	// There may still be obsolete tables if an existing async cleaning job
	// prevented a new cleaning job when a readState was unrefed. If needed,
	// synchronously delete obsolete files.
	if len(d.mu.versions.obsoleteTables) > 0 {
		d.deleteObsoleteFiles(d.mu.nextJobID, true /* waitForOngoing */)
	}
	// Wait for all the deletion goroutines spawned by cleaning jobs to finish.
	d.mu.Unlock()
	d.deleters.Wait()
	d.compactionSchedulers.Wait()
	d.mu.Lock()
	return err
}

// Compact the specified range of keys in the database.
func (d *DB) Compact(
	start, end []byte, /* CompactionOptions */
) error {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	if d.cmp(start, end) >= 0 {
		return errors.Errorf("Compact start %s is not less than end %s",
			d.opts.Comparer.FormatKey(start), d.opts.Comparer.FormatKey(end))
	}
	iStart := base.MakeInternalKey(start, InternalKeySeqNumMax, InternalKeyKindMax)
	iEnd := base.MakeInternalKey(end, 0, 0)
	meta := []*fileMetadata{{Smallest: iStart, Largest: iEnd}}

	d.mu.Lock()
	maxLevelWithFiles := 1
	cur := d.mu.versions.currentVersion()
	for level := 0; level < numLevels; level++ {
		overlaps := cur.Overlaps(level, d.cmp, start, end, iEnd.IsExclusiveSentinel())
		if !overlaps.Empty() {
			maxLevelWithFiles = level + 1
		}
	}

	// Determine if any memtable overlaps with the compaction range. We wait for
	// any such overlap to flush (initiating a flush if necessary).
	mem, err := func() (*flushableEntry, error) {
		// Check to see if any files overlap with any of the memtables. The queue
		// is ordered from oldest to newest with the mutable memtable being the
		// last element in the slice. We want to wait for the newest table that
		// overlaps.
		for i := len(d.mu.mem.queue) - 1; i >= 0; i-- {
			mem := d.mu.mem.queue[i]
			if ingestMemtableOverlaps(d.cmp, mem, meta) {
				var err error
				if mem.flushable == d.mu.mem.mutable {
					// We have to hold both commitPipeline.mu and DB.mu when calling
					// makeRoomForWrite(). Lock order requirements elsewhere force us to
					// unlock DB.mu in order to grab commitPipeline.mu first.
					d.mu.Unlock()
					d.commit.mu.Lock()
					d.mu.Lock()
					defer d.commit.mu.Unlock()
					if mem.flushable == d.mu.mem.mutable {
						// Only flush if the active memtable is unchanged.
						err = d.makeRoomForWrite(nil)
					}
				}
				mem.flushForced = true
				d.maybeScheduleFlush()
				return mem, err
			}
		}
		return nil, nil
	}()

	d.mu.Unlock()

	if err != nil {
		return err
	}
	if mem != nil {
		<-mem.flushed
	}

	for level := 0; level < maxLevelWithFiles; {
		manual := &manualCompaction{
			done:  make(chan error, 1),
			level: level,
			start: iStart,
			end:   iEnd,
		}
		if err := d.manualCompact(manual); err != nil {
			return err
		}
		level = manual.outputLevel
		if level == numLevels-1 {
			// A manual compaction of the bottommost level occurred.
			// There is no next level to try and compact.
			break
		}
	}
	return nil
}

func (d *DB) manualCompact(manual *manualCompaction) error {
	d.mu.Lock()
	d.mu.compact.manual = append(d.mu.compact.manual, manual)
	d.maybeScheduleCompaction()
	d.mu.Unlock()
	return <-manual.done
}

// Flush the memtable to stable storage.
func (d *DB) Flush() error {
	flushDone, err := d.AsyncFlush()
	if err != nil {
		return err
	}
	<-flushDone
	return nil
}

// AsyncFlush asynchronously flushes the memtable to stable storage.
//
// If no error is returned, the caller can receive from the returned channel in
// order to wait for the flush to complete.
func (d *DB) AsyncFlush() (<-chan struct{}, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return nil, ErrReadOnly
	}

	d.commit.mu.Lock()
	defer d.commit.mu.Unlock()

	d.mu.Lock()
	defer d.mu.Unlock()

	flushed := d.mu.mem.queue[len(d.mu.mem.queue)-1].flushed
	if err := d.makeRoomForWrite(nil); err != nil {
		return nil, err
	}

	return flushed, nil
}

// Metrics returns metrics about the database.
func (d *DB) Metrics() *Metrics {
	metrics := &Metrics{}
	recycledLogsCount, recycledLogSize := d.logRecycler.stats()

	d.mu.Lock()
	*metrics = d.mu.versions.metrics
	metrics.Compact.EstimatedDebt = d.mu.versions.picker.estimatedCompactionDebt(0)
	metrics.Compact.InProgressBytes = atomic.LoadInt64(&d.mu.versions.atomic.atomicInProgressBytes)
	metrics.Compact.NumInProgress = int64(d.mu.compact.compactingCount)
	for _, m := range d.mu.mem.queue {
		metrics.MemTable.Size += m.totalBytes()
	}
	metrics.Snapshots.Count = d.mu.snapshots.count()
	if metrics.Snapshots.Count > 0 {
		metrics.Snapshots.EarliestSeqNum = d.mu.snapshots.earliest()
	}
	metrics.MemTable.Count = int64(len(d.mu.mem.queue))
	metrics.MemTable.ZombieCount = atomic.LoadInt64(&d.atomic.memTableCount) - metrics.MemTable.Count
	metrics.MemTable.ZombieSize = uint64(atomic.LoadInt64(&d.atomic.memTableReserved)) - metrics.MemTable.Size
	metrics.WAL.ObsoleteFiles = int64(recycledLogsCount)
	metrics.WAL.ObsoletePhysicalSize = recycledLogSize
	metrics.WAL.Size = atomic.LoadUint64(&d.atomic.logSize)
	// The current WAL size (d.atomic.logSize) is the current logical size,
	// which may be less than the WAL's physical size if it was recycled.
	// The file sizes in d.mu.log.queue are updated to the physical size
	// during WAL rotation. Use the larger of the two for the current WAL. All
	// the previous WALs's fileSizes in d.mu.log.queue are already updated.
	metrics.WAL.PhysicalSize = metrics.WAL.Size
	if len(d.mu.log.queue) > 0 && metrics.WAL.PhysicalSize < d.mu.log.queue[len(d.mu.log.queue)-1].fileSize {
		metrics.WAL.PhysicalSize = d.mu.log.queue[len(d.mu.log.queue)-1].fileSize
	}
	for i, n := 0, len(d.mu.log.queue)-1; i < n; i++ {
		metrics.WAL.PhysicalSize += d.mu.log.queue[i].fileSize
	}

	metrics.WAL.BytesIn = d.mu.log.bytesIn // protected by d.mu
	for i, n := 0, len(d.mu.mem.queue)-1; i < n; i++ {
		metrics.WAL.Size += d.mu.mem.queue[i].logSize
	}
	metrics.WAL.BytesWritten = metrics.Levels[0].BytesIn + metrics.WAL.Size
	if p := d.mu.versions.picker; p != nil {
		compactions := d.getInProgressCompactionInfoLocked(nil)
		for level, score := range p.getScores(compactions) {
			metrics.Levels[level].Score = score
		}
	}
	metrics.Table.ZombieCount = int64(len(d.mu.versions.zombieTables))
	for _, size := range d.mu.versions.zombieTables {
		metrics.Table.ZombieSize += size
	}
	metrics.private.optionsFileSize = d.optionsFileSize

	d.mu.versions.logLock()
	metrics.private.manifestFileSize = uint64(d.mu.versions.manifest.Size())
	d.mu.versions.logUnlock()
	d.mu.Unlock()

	metrics.BlockCache = d.opts.Cache.Metrics()
	metrics.TableCache, metrics.Filter = d.tableCache.metrics()
	metrics.TableIters = int64(d.tableCache.iterCount())
	return metrics
}

// sstablesOptions hold the optional parameters to retrieve TableInfo for all sstables.
type sstablesOptions struct {
	// set to true will return the sstable properties in TableInfo
	withProperties bool
}

// SSTablesOption set optional parameter used by `DB.SSTables`.
type SSTablesOption func(*sstablesOptions)

// WithProperties enable return sstable properties in each TableInfo.
//
// NOTE: if most of the sstable properties need to be read from disk,
// this options may make method `SSTables` quite slow.
func WithProperties() SSTablesOption {
	return func(opt *sstablesOptions) {
		opt.withProperties = true
	}
}

// SSTableInfo export manifest.TableInfo with sstable.Properties
type SSTableInfo struct {
	manifest.TableInfo

	// Properties is the sstable properties of this table.
	Properties *sstable.Properties
}

// SSTables retrieves the current sstables. The returned slice is indexed by
// level and each level is indexed by the position of the sstable within the
// level. Note that this information may be out of date due to concurrent
// flushes and compactions.
func (d *DB) SSTables(opts ...SSTablesOption) ([][]SSTableInfo, error) {
	opt := &sstablesOptions{}
	for _, fn := range opts {
		fn(opt)
	}

	// Grab and reference the current readState.
	readState := d.loadReadState()
	defer readState.unref()

	// TODO(peter): This is somewhat expensive, especially on a large
	// database. It might be worthwhile to unify TableInfo and FileMetadata and
	// then we could simply return current.Files. Note that RocksDB is doing
	// something similar to the current code, so perhaps it isn't too bad.
	srcLevels := readState.current.Levels
	var totalTables int
	for i := range srcLevels {
		totalTables += srcLevels[i].Len()
	}

	destTables := make([]SSTableInfo, totalTables)
	destLevels := make([][]SSTableInfo, len(srcLevels))
	for i := range destLevels {
		iter := srcLevels[i].Iter()
		j := 0
		for m := iter.First(); m != nil; m = iter.Next() {
			destTables[j] = SSTableInfo{TableInfo: m.TableInfo()}
			if opt.withProperties {
				p, err := d.tableCache.getTableProperties(m)
				if err != nil {
					return nil, err
				}
				destTables[j].Properties = p
			}
			j++
		}
		destLevels[i] = destTables[:j]
		destTables = destTables[j:]
	}
	return destLevels, nil
}

// EstimateDiskUsage returns the estimated filesystem space used in bytes for
// storing the range `[start, end]`. The estimation is computed as follows:
//
// - For sstables fully contained in the range the whole file size is included.
// - For sstables partially contained in the range the overlapping data block sizes
//   are included. Even if a data block partially overlaps, or we cannot determine
//   overlap due to abbreviated index keys, the full data block size is included in
//   the estimation. Note that unlike fully contained sstables, none of the
//   meta-block space is counted for partially overlapped files.
// - There may also exist WAL entries for unflushed keys in this range. This
//   estimation currently excludes space used for the range in the WAL.
func (d *DB) EstimateDiskUsage(start, end []byte) (uint64, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.Comparer.Compare(start, end) > 0 {
		return 0, errors.New("invalid key-range specified (start > end)")
	}

	// Grab and reference the current readState. This prevents the underlying
	// files in the associated version from being deleted if there is a concurrent
	// compaction.
	readState := d.loadReadState()
	defer readState.unref()

	var totalSize uint64
	for level, files := range readState.current.Levels {
		iter := files.Iter()
		if level > 0 {
			// We can only use `Overlaps` to restrict `files` at L1+ since at L0 it
			// expands the range iteratively until it has found a set of files that
			// do not overlap any other L0 files outside that set.
			overlaps := readState.current.Overlaps(level, d.opts.Comparer.Compare, start, end, false /* exclusiveEnd */)
			iter = overlaps.Iter()
		}
		for file := iter.First(); file != nil; file = iter.Next() {
			if d.opts.Comparer.Compare(start, file.Smallest.UserKey) <= 0 &&
				d.opts.Comparer.Compare(file.Largest.UserKey, end) <= 0 {
				// The range fully contains the file, so skip looking it up in
				// table cache/looking at its indexes, and add the full file size.
				totalSize += file.Size
			} else if d.opts.Comparer.Compare(file.Smallest.UserKey, end) <= 0 &&
				d.opts.Comparer.Compare(start, file.Largest.UserKey) <= 0 {
				var size uint64
				err := d.tableCache.withReader(file, func(r *sstable.Reader) (err error) {
					size, err = r.EstimateDiskUsage(start, end)
					return err
				})
				if err != nil {
					return 0, err
				}
				totalSize += size
			}
		}
	}
	return totalSize, nil
}

func (d *DB) walPreallocateSize() int {
	// Set the WAL preallocate size to 110% of the memtable size. Note that there
	// is a bit of apples and oranges in units here as the memtabls size
	// corresponds to the memory usage of the memtable while the WAL size is the
	// size of the batches (plus overhead) stored in the WAL.
	//
	// TODO(peter): 110% of the memtable size is quite hefty for a block
	// size. This logic is taken from GetWalPreallocateBlockSize in
	// RocksDB. Could a smaller preallocation block size be used?
	size := d.opts.MemTableSize
	size = (size / 10) + size
	return size
}

func (d *DB) newMemTable(logNum FileNum, logSeqNum uint64) (*memTable, *flushableEntry) {
	size := d.mu.mem.nextSize
	if d.mu.mem.nextSize < d.opts.MemTableSize {
		d.mu.mem.nextSize *= 2
		if d.mu.mem.nextSize > d.opts.MemTableSize {
			d.mu.mem.nextSize = d.opts.MemTableSize
		}
	}

	atomic.AddInt64(&d.atomic.memTableCount, 1)
	atomic.AddInt64(&d.atomic.memTableReserved, int64(size))
	releaseAccountingReservation := d.opts.Cache.Reserve(size)

	mem := newMemTable(memTableOptions{
		Options:   d.opts,
		arenaBuf:  manual.New(int(size)),
		logSeqNum: logSeqNum,
	})

	// Note: this is a no-op if invariants are disabled or race is enabled.
	invariants.SetFinalizer(mem, checkMemTable)

	entry := d.newFlushableEntry(mem, logNum, logSeqNum)
	entry.releaseMemAccounting = func() {
		manual.Free(mem.arenaBuf)
		mem.arenaBuf = nil
		atomic.AddInt64(&d.atomic.memTableCount, -1)
		atomic.AddInt64(&d.atomic.memTableReserved, -int64(size))
		releaseAccountingReservation()
	}
	return mem, entry
}

func (d *DB) newFlushableEntry(f flushable, logNum FileNum, logSeqNum uint64) *flushableEntry {
	return &flushableEntry{
		flushable:  f,
		flushed:    make(chan struct{}),
		logNum:     logNum,
		logSeqNum:  logSeqNum,
		readerRefs: 1,
	}
}

// makeRoomForWrite ensures that the memtable has room to hold the contents of
// Batch. It reserves the space in the memtable and adds a reference to the
// memtable. The caller must later ensure that the memtable is unreferenced. If
// the memtable is full, or a nil Batch is provided, the current memtable is
// rotated (marked as immutable) and a new mutable memtable is allocated. This
// memtable rotation also causes a log rotation.
//
// Both DB.mu and commitPipeline.mu must be held by the caller. Note that DB.mu
// may be released and reacquired.
//
//
// 确保当前 Memtable 是否足以容纳 batch 的数据，如果当前 Memtable 容量已经满了，
// 会将其转变为 Immutable 并重新创建 Memtable。
//
// 由于 makeRoomForWrite 会对 memtable 和 log 进行操作，因此这里会加锁，该方法执行逻辑比较复杂，
//
//
// 主要逻辑:
// 	当前 memtable 空间足够，直接写入
//	当前 memtable 空间不够，将当前 memtable 切换为 immutable memtable，然后将当前 memtable 刷盘
//	batch 为空或者为 large batch 则直接切换当前 memtable（注：这两种视为非常规 batch）
//	如果切换 memtable 则同时会生成新的 log 文件
//
//
// 可以看到，这里有两种情况会强制切换 memtable，一种是 batch 为空，代表手动 flush，另一种是 batch 为 large batch。
// 下面循环中，
//	首先检查当前 memtable 是否正在切换中，如果是则等待当前 memtable 切换完毕；
//
//	随后判断当前 batch 是否为常规 batch，如果为常规 batch ，则调用 memtable 的 prepare 函数判断当前 batch 空间是否足够，
//	如果足够则直接返回，否则返回 ErrArenaFull，代表空间已满；
//
//	下面进入切换 memtable 的逻辑，到这里可以看出，有三种情况会切换 memtable：
//		1. 手动 flush，
//		2. Large batch
//		3. 当前 memtable 已满；
//
//	接下来会计算当前内存所有 memtable 的空间大小，如果总大小超过停写阈值，则会阻塞写，等待 compact 完成；
//
//	再接下来会判断 L0 的读放大是否超过阈值，如果超过则阻塞写，等待 compact 完成；
//
//	如果是 large batch，则会生成 flushableEntry，然后添加到 immutable queue 中；
//
//	最后会将当前 memtable 切换为 immutable 并加入到 queue 中。
//
//	最后将 immutable 解引用，并调用 maybeScheduleFlush 触发写入操作。
//
//
//
//
// makeRoomForWrite 主要逻辑：
//	当前 memtable 空间足够，直接写入
//	当前 memtable 空间不够，将当前 memtable 切换为 immutable memtable ，然后将当前 memtable 刷盘
//	batch 为空或者为 large batch 则直接切换当前 memtable（注：这两种视为非常规 batch ）
//	如果切换 memtable 则同时会生成新的 log 文件
//
func (d *DB) makeRoomForWrite(b *Batch) error {
	force := b == nil || b.flushable != nil
	stalled := false
	for {

		// 检查当前 memTable 是否正在切换中，如果是则等待当前 memtable 切换完毕；
		if d.mu.mem.switching {
			d.mu.mem.cond.Wait()
			continue
		}

		// 判断当前 batch 是否为常规 batch，如果为常规 batch ，
		// 则调用 memtable 的 prepare 函数判断当前 batch 空间是否足够，
		// 如果足够则直接返回，否则返回 ErrArenaFull，代表空间已满；
		if b != nil && b.flushable == nil {
			err := d.mu.mem.mutable.prepare(b)
			if err != arenaskl.ErrArenaFull {
				if stalled {
					d.opts.EventListener.WriteStallEnd()
				}
				return err
			}
		} else if !force {
			if stalled {
				d.opts.EventListener.WriteStallEnd()
			}
			return nil
		}


		// 下面进入切换 memtable 的逻辑，到这里可以看出，有三种情况会切换 memtable ：
		// 1. 手动 flush，
		// 2. Large batch
		// 3. 当前 memtable 已满；


		// force || err == ErrArenaFull, so we need to rotate the current memtable.
		{

			var size uint64

			// 计算当前内存所有 memTable 的空间大小
			for i := range d.mu.mem.queue {
				size += d.mu.mem.queue[i].totalBytes()
			}

			// 总大小超过阈值，需要阻塞写，等待 compact 完成
			if size >= uint64(d.opts.MemTableStopWritesThreshold)*uint64(d.opts.MemTableSize) {
				// We have filled up the current memtable, but already queued memtables
				// are still flushing, so we wait.
				if !stalled {
					stalled = true
					d.opts.EventListener.WriteStallBegin(WriteStallBeginInfo{
						Reason: "memtable count limit reached",
					})
				}
				d.mu.compact.cond.Wait()
				continue
			}
		}

		l0ReadAmp := d.mu.versions.currentVersion().L0Sublevels.ReadAmplification()

		// l0 读放大超过阈值需要阻塞等待
		if l0ReadAmp >= d.opts.L0StopWritesThreshold {
			// There are too many level-0 files, so we wait.
			if !stalled {
				stalled = true
				d.opts.EventListener.WriteStallBegin(WriteStallBeginInfo{
					Reason: "L0 file count limit exceeded",
				})
			}
			d.mu.compact.cond.Wait()
			continue
		}


		var newLogNum FileNum
		var newLogFile vfs.File
		var newLogSize uint64
		var prevLogSize uint64
		var err error

		if !d.opts.DisableWAL {

			jobID := d.mu.nextJobID
			d.mu.nextJobID++
			newLogNum = d.mu.versions.getNextFileNum()
			d.mu.mem.switching = true

			// 当前日志文件大小
			prevLogSize = uint64(d.mu.log.Size())

			// The previous log may have grown past its original physical
			// size. Update its file size in the queue so we have a proper
			// accounting of its file size.
			//
			if d.mu.log.queue[len(d.mu.log.queue)-1].fileSize < prevLogSize {
				d.mu.log.queue[len(d.mu.log.queue)-1].fileSize = prevLogSize
			}
			d.mu.Unlock()

			// Close the previous log first. This writes an EOF trailer
			// signifying the end of the file and syncs it to disk. We must
			// close the previous log before linking the new log file,
			// otherwise a crash could leave both logs with unclean tails, and
			// Open will treat the previous log as corrupt.
			//
			// 关闭当前日志，这会写入一个 EOF 标记然后 flush 到磁盘。
			// 我们必须关闭上一个 log ，然后链接到新 log ，否则 crash 会导致
			err = d.mu.log.Close()


			// 创建新日志文件
			newLogName := base.MakeFilepath(d.opts.FS, d.walDirname, fileTypeLog, newLogNum)

			// Try to use a recycled log file. Recycling log files is an important
			// performance optimization as it is faster to sync a file that has
			// already been written, than one which is being written for the first
			// time. This is due to the need to sync file metadata when a file is
			// being written for the first time. Note this is true even if file
			// preallocation is performed (e.g. fallocate).
			//
			// 尝试重用日志文件。
			var recycleLog fileInfo
			var recycleOK bool
			if err == nil {
				recycleLog, recycleOK = d.logRecycler.peek()
				if recycleOK {
					recycleLogName := base.MakeFilepath(d.opts.FS, d.walDirname, fileTypeLog, recycleLog.fileNum)
					newLogFile, err = d.opts.FS.ReuseForWrite(recycleLogName, newLogName)
					base.MustExist(d.opts.FS, newLogName, d.opts.Logger, err)
				} else {
					newLogFile, err = d.opts.FS.Create(newLogName)
					base.MustExist(d.opts.FS, newLogName, d.opts.Logger, err)
				}
			}

			if err == nil && recycleOK {
				// Figure out the recycled WAL size. This Stat is necessary
				// because ReuseForWrite's contract allows for removing the
				// old file and creating a new one. We don't know whether the
				// WAL was actually recycled.
				// TODO(jackson): Adding a boolean to the ReuseForWrite return
				// value indicating whether or not the file was actually
				// reused would allow us to skip the stat and use
				// recycleLog.fileSize.
				var finfo os.FileInfo
				finfo, err = newLogFile.Stat()
				if err == nil {
					newLogSize = uint64(finfo.Size())
				}
			}

			if err == nil {
				// TODO(peter): RocksDB delays sync of the parent directory until the
				// first time the log is synced. Is that worthwhile?
				err = d.walDir.Sync()
			}

			if err != nil && newLogFile != nil {
				newLogFile.Close()
			} else if err == nil {
				newLogFile = vfs.NewSyncingFile(newLogFile, vfs.SyncingFileOptions{
					BytesPerSync:    d.opts.WALBytesPerSync,
					PreallocateSize: d.walPreallocateSize(),
				})
			}

			if recycleOK {
				err = firstError(err, d.logRecycler.pop(recycleLog.fileNum))
			}

			// 事件回调
			d.opts.EventListener.WALCreated(WALCreateInfo{
				JobID:           jobID,
				Path:            newLogName,
				FileNum:         newLogNum,
				RecycledFileNum: recycleLog.fileNum,
				Err:             err,
			})

			d.mu.Lock()
			d.mu.mem.switching = false
			d.mu.mem.cond.Broadcast()

			d.mu.versions.metrics.WAL.Files++
		}

		if err != nil {
			// TODO(peter): avoid chewing through file numbers in a tight loop if there
			// is an error here.
			//
			// What to do here? Stumbling on doesn't seem worthwhile. If we failed to
			// close the previous log it is possible we lost a write.
			panic(err)
		}

		if !d.opts.DisableWAL {
			// 保存 log info 到 queue
			d.mu.log.queue = append(d.mu.log.queue, fileInfo{fileNum: newLogNum, fileSize: newLogSize})
			// 构造 log writer ，用于写入 wal 日志
			d.mu.log.LogWriter = record.NewLogWriter(newLogFile, newLogNum)
			d.mu.log.LogWriter.SetMinSyncInterval(d.opts.WALMinSyncInterval)
		}


		//
		immMem := d.mu.mem.mutable
		imm := d.mu.mem.queue[len(d.mu.mem.queue)-1]
		imm.logSize = prevLogSize
		imm.flushForced = imm.flushForced || (b == nil)

		// If we are manually flushing and we used less than half of the bytes in
		// the memtable, don't increase the size for the next memtable. This
		// reduces memtable memory pressure when an application is frequently
		// manually flushing.
		if (b == nil) && uint64(immMem.availBytes()) > immMem.totalBytes()/2 {
			d.mu.mem.nextSize = int(immMem.totalBytes())
		}

		if b != nil && b.flushable != nil {
			// The batch is too large to fit in the memtable so add it directly to
			// the immutable queue. The flushable batch is associated with the same
			// log as the immutable memtable, but logically occurs after it in
			// seqnum space. So give the flushable batch the logNum and clear it from
			// the immutable log. This is done as a defensive measure to prevent the
			// WAL containing the large batch from being deleted prematurely if the
			// corresponding memtable is flushed without flushing the large batch.
			//
			// See DB.commitWrite for the special handling of log writes for large
			// batches. In particular, the large batch has already written to
			// imm.logNum.
			entry := d.newFlushableEntry(b.flushable, imm.logNum, b.SeqNum())
			// The large batch is by definition large. Reserve space from the cache
			// for it until it is flushed.
			entry.releaseMemAccounting = d.opts.Cache.Reserve(int(b.flushable.totalBytes()))
			d.mu.mem.queue = append(d.mu.mem.queue, entry)
			imm.logNum = 0
		}

		var logSeqNum uint64
		if b != nil {
			logSeqNum = b.SeqNum()
			if b.flushable != nil {
				logSeqNum += uint64(b.Count())
			}
		} else {
			logSeqNum = atomic.LoadUint64(&d.mu.versions.atomic.logSeqNum)
		}

		// Create a new memtable, scheduling the previous one for flushing. We do
		// this even if the previous memtable was empty because the DB.Flush
		// mechanism is dependent on being able to wait for the empty memtable to
		// flush. We can't just mark the empty memtable as flushed here because we
		// also have to wait for all previous immutable tables to
		// flush. Additionally, the memtable is tied to particular WAL file and we
		// want to go through the flush path in order to recycle that WAL file.
		//
		// NB: newLogNum corresponds to the WAL that contains mutations that are
		// present in the new memtable. When immutable memtables are flushed to
		// disk, a VersionEdit will be created telling the manifest the minimum
		// unflushed log number (which will be the next one in d.mu.mem.mutable
		// that was not flushed).
		var entry *flushableEntry
		d.mu.mem.mutable, entry = d.newMemTable(newLogNum, logSeqNum)
		d.mu.mem.queue = append(d.mu.mem.queue, entry)
		d.updateReadStateLocked(nil)
		if immMem.writerUnref() {
			d.maybeScheduleFlush()
		}
		force = false
	}
}

func (d *DB) getEarliestUnflushedSeqNumLocked() uint64 {
	seqNum := InternalKeySeqNumMax
	for i := range d.mu.mem.queue {
		logSeqNum := d.mu.mem.queue[i].logSeqNum
		if seqNum > logSeqNum {
			seqNum = logSeqNum
		}
	}
	return seqNum
}

func (d *DB) getInProgressCompactionInfoLocked(finishing *compaction) (rv []compactionInfo) {
	for c := range d.mu.compact.inProgress {
		if len(c.flushing) == 0 && (finishing == nil || c != finishing) {
			info := compactionInfo{
				inputs:      c.inputs,
				smallest:    c.smallest,
				largest:     c.largest,
				outputLevel: -1,
			}
			if c.outputLevel != nil {
				info.outputLevel = c.outputLevel.level
			}
			rv = append(rv, info)
		}
	}
	return
}

func inProgressL0Compactions(inProgress []compactionInfo) []manifest.L0Compaction {
	var compactions []manifest.L0Compaction
	for _, info := range inProgress {
		l0 := false
		for _, cl := range info.inputs {
			l0 = l0 || cl.level == 0
		}
		if !l0 {
			continue
		}
		compactions = append(compactions, manifest.L0Compaction{
			Smallest:  info.smallest,
			Largest:   info.largest,
			IsIntraL0: info.outputLevel == 0,
		})
	}
	return compactions
}

// firstError returns the first non-nil error of err0 and err1, or nil if both
// are nil.
func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}
