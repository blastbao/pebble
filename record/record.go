// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package record reads and writes sequences of records. Each record is a stream
// of bytes that completes before the next record starts.
//
// When reading, call Next to obtain an io.Reader for the next record. Next will
// return io.EOF when there are no more records. It is valid to call Next
// without reading the current record to exhaustion.
//
// When writing, call Next to obtain an io.Writer for the next record. Calling
// Next finishes the current record. Call Close to finish the final record.
//
// Optionally, call Flush to finish the current record and flush the underlying
// writer without starting a new record. To start a new record after flushing,
// call Next.
//
// Neither Readers or Writers are safe to use concurrently.
//
// Example code:
//	func read(r io.Reader) ([]string, error) {
//		var ss []string
//    // 构造 reader
//		records := record.NewReader(r)
//		for {
//			// 不断读取
//			rec, err := records.Next()
//			if err == io.EOF {
//				break
//			}
//			if err != nil {
//				log.Printf("recovering from %v", err)
//				r.Recover()
//				continue
//			}
//			s, err := ioutil.ReadAll(rec)
//			if err != nil {
//				log.Printf("recovering from %v", err)
//				r.Recover()
//				continue
//			}
//			ss = append(ss, string(s))
//		}
//		return ss, nil
//	}
//
//	func write(w io.Writer, ss []string) error {
//		records := record.NewWriter(w)
//		for _, s := range ss {
//			rec, err := records.Next()
//			if err != nil {
//				return err
//			}
//			if _, err := rec.Write([]byte(s)), err != nil {
//				return err
//			}
//		}
//		return records.Close()
//	}
//
// The wire format is that the stream is divided into 32KiB blocks, and each
// block contains a number of tightly packed chunks. Chunks cannot cross block
// boundaries. The last block may be shorter than 32 KiB. Any unused bytes in a
// block must be zero.
//
// A record maps to one or more chunks. There are two chunk formats: legacy and
// recyclable. The legacy chunk format:
//
//   +----------+-----------+-----------+--- ... ---+
//   | CRC (4B) | Size (2B) | Type (1B) | Payload   |
//   +----------+-----------+-----------+--- ... ---+
//
// CRC is computed over the type and payload
// Size is the length of the payload in bytes
// Type is the chunk type
//
// There are four chunk types: whether the chunk is the full record, or the
// first, middle or last chunk of a multi-chunk record. A multi-chunk record
// has one first chunk, zero or more middle chunks, and one last chunk.
//
// The recyclyable chunk format is similar to the legacy format, but extends
// the chunk header with an additional log number field. This allows reuse
// (recycling) of log files which can provide significantly better performance
// when syncing frequently as it avoids needing to update the file
// metadata. Additionally, recycling log files is a prequisite for using direct
// IO with log writing. The recyclyable format is:
//
//   +----------+-----------+-----------+----------------+--- ... ---+
//   | CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
//   +----------+-----------+-----------+----------------+--- ... ---+
//
// Recyclable chunks are distinguished from legacy chunks by the addition of 4
// extra "recyclable" chunk types that map directly to the legacy chunk types
// (i.e. full, first, middle, last). The CRC is computed over the type, log
// number, and payload.
//
// The wire format allows for limited recovery in the face of data corruption:
// on a format error (such as a checksum mismatch), the reader moves to the
// next block and looks for the next full or first chunk.
//
// 有两种块格式：legacy 和 recyclable 。
//
// legacy :
//   +----------+-----------+-----------+--- ... ---+
//   | CRC (4B) | Size (2B) | Type (1B) | Payload   |
//   +----------+-----------+-----------+--- ... ---+
// 其中 Type 有四种:
//		- fullChunkType
//		- firstChunkType
//		- middleChunkType
//		- lastChunkType
//
// recyclyable:
//	 同 legacy 格式类似，只是增加了 Log number 字段。
//   +----------+-----------+-----------+----------------+--- ... ---+
//   | CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
//   +----------+-----------+-----------+----------------+--- ... ---+
//
//   这允许重复使用（回收）日志文件，这可以在频繁同步时提供明显更好的性能，因为它避免了需要更新文件元数据。
//   此外，回收日志文件是使用直接 IO 写入日志的前提。
//
//
package record

// The C++ Level-DB code calls this the log, but it has been renamed to record
// to avoid clashing with the standard log package, and because it is generally
// useful outside of logging. The C++ code also uses the term "physical record"
// instead of "chunk", but "chunk" is shorter and less confusing.

import (
	"encoding/binary"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/crc"
)

// These constants are part of the wire format and should not be changed.
const (
	fullChunkType   = 1
	firstChunkType  = 2
	middleChunkType = 3
	lastChunkType   = 4

	recyclableFullChunkType   = 5
	recyclableFirstChunkType  = 6
	recyclableMiddleChunkType = 7
	recyclableLastChunkType   = 8
)

const (
	blockSize            = 32 * 1024            // 块大小 32KB
	blockSizeMask        = blockSize - 1        // 块偏移掩码，通过 offset & blockSizeMask、offset &^ blockSizeMask 计算块、记录的偏移
	legacyHeaderSize     = 7                    // legacy 类型块的 header 长度
	recyclableHeaderSize = legacyHeaderSize + 4 // recyclable 类型块的 header 长度
)

var (
	// ErrNotAnIOSeeker is returned if the io.Reader underlying a Reader does not implement io.Seeker.
	ErrNotAnIOSeeker = errors.New("pebble/record: reader does not implement io.Seeker")

	// ErrNoLastRecord is returned if LastRecordOffset is called and there is no previous record.
	ErrNoLastRecord = errors.New("pebble/record: no last record exists")

	// ErrZeroedChunk is returned if a chunk is encountered that is zeroed. This
	// usually occurs due to log file preallocation.
	ErrZeroedChunk = base.CorruptionErrorf("pebble/record: zeroed chunk")

	// ErrInvalidChunk is returned if a chunk is encountered with an invalid
	// header, length, or checksum. This usually occurs when a log is recycled,
	// but can also occur due to corruption.
	ErrInvalidChunk = base.CorruptionErrorf("pebble/record: invalid chunk")
)

// IsInvalidRecord returns true if the error matches one of the error types
// returned for invalid records. These are treated in a way similar to io.EOF
// in recovery code.
func IsInvalidRecord(err error) bool {
	return err == ErrZeroedChunk || err == ErrInvalidChunk || err == io.ErrUnexpectedEOF
}

// Reader reads records from an underlying io.Reader.
type Reader struct {
	// r is the underlying reader.
	r io.Reader

	// logNum is the low 32-bits of the log's file number. May be zero when used
	// with log files that do not have a file number (e.g. the MANIFEST).
	//
	// 日志的文件号的低 32 位。
	// 当用于没有文件号的日志文件时（例如 MANIFEST ），可以是 0 。
	logNum uint32

	// blockNum is the zero based block number currently held in buf.
	//
	// blockNum 是当前保存在 buf 中的块编号(从 0 开始)。
	blockNum int64

	// seq is the sequence number of the current record.
	//
	// 当前记录序号。
	seq int

	// buf[begin:end] is the unread portion of the current chunk's payload.
	// The low bound, begin, excludes the chunk header.
	//
	// buf[begin:end] 是当前块的有效载荷的未读部分。
	begin, end int

	// n is the number of bytes of buf that are valid. Once reading has started,
	// only the final block can have n < blockSize.
	//
	// n 是 buf 的有效字节数。一旦开始读取，只有最后一个块可以有 n<blockSize 。
	n int

	// recovering is true when recovering from corruption.
	//
	// 正在从崩溃中恢复。
	recovering bool

	// last is whether the current chunk is the last chunk of the record.
	//
	// 当前块是否是该记录的最后一块
	last bool

	// err is any accumulated error.
	//
	// 累计错误
	err error

	// buf is the buffer.
	//
	// 缓存
	buf [blockSize]byte
}

// NewReader returns a new reader. If the file contains records encoded using
// the recyclable record format, then the log number in those records must
// match the specified logNum.
func NewReader(r io.Reader, logNum base.FileNum) *Reader {
	return &Reader{
		r:        r,
		logNum:   uint32(logNum),
		blockNum: -1,
	}
}

// nextChunk sets r.buf[r.i:r.j] to hold the next chunk's payload, reading the
// next block into the buffer if necessary.
//
// nextChunk 让 r.buf[r.i:r.j] 存储下一个块的有效载荷，如果有必要，将下一个块读入缓冲区。
func (r *Reader) nextChunk(wantFirst bool) error {

	for {

		// 当前块为 r.buf[r.begin, r.end] ，需要更新 r.begin/r.end 来移动到下个块。

		// 如果当前块后有足够一个 legacy header 的数据，就解析这个 Header 。
		if r.end+legacyHeaderSize <= r.n {

			// +----------+-----------+-----------+--- ... ---+
			// | CRC (4B) | Size (2B) | Type (1B) | Payload   |
			// +----------+-----------+-----------+--- ... ---+

			checksum := binary.LittleEndian.Uint32(r.buf[r.end+0 : r.end+4])
			length := binary.LittleEndian.Uint16(r.buf[r.end+4 : r.end+6])
			chunkType := r.buf[r.end+6]

			// 空白块
			if checksum == 0 && length == 0 && chunkType == 0 {

				// 如果不足一个 recyclable header 的大小，则跳过该块的其余部分。
				if r.end+recyclableHeaderSize > r.n {
					// Skip the rest of the block if the recyclable header size does not
					// fit within it.
					r.end = r.n
					continue
				}

				// 正在恢复中
				if r.recovering {
					// Skip the rest of the block, if it looks like it is all zeroes.
					// This is common with WAL preallocation.
					//
					// Set r.err to be an error so r.recover actually recovers.
					r.err = ErrZeroedChunk
					r.recover()
					continue
				}

				return ErrZeroedChunk
			}

			// 默认是 legacy 块
			headerSize := legacyHeaderSize

			// 如果是 recyclable 块
			if chunkType >= recyclableFullChunkType && chunkType <= recyclableLastChunkType {

				headerSize = recyclableHeaderSize

				// 数据不足，块非法
				if r.end+headerSize > r.n {
					return ErrInvalidChunk
				}

				// 解析 log num
				logNum := binary.LittleEndian.Uint32(r.buf[r.end+7 : r.end+11])
				if logNum != r.logNum {
					if wantFirst {
						// If we're looking for the first chunk of a record, we can treat a
						// previous instance of the log as EOF.
						return io.EOF
					}
					// Otherwise, treat this chunk as invalid in order to prevent reading
					// of a partial record.
					return ErrInvalidChunk
				}

				// 将 recyclable 转换为对应的 legacy 块类型
				chunkType -= (recyclableFullChunkType - 1)
			}


			// begin 指向下一个 chunk 的有效载荷部分（忽略头部）
			r.begin = r.end + headerSize
			// end 指向下一个的结尾
			r.end = r.begin + int(length)

			// 如果缓存数据不足，报错
			if r.end > r.n {
				if r.recovering {
					r.recover()
					continue
				}
				return ErrInvalidChunk
			}

			// 校验 crc
			if checksum != crc.New(r.buf[r.begin-headerSize+6:r.end]).Value() {
				if r.recovering {
					r.recover()
					continue
				}
				return ErrInvalidChunk
			}

			//
			if wantFirst {
				if chunkType != fullChunkType && chunkType != firstChunkType {
					continue
				}
			}


			r.last = chunkType == fullChunkType || chunkType == lastChunkType
			r.recovering = false
			return nil
		}

		//
		if r.n < blockSize && r.blockNum >= 0 {
			if !wantFirst || r.end != r.n {
				// This can happen if the previous instance of the log ended with a
				// partial block at the same blockNum as the new log but extended
				// beyond the partial block of the new log.
				return ErrInvalidChunk
			}
			return io.EOF
		}

		// 从 r.r 读取数据到 r.buf 中，返回字节数
		n, err := io.ReadFull(r.r, r.buf[:])
		if err != nil && err != io.ErrUnexpectedEOF {
			if err == io.EOF && !wantFirst {
				return io.ErrUnexpectedEOF
			}
			return err
		}

		// 重置 begin/end/n 变量
		r.begin, r.end, r.n = 0, 0, n
		// 增加块计数
		r.blockNum++
	}
}

// Next returns a reader for the next record. It returns io.EOF if there are no
// more records. The reader returned becomes stale after the next Next call,
// and should no longer be used.
//
//
func (r *Reader) Next() (io.Reader, error) {
	// 序号 +1
	r.seq++

	// 错误检查
	if r.err != nil {
		return nil, r.err
	}

	// 移动到下个 chunk
	r.begin = r.end
	r.err = r.nextChunk(true)
	if r.err != nil {
		return nil, r.err
	}

	// 封装 reader
	return singleReader{r, r.seq}, nil
}

// Offset returns the current offset within the file. If called immediately
// before a call to Next(), Offset() will return the record offset.
func (r *Reader) Offset() int64 {
	if r.blockNum < 0 {
		return 0
	}
	return int64(r.blockNum)*blockSize + int64(r.end)
}

// recover clears any errors read so far, so that calling Next will start
// reading from the next good 32KiB block. If there are no such blocks, Next
// will return io.EOF. recover also marks the current reader, the one most
// recently returned by Next, as stale. If recover is called without any
// prior error, then recover is a no-op.
func (r *Reader) recover() {
	if r.err == nil {
		return
	}
	r.recovering = true
	r.err = nil
	// Discard the rest of the current block.
	r.begin, r.end, r.last = r.n, r.n, false
	// Invalidate any outstanding singleReader.
	r.seq++
}

// seekRecord seeks in the underlying io.Reader such that calling r.Next
// returns the record whose first chunk header starts at the provided offset.
// Its behavior is undefined if the argument given is not such an offset, as
// the bytes at that offset may coincidentally appear to be a valid header.
//
// It returns ErrNotAnIOSeeker if the underlying io.Reader does not implement
// io.Seeker.
//
// seekRecord will fail and return an error if the Reader previously
// encountered an error, including io.EOF. Such errors can be cleared by
// calling Recover. Calling seekRecord after Recover will make calling Next
// return the record at the given offset, instead of the record at the next
// good 32KiB block as Recover normally would. Calling seekRecord before
// Recover has no effect on Recover's semantics other than changing the
// starting point for determining the next good 32KiB block.
//
// The offset is always relative to the start of the underlying io.Reader, so
// negative values will result in an error as per io.Seeker.
func (r *Reader) seekRecord(offset int64) error {
	r.seq++
	if r.err != nil {
		return r.err
	}

	s, ok := r.r.(io.Seeker)
	if !ok {
		return ErrNotAnIOSeeker
	}

	// Only seek to an exact block offset.
	// 块内记录偏移
	recordOffset := int(offset & blockSizeMask)
	// 块偏移
	blockOffset := offset &^ blockSizeMask

	// 先 seek 到块偏移，然后根据块内记录偏移即可定位到 record
	if _, r.err = s.Seek(blockOffset, io.SeekStart); r.err != nil {
		return r.err
	}

	// Clear the state of the internal reader.
	r.begin, r.end, r.n = 0, 0, 0
	r.blockNum, r.recovering, r.last = -1, false, false
	if r.err = r.nextChunk(false); r.err != nil {
		return r.err
	}

	// Now skip to the offset requested within the block. A subsequent
	// call to Next will return the block at the requested offset.
	r.begin, r.end = recordOffset, recordOffset

	return nil
}

type singleReader struct {
	r   *Reader
	seq int
}

func (x singleReader) Read(p []byte) (int, error) {
	r := x.r
	if r.seq != x.seq {
		return 0, errors.New("pebble/record: stale reader")
	}
	if r.err != nil {
		return 0, r.err
	}
	for r.begin == r.end {
		if r.last {
			return 0, io.EOF
		}
		if r.err = r.nextChunk(false); r.err != nil {
			return 0, r.err
		}
	}
	n := copy(p, r.buf[r.begin:r.end])
	r.begin += n
	return n, nil
}

// Writer writes records to an underlying io.Writer.
type Writer struct {
	// w is the underlying writer.
	w io.Writer
	// seq is the sequence number of the current record.
	seq int
	// f is w as a flusher.
	f flusher
	// buf[i:j] is the bytes that will become the current chunk.
	// The low bound, i, includes the chunk header.
	i, j int
	// buf[:written] has already been written to w.
	// written is zero unless Flush has been called.
	written int
	// baseOffset is the base offset in w at which writing started. If
	// w implements io.Seeker, it's relative to the start of w, 0 otherwise.
	baseOffset int64
	// blockNumber is the zero based block number currently held in buf.
	blockNumber int64
	// lastRecordOffset is the offset in w where the last record was
	// written (including the chunk header). It is a relative offset to
	// baseOffset, thus the absolute offset of the last record is
	// baseOffset + lastRecordOffset.
	lastRecordOffset int64
	// first is whether the current chunk is the first chunk of the record.
	first bool
	// pending is whether a chunk is buffered but not yet written.
	pending bool
	// err is any accumulated error.
	err error
	// buf is the buffer.
	buf [blockSize]byte
}

// NewWriter returns a new Writer.
func NewWriter(w io.Writer) *Writer {
	f, _ := w.(flusher)

	var o int64
	if s, ok := w.(io.Seeker); ok {
		var err error
		if o, err = s.Seek(0, io.SeekCurrent); err != nil {
			o = 0
		}
	}
	return &Writer{
		w:                w,
		f:                f,
		baseOffset:       o,
		lastRecordOffset: -1,
	}
}

// fillHeader fills in the header for the pending chunk.
func (w *Writer) fillHeader(last bool) {
	if w.i+legacyHeaderSize > w.j || w.j > blockSize {
		panic("pebble/record: bad writer state")
	}
	if last {
		if w.first {
			w.buf[w.i+6] = fullChunkType
		} else {
			w.buf[w.i+6] = lastChunkType
		}
	} else {
		if w.first {
			w.buf[w.i+6] = firstChunkType
		} else {
			w.buf[w.i+6] = middleChunkType
		}
	}
	binary.LittleEndian.PutUint32(w.buf[w.i+0:w.i+4], crc.New(w.buf[w.i+6:w.j]).Value())
	binary.LittleEndian.PutUint16(w.buf[w.i+4:w.i+6], uint16(w.j-w.i-legacyHeaderSize))
}

// writeBlock writes the buffered block to the underlying writer, and reserves
// space for the next chunk's header.
func (w *Writer) writeBlock() {
	_, w.err = w.w.Write(w.buf[w.written:])
	w.i = 0
	w.j = legacyHeaderSize
	w.written = 0
	w.blockNumber++
}

// writePending finishes the current record and writes the buffer to the
// underlying writer.
func (w *Writer) writePending() {
	if w.err != nil {
		return
	}
	if w.pending {
		w.fillHeader(true)
		w.pending = false
	}
	_, w.err = w.w.Write(w.buf[w.written:w.j])
	w.written = w.j
}

// Close finishes the current record and closes the writer.
func (w *Writer) Close() error {
	w.seq++
	w.writePending()
	if w.err != nil {
		return w.err
	}
	w.err = errors.New("pebble/record: closed Writer")
	return nil
}

// Flush finishes the current record, writes to the underlying writer, and
// flushes it if that writer implements interface{ Flush() error }.
func (w *Writer) Flush() error {
	w.seq++
	w.writePending()
	if w.err != nil {
		return w.err
	}
	if w.f != nil {
		w.err = w.f.Flush()
		return w.err
	}
	return nil
}

// Next returns a writer for the next record. The writer returned becomes stale
// after the next Close, Flush or Next call, and should no longer be used.
func (w *Writer) Next() (io.Writer, error) {
	w.seq++
	if w.err != nil {
		return nil, w.err
	}
	if w.pending {
		w.fillHeader(true)
	}
	w.i = w.j
	w.j = w.j + legacyHeaderSize
	// Check if there is room in the block for the header.
	if w.j > blockSize {
		// Fill in the rest of the block with zeroes.
		for k := w.i; k < blockSize; k++ {
			w.buf[k] = 0
		}
		w.writeBlock()
		if w.err != nil {
			return nil, w.err
		}
	}
	w.lastRecordOffset = w.baseOffset + w.blockNumber*blockSize + int64(w.i)
	w.first = true
	w.pending = true
	return singleWriter{w, w.seq}, nil
}

// WriteRecord writes a complete record. Returns the offset just past the end
// of the record.
func (w *Writer) WriteRecord(p []byte) (int64, error) {
	if w.err != nil {
		return -1, w.err
	}
	t, err := w.Next()
	if err != nil {
		return -1, err
	}
	if _, err := t.Write(p); err != nil {
		return -1, err
	}
	w.writePending()
	offset := w.blockNumber*blockSize + int64(w.j)
	return offset, w.err
}

// Size returns the current size of the file.
func (w *Writer) Size() int64 {
	if w == nil {
		return 0
	}
	return w.blockNumber*blockSize + int64(w.j)
}

// LastRecordOffset returns the offset in the underlying io.Writer of the last
// record so far - the one created by the most recent Next call. It is the
// offset of the first chunk header, suitable to pass to Reader.SeekRecord.
//
// If that io.Writer also implements io.Seeker, the return value is an absolute
// offset, in the sense of io.SeekStart, regardless of whether the io.Writer
// was initially at the zero position when passed to NewWriter. Otherwise, the
// return value is a relative offset, being the number of bytes written between
// the NewWriter call and any records written prior to the last record.
//
// If there is no last record, i.e. nothing was written, LastRecordOffset will
// return ErrNoLastRecord.
func (w *Writer) LastRecordOffset() (int64, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.lastRecordOffset < 0 {
		return 0, ErrNoLastRecord
	}
	return w.lastRecordOffset, nil
}

type singleWriter struct {
	w   *Writer
	seq int
}

func (x singleWriter) Write(p []byte) (int, error) {
	w := x.w
	if w.seq != x.seq {
		return 0, errors.New("pebble/record: stale writer")
	}
	if w.err != nil {
		return 0, w.err
	}
	n0 := len(p)
	for len(p) > 0 {
		// Write a block, if it is full.
		if w.j == blockSize {
			w.fillHeader(false)
			w.writeBlock()
			if w.err != nil {
				return 0, w.err
			}
			w.first = false
		}
		// Copy bytes into the buffer.
		n := copy(w.buf[w.j:], p)
		w.j += n
		p = p[n:]
	}
	return n0, nil
}
