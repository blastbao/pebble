// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"errors"
	"unsafe"

	"github.com/petermattis/pebble/db"
)

func uvarintLen(v uint32) int {
	i := 0
	for v >= 0x80 {
		v >>= 7
		i++
	}
	return i + 1
}

type blockWriter struct {
	restartInterval int
	nEntries        int
	buf             []byte
	restarts        []uint32
	curKey          []byte
	curValue        []byte
	prevKey         []byte
	tmp             [50]byte
}

func (w *blockWriter) store(keySize int, value []byte) {
	shared := 0
	if w.nEntries%w.restartInterval == 0 {
		w.restarts = append(w.restarts, uint32(len(w.buf)))
	} else {
		shared = db.SharedPrefixLen(w.curKey, w.prevKey)
	}

	n := binary.PutUvarint(w.tmp[0:], uint64(shared))
	n += binary.PutUvarint(w.tmp[n:], uint64(keySize-shared))
	n += binary.PutUvarint(w.tmp[n:], uint64(len(value)))
	w.buf = append(w.buf, w.tmp[:n]...)
	w.buf = append(w.buf, w.curKey[shared:]...)
	w.buf = append(w.buf, value...)
	w.curValue = w.buf[len(w.buf)-len(value):]

	w.nEntries++
}

func (w *blockWriter) add(key db.InternalKey, value []byte) {
	w.curKey, w.prevKey = w.prevKey, w.curKey

	size := key.Size()
	if cap(w.curKey) < size {
		w.curKey = make([]byte, 0, size*2)
	}
	w.curKey = w.curKey[:size]
	key.Encode(w.curKey)

	w.store(size, value)
}

func (w *blockWriter) finish() []byte {
	// Write the restart points to the buffer.
	if w.nEntries == 0 {
		// Every block must have at least one restart point.
		if cap(w.restarts) > 0 {
			w.restarts = w.restarts[:1]
			w.restarts[0] = 0
		} else {
			w.restarts = append(w.restarts, 0)
		}
	}
	tmp4 := w.tmp[:4]
	for _, x := range w.restarts {
		binary.LittleEndian.PutUint32(tmp4, x)
		w.buf = append(w.buf, tmp4...)
	}
	binary.LittleEndian.PutUint32(tmp4, uint32(len(w.restarts)))
	w.buf = append(w.buf, tmp4...)
	return w.buf
}

func (w *blockWriter) reset() {
	w.nEntries = 0
	w.buf = w.buf[:0]
	w.restarts = w.restarts[:0]
}

func (w *blockWriter) estimatedSize() int {
	return len(w.buf) + 4*(len(w.restarts)+1)
}

type blockEntry struct {
	offset int
	key    []byte
	val    []byte
}

// blockIter is an iterator over a single block of data.
type blockIter struct {
	cmp          db.Compare
	offset       int
	nextOffset   int
	restarts     int
	numRestarts  int
	globalSeqNum uint64
	ptr          unsafe.Pointer
	data         []byte
	key, val     []byte
	ikey         db.InternalKey
	cached       []blockEntry
	cachedBuf    []byte
	err          error
}

func newBlockIter(cmp db.Compare, block block) (*blockIter, error) {
	i := &blockIter{}
	return i, i.init(cmp, block, 0)
}

func (i *blockIter) init(cmp db.Compare, block block, globalSeqNum uint64) error {
	numRestarts := int(binary.LittleEndian.Uint32(block[len(block)-4:]))
	if numRestarts == 0 {
		return errors.New("pebble/table: invalid table (block has no restart points)")
	}
	i.cmp = cmp
	i.restarts = len(block) - 4*(1+numRestarts)
	i.numRestarts = numRestarts
	i.globalSeqNum = globalSeqNum
	i.ptr = unsafe.Pointer(&block[0])
	i.data = block
	if i.key == nil {
		i.key = make([]byte, 0, 256)
	} else {
		i.key = i.key[:0]
	}
	i.val = nil
	i.clearCache()
	return nil
}

func (i *blockIter) readEntry() {
	ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(i.offset))

	// This is an ugly performance hack. Reading entries from blocks is one of
	// the inner-most routines and decoding the 3 varints per-entry takes a
	// significant time. Neither go1.11 or go1.12 will inline decodeVarint for
	// us, so we do it manually. This provides a 10-15% performance improvement
	// on blockIter benchmarks on both go1.11 and go1.12.
	//
	// TODO(peter): remove this hack if go:inline is ever supported.

	var shared uint32
	src := (*[5]uint8)(ptr)
	if a := (*src)[0]; a < 128 {
		shared = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, (*src)[1]; b < 128 {
		shared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, (*src)[2]; c < 128 {
		shared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, (*src)[3]; d < 128 {
		shared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, (*src)[4]
		shared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	var unshared uint32
	src = (*[5]uint8)(ptr)
	if a := (*src)[0]; a < 128 {
		unshared = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, (*src)[1]; b < 128 {
		unshared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, (*src)[2]; c < 128 {
		unshared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, (*src)[3]; d < 128 {
		unshared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, (*src)[4]
		unshared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	var value uint32
	src = (*[5]uint8)(ptr)
	if a := (*src)[0]; a < 128 {
		value = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, (*src)[1]; b < 128 {
		value = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, (*src)[2]; c < 128 {
		value = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, (*src)[3]; d < 128 {
		value = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, (*src)[4]
		value = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	i.key = append(i.key[:shared], getBytes(ptr, int(unshared))...)
	ptr = unsafe.Pointer(uintptr(ptr) + uintptr(unshared))
	i.val = getBytes(ptr, int(value))
	i.nextOffset = int(uintptr(ptr)-uintptr(i.ptr)) + int(value)
}

func (i *blockIter) decodeInternalKey(key []byte) {
	// Manually inlining db.DecodeInternalKey provides a 5-10% speedup on
	// BlockIter benchmarks.
	if n := len(key) - 8; n >= 0 {
		i.ikey.Trailer = binary.LittleEndian.Uint64(key[n:])
		i.ikey.UserKey = key[:n:n]
		if i.globalSeqNum != 0 {
			i.ikey.SetSeqNum(i.globalSeqNum)
		}
	} else {
		i.ikey.Trailer = uint64(db.InternalKeyKindInvalid)
		i.ikey.UserKey = nil
	}
}

func (i *blockIter) loadEntry() {
	i.readEntry()
	i.decodeInternalKey(i.key)
}

func (i *blockIter) clearCache() {
	i.cached = i.cached[:0]
	i.cachedBuf = i.cachedBuf[:0]
}

func (i *blockIter) cacheEntry() {
	i.cachedBuf = append(i.cachedBuf, i.key...)
	i.cached = append(i.cached, blockEntry{
		offset: i.offset,
		key:    i.cachedBuf[len(i.cachedBuf)-len(i.key) : len(i.cachedBuf) : len(i.cachedBuf)],
		val:    i.val,
	})
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package.
func (i *blockIter) SeekGE(key []byte) bool {
	ikey := db.MakeSearchKey(key)

	// Find the index of the smallest restart point whose key is > the key
	// sought; index will be numRestarts if there is no such restart point.
	i.offset = 0
	var index int

	{
		// NB: manually inlined sort.Seach is ~5% faster.
		//
		// Define f(-1) == false and f(n) == true.
		// Invariant: f(index-1) == false, f(upper) == true.
		upper := i.numRestarts
		for index < upper {
			h := int(uint(index+upper) >> 1) // avoid overflow when computing h
			// index ≤ h < upper
			offset := int(binary.LittleEndian.Uint32(i.data[i.restarts+4*h:]))
			// For a restart point, there are 0 bytes shared with the previous key.
			// The varint encoding of 0 occupies 1 byte.
			ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(offset+1))

			// Decode the key at that restart point, and compare it to the key
			// sought. See the comment in readEntry for why we manually inline the
			// varint decoding.
			var v1 uint32
			src := (*[5]uint8)(ptr)
			if a := (*src)[0]; a < 128 {
				v1 = uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if a, b := a&0x7f, (*src)[1]; b < 128 {
				v1 = uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if b, c := b&0x7f, (*src)[2]; c < 128 {
				v1 = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if c, d := c&0x7f, (*src)[3]; d < 128 {
				v1 = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				d, e := d&0x7f, (*src)[4]
				v1 = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			if src := (*[5]uint8)(ptr); (*src)[0] < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if (*src)[1] < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if (*src)[2] < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if (*src)[3] < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			// Manually inlining db.DecodeInternalKey provides a 5-10% speedup on
			// BlockIter benchmarks.
			s := getBytes(ptr, int(v1))
			var k db.InternalKey
			if n := len(s) - 8; n >= 0 {
				k.Trailer = binary.LittleEndian.Uint64(s[n:])
				k.UserKey = s[:n:n]
				// NB: We can't have duplicate keys if the globalSeqNum != 0, so we
				// leave the seqnum on this key as 0 as it won't affect our search
				// since ikey has the maximum seqnum.
			} else {
				k.Trailer = uint64(db.InternalKeyKindInvalid)
			}

			if db.InternalCompare(i.cmp, ikey, k) >= 0 {
				index = h + 1 // preserves f(i-1) == false
			} else {
				upper = h // preserves f(j) == true
			}
		}
		// index == upper, f(index-1) == false, and f(upper) (= f(index)) == true
		// => answer is index.
	}

	// Since keys are strictly increasing, if index > 0 then the restart point at
	// index-1 will be the largest whose key is <= the key sought.  If index ==
	// 0, then all keys in this block are larger than the key sought, and offset
	// remains at zero.
	if index > 0 {
		i.offset = int(binary.LittleEndian.Uint32(i.data[i.restarts+4*(index-1):]))
	}
	i.loadEntry()

	// Iterate from that restart point to somewhere >= the key sought.
	for valid := i.Valid(); valid; valid = i.Next() {
		if db.InternalCompare(i.cmp, i.ikey, ikey) >= 0 {
			return true
		}
	}

	return false
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package.
func (i *blockIter) SeekLT(key []byte) bool {
	ikey := db.MakeSearchKey(key)

	// Find the index of the smallest restart point whose key is >= the key
	// sought; index will be numRestarts if there is no such restart point.
	i.offset = 0
	var index int

	{
		// NB: manually inlined sort.Search is ~5% faster.
		//
		// Define f(-1) == false and f(n) == true.
		// Invariant: f(index-1) == false, f(upper) == true.
		upper := i.numRestarts
		for index < upper {
			h := int(uint(index+upper) >> 1) // avoid overflow when computing h
			// index ≤ h < upper
			offset := int(binary.LittleEndian.Uint32(i.data[i.restarts+4*h:]))
			// For a restart point, there are 0 bytes shared with the previous key.
			// The varint encoding of 0 occupies 1 byte.
			ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(offset+1))

			// Decode the key at that restart point, and compare it to the key
			// sought. See the comment in readEntry for why we manually inline the
			// varint decoding.
			var v1 uint32
			src := (*[5]uint8)(ptr)
			if a := (*src)[0]; a < 128 {
				v1 = uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if a, b := a&0x7f, (*src)[1]; b < 128 {
				v1 = uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if b, c := b&0x7f, (*src)[2]; c < 128 {
				v1 = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if c, d := c&0x7f, (*src)[3]; d < 128 {
				v1 = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				d, e := d&0x7f, (*src)[4]
				v1 = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			if src := (*[5]uint8)(ptr); (*src)[0] < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if (*src)[1] < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if (*src)[2] < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if (*src)[3] < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			// Manually inlining db.DecodeInternalKey provides a 5-10% speedup on
			// BlockIter benchmarks.
			s := getBytes(ptr, int(v1))
			var k db.InternalKey
			if n := len(s) - 8; n >= 0 {
				k.Trailer = binary.LittleEndian.Uint64(s[n:])
				k.UserKey = s[:n:n]
				// NB: We can't have duplicate keys if the globalSeqNum != 0, so we
				// leave the seqnum on this key as 0 as it won't affect our search
				// since ikey has the maximum seqnum.
			} else {
				k.Trailer = uint64(db.InternalKeyKindInvalid)
			}

			if db.InternalCompare(i.cmp, ikey, k) > 0 {
				index = h + 1 // preserves f(i-1) == false
			} else {
				upper = h // preserves f(j) == true
			}
		}
		// index == upper, f(index-1) == false, and f(upper) (= f(index)) == true
		// => answer is index.
	}

	// Since keys are strictly increasing, if index > 0 then the restart point at
	// index-1 will be the largest whose key is < the key sought.
	if index > 0 {
		i.offset = int(binary.LittleEndian.Uint32(i.data[i.restarts+4*(index-1):]))
	} else if index == 0 {
		// If index == 0 then all keys in this block are larger than the key
		// sought.
		i.offset = -1
		i.nextOffset = 0
		return false
	}

	// Iterate from that restart point to somewhere >= the key sought, then back
	// up to the previous entry. The expectation is that we'll be performing
	// reverse iteration, so we cache the entries as we advance forward.
	i.clearCache()
	i.nextOffset = i.offset

	for {
		i.offset = i.nextOffset
		i.loadEntry()
		i.cacheEntry()

		if i.cmp(i.ikey.UserKey, ikey.UserKey) >= 0 {
			// The current key is greater than or equal to our search key. Back up to
			// the previous key which was less than our search key.
			i.Prev()
			return true
		}

		if i.nextOffset >= i.restarts {
			// We've reached the end of the block. Return the current key.
			break
		}
	}

	return i.Valid()
}

// First implements internalIterator.First, as documented in the pebble
// package.
func (i *blockIter) First() bool {
	i.offset = 0
	if !i.Valid() {
		return false
	}
	i.loadEntry()
	return true
}

// Last implements internalIterator.Last, as documented in the pebble package.
func (i *blockIter) Last() bool {
	// Seek forward from the last restart point.
	i.offset = int(binary.LittleEndian.Uint32(i.data[i.restarts+4*(i.numRestarts-1):]))
	if !i.Valid() {
		return false
	}

	i.readEntry()
	i.clearCache()
	i.cacheEntry()

	for i.nextOffset < i.restarts {
		i.offset = i.nextOffset
		i.readEntry()
		i.cacheEntry()
	}

	i.decodeInternalKey(i.key)
	return true
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
func (i *blockIter) Next() bool {
	i.offset = i.nextOffset
	if !i.Valid() {
		return false
	}
	i.loadEntry()
	return true
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *blockIter) Prev() bool {
	if n := len(i.cached) - 1; n > 0 && i.cached[n].offset == i.offset {
		i.nextOffset = i.offset
		e := &i.cached[n-1]
		i.offset = e.offset
		i.val = e.val
		i.decodeInternalKey(e.key)
		i.cached = i.cached[:n]
		return true
	}

	if i.offset == 0 {
		i.offset = -1
		i.nextOffset = 0
		return false
	}

	targetOffset := i.offset
	var index int

	{
		// NB: manually inlined sort.Sort is ~5% faster.
		//
		// Define f(-1) == false and f(n) == true.
		// Invariant: f(index-1) == false, f(upper) == true.
		upper := i.numRestarts
		for index < upper {
			h := int(uint(index+upper) >> 1) // avoid overflow when computing h
			// index ≤ h < upper
			offset := int(binary.LittleEndian.Uint32(i.data[i.restarts+4*h:]))
			if offset < targetOffset {
				index = h + 1 // preserves f(i-1) == false
			} else {
				upper = h // preserves f(j) == true
			}
		}
		// index == upper, f(index-1) == false, and f(upper) (= f(index)) == true
		// => answer is index.
	}

	i.offset = 0
	if index > 0 {
		i.offset = int(binary.LittleEndian.Uint32(i.data[i.restarts+4*(index-1):]))
	}

	i.readEntry()
	i.clearCache()
	i.cacheEntry()

	for i.nextOffset < targetOffset {
		i.offset = i.nextOffset
		i.readEntry()
		i.cacheEntry()
	}

	i.decodeInternalKey(i.key)
	return true
}

// Key implements internalIterator.Key, as documented in the pebble package.
func (i *blockIter) Key() db.InternalKey {
	return i.ikey
}

// Value implements internalIterator.Value, as documented in the pebble
// package.
func (i *blockIter) Value() []byte {
	return i.val
}

// Valid implements internalIterator.Valid, as documented in the pebble
// package.
func (i *blockIter) Valid() bool {
	return i.offset >= 0 && i.offset < i.restarts
}

// Error implements internalIterator.Error, as documented in the pebble
// package.
func (i *blockIter) Error() error {
	return i.err
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *blockIter) Close() error {
	i.val = nil
	return i.err
}
