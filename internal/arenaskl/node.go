/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 * Modifications copyright (C) 2017 Andy Kimball and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package arenaskl

import (
	"math"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/base"
)

// MaxNodeSize returns the maximum space needed for a node with the specified
// key and value sizes.
func MaxNodeSize(keySize, valueSize uint32) uint32 {
	return uint32(maxNodeSize) + keySize + valueSize + align4
}

// 链接信息
type links struct {
	nextOffset uint32
	prevOffset uint32
}

func (l *links) init(prevOffset, nextOffset uint32) {
	l.nextOffset = nextOffset
	l.prevOffset = prevOffset
}

type node struct {
	// Immutable fields, so no need to lock to access key.
	// 不可变字段，所以不需要对其加锁访问
	keyOffset uint32
	keySize   uint32
	valueSize uint32
	allocSize uint32

	// Most nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated in the arena, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	//
	// All accesses to elements should use CAS operations, with no need to lock.
	//
	// 每个节点都有 maxHeight 高度个链表，用于指向每层的下一个或者上一个节点，用于加速查找
	// 大部分节点不需要塔的全部高度，因为每个连续层的概率指数下降。
	// 因为这些元素永远不会被访问，不需要被分配。
	// 因此，当在区域中分配节点时，其内存占用被截断，不包括不需要的塔元素。
	// 所有元素的访问都应该使用 CAS 操作，而不需要锁定。
	tower [maxHeight]links
}

func newNode(
	arena *Arena, height uint32, key base.InternalKey, value []byte,
) (nd *node, err error) {
	if height < 1 || height > maxHeight {
		panic("height cannot be less than one or greater than the max height")
	}
	keySize := key.Size()
	if int64(keySize) > math.MaxUint32 {
		panic("key is too large")
	}
	valueSize := len(value)
	if int64(len(value)) > math.MaxUint32 {
		panic("value is too large")
	}

	nd, err = newRawNode(arena, height, uint32(keySize), uint32(valueSize))
	if err != nil {
		return
	}

	key.Encode(nd.getKeyBytes(arena))
	copy(nd.getValue(arena), value)
	return
}

func newRawNode(arena *Arena, height uint32, keySize, valueSize uint32) (nd *node, err error) {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := uint32((maxHeight - int(height)) * linksSize)
	nodeSize := uint32(maxNodeSize) - unusedSize

	nodeOffset, allocSize, err := arena.alloc(nodeSize+keySize+valueSize, align4, unusedSize)
	if err != nil {
		return
	}

	nd = (*node)(arena.getPointer(nodeOffset))
	nd.keyOffset = nodeOffset + nodeSize
	nd.keySize = keySize
	nd.valueSize = valueSize
	nd.allocSize = allocSize
	return
}

func (n *node) getKeyBytes(arena *Arena) []byte {
	return arena.getBytes(n.keyOffset, n.keySize)
}

func (n *node) getValue(arena *Arena) []byte {
	return arena.getBytes(n.keyOffset+n.keySize, uint32(n.valueSize))
}

func (n *node) nextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h].nextOffset)
}

func (n *node) prevOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h].prevOffset)
}

func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h].nextOffset, old, val)
}

func (n *node) casPrevOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h].prevOffset, old, val)
}
