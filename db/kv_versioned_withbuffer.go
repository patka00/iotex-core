// Copyright (c) 2023 IoTeX Foundation
// This is an alpha (internal) release and is not suitable for production. This source code is provided 'as is' and no
// warranties are given as to title or non-infringement, merchantability or fitness for purpose and, to the extent
// permitted by law, all liability for your use of the code is disclaimed. This source code is governed by Apache
// License 2.0 that can be found in the LICENSE file.

package db

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/iotexproject/iotex-core/db/batch"
)

// KvVersionedWithBuffer is KvVersioned with a buffer
type KvVersionedWithBuffer struct {
	kvStore KvVersioned
	buffer  batch.CachedBatch
	height  uint64
}

// NewKvVersionedWithBuffer returns a new instance of versioned kvStore with buffer
func NewKvVersionedWithBuffer(kv KvVersioned, buffer batch.CachedBatch, h uint64) (*KvVersionedWithBuffer, error) {
	if kv == nil {
		return nil, errors.New("kvStore cannot be nil")
	}
	if buffer == nil {
		return nil, errors.New("buffer cannot be nil")
	}
	return &KvVersionedWithBuffer{
		kvStore: kv.SetVersion(h),
		buffer:  buffer,
		height:  h,
	}, nil
}

// Start starts
func (kv *KvVersionedWithBuffer) Start(ctx context.Context) error {
	return kv.kvStore.Start(ctx)
}

// Stop stops
func (kv *KvVersionedWithBuffer) Stop(ctx context.Context) error {
	return kv.kvStore.Stop(ctx)
}

// Snapshot takes a snapshot
func (kv *KvVersionedWithBuffer) Snapshot() int {
	return kv.buffer.Snapshot()
}

// Revert reverts to an earlier snapshot
func (kv *KvVersionedWithBuffer) Revert(sid int) error {
	return kv.buffer.RevertSnapshot(sid)
}

// SerializeQueue serializes the buffer
func (kv *KvVersionedWithBuffer) SerializeQueue(
	serialize batch.WriteInfoSerialize,
	filter batch.WriteInfoFilter,
) []byte {
	return kv.buffer.SerializeQueue(serialize, filter)
}

// MustPut puts
func (kv *KvVersionedWithBuffer) MustPut(ns string, key, value []byte) {
	kv.buffer.Put(ns, key, value, fmt.Sprintf("faild to put %x in %s", key, ns))
}

// MustDelete deletes
func (kv *KvVersionedWithBuffer) MustDelete(ns string, key []byte) {
	kv.buffer.Delete(ns, key, fmt.Sprintf("failed to delete %x in %s", key, ns))
}

// Size returns the buffer size
func (kv *KvVersionedWithBuffer) Size() int {
	return kv.buffer.Size()
}

// Put puts
func (kv *KvVersionedWithBuffer) Put(ns string, key, value []byte) error {
	kv.buffer.Put(ns, key, value, fmt.Sprintf("faild to put %x in %s", key, ns))
	return nil
}

// Get gets
func (kv *KvVersionedWithBuffer) Get(ns string, key []byte) ([]byte, error) {
	value, err := kv.buffer.Get(ns, key)
	if errors.Cause(err) == batch.ErrAlreadyDeleted {
		return nil, errors.Wrapf(ErrNotExist, "failed to get key %x in %s, deleted in buffer level", key, ns)
	}
	if errors.Cause(err) == batch.ErrNotExist {
		return kv.kvStore.SetVersion(kv.height).Get(ns, key)
	}
	return value, err
}

// Delete deletes
func (kv *KvVersionedWithBuffer) Delete(ns string, key []byte) error {
	kv.buffer.Delete(ns, key, fmt.Sprintf("failed to delete %x in %s", key, ns))
	return nil
}

// WriteBatch writes a batch
func (kv *KvVersionedWithBuffer) WriteBatch(kvsb batch.KVStoreBatch) (err error) {
	return kv.kvStore.WriteBatch(kvsb)
}

// Filter filters
func (kv *KvVersionedWithBuffer) Filter(ns string, cond Condition, minKey, maxKey []byte) ([][]byte, [][]byte, error) {
	return kv.kvStore.Filter(ns, cond, minKey, maxKey)
}

// Commit writes and batch and then clears it
func (kv *KvVersionedWithBuffer) Commit() error {
	err := kv.kvStore.WriteBatch(kv.buffer)
	kv.buffer.Lock()
	kv.buffer.ClearAndUnlock()
	return err
}

// CreateVersionedNamespace creates a namespace
func (kv *KvVersionedWithBuffer) CreateVersionedNamespace(ns string, n uint32) error {
	return kv.kvStore.CreateVersionedNamespace(ns, n)
}
