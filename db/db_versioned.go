// Copyright (c) 2023 IoTeX Foundation
// This is an alpha (internal) release and is not suitable for production. This source code is provided 'as is' and no
// warranties are given as to title or non-infringement, merchantability or fitness for purpose and, to the extent
// permitted by law, all liability for your use of the code is disclaimed. This source code is governed by Apache
// License 2.0 that can be found in the LICENSE file.

package db

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/iotexproject/go-pkgs/byteutil"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
	bolt "go.etcd.io/bbolt"

	"github.com/iotexproject/iotex-core/db/batch"
)

// vars
var (
	ErrDBCorruption     = errors.New("DB is corrupted")
	ErrInvalidKeyLength = errors.New("invalid key length")
	ErrInvalidWrite     = errors.New("invalid write attempt")
	_minKey             = []byte{0} // the minimum key, used to store namespace's metadata
)

type (
	// KvVersioned is a versioned key-value store, where each key can (but doesn't
	// have to) have multiple versions of value (corresponding to different heights
	// in a blockchain)
	//
	// A namespace/bucket is considered versioned by default, user needs to use the
	// NonversionedNamespaceOption() to specify non-versioned namespaces at the time
	// of creating the versioned key-value store
	//
	// Versioning is achieved by using (key + 8-byte version) as the actual storage
	// key in the underlying DB. For buckets containing versioned keys, a metadata
	// is stored at the special key = []byte{0}. The metadata specifies the bucket's
	// name and the key length
	//
	// For each versioned key, the special location = key + []byte{0} stores the
	// key's version (as 8-byte big endian). If the location does not store a value,
	// the key has never been written. A zero value means the key has been deleted
	//
	// Here's an example of a versioned DB which has 3 buckets:
	// 1. "mta" -- regular bucket storing metadata, key is not versioned
	// 2. "act" -- versioned namespace, key length = 20
	// 3. "stk" -- versioned namespace, key length = 8
	KvVersioned interface {
		KVStore

		// Version returns the key's most recent version
		Version(string, []byte) (uint64, error)

		// SetVersion sets the version before calling Put()
		SetVersion(uint64) KvVersioned

		// CreateVersionedNamespace creates a namespace to store versioned keys
		CreateVersionedNamespace(string, uint32) error
	}

	// Hasher is a function to compute hash
	Hasher func([]byte) []byte

	// BoltDBVersioned is KvVersioned implementation based on bolt DB
	BoltDBVersioned struct {
		*BoltDB
		hasher       Hasher
		version      uint64          // version for Get/Put()
		versioned    map[string]int  // buckets for versioned keys
		nonversioned map[string]bool // buckets for non-versioned keys
	}
)

// Option sets an option
type Option func(b *BoltDBVersioned)

// NonversionedNamespaceOption sets non-versioned namespace
func NonversionedNamespaceOption(ns ...string) Option {
	return func(b *BoltDBVersioned) {
		for _, v := range ns {
			b.nonversioned[v] = true
		}
	}
}

// VersionedNamespaceOption sets versioned namespace
func VersionedNamespaceOption(ns string, n int) Option {
	return func(b *BoltDBVersioned) {
		b.versioned[ns] = n
	}
}

// NewBoltDBVersioned instantiates an BoltDB with implements KvVersioned
func NewBoltDBVersioned(cfg Config, h Hasher, opts ...Option) *BoltDBVersioned {
	b := &BoltDBVersioned{
		BoltDB:       NewBoltDB(cfg),
		hasher:       h,
		versioned:    make(map[string]int),
		nonversioned: make(map[string]bool),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Start starts the DB
func (b *BoltDBVersioned) Start(ctx context.Context) error {
	if err := b.BoltDB.Start(ctx); err != nil {
		return err
	}
	// verify non-versioned namespace
	for ns := range b.nonversioned {
		vn, err := b.checkVersionedNS(ns)
		if err != nil {
			return err
		}
		if vn != nil {
			return errors.Wrapf(ErrDBCorruption, "expect namespace %s to be non-versioned, but got versioned", ns)
		}
	}
	// verify initial versioned namespace
	buf := batch.NewBatch()
	for ns, n := range b.versioned {
		vn, err := b.checkVersionedNS(ns)
		if err != nil {
			return err
		}
		if vn == nil {
			// create the versioned namespace
			buf.Put(ns, _minKey, (&versionedNamespace{
				name:   ns,
				keyLen: uint32(n),
			}).Serialize(), "failed to create metadata")
		}
	}
	if buf.Size() > 0 {
		return b.BoltDB.WriteBatch(buf)
	}
	return nil
}

// Put writes a <key, value> record
func (b *BoltDBVersioned) Put(ns string, key, value []byte) error {
	versioned, keyLen, err := b.isVersioned(ns, key)
	if err != nil && err != ErrNotExist {
		return err
	}
	if !versioned {
		return b.BoltDB.Put(ns, key, value)
	}
	buf := batch.NewBatch()
	if err == ErrNotExist {
		// namespace not yet created
		buf.Put(ns, _minKey, (&versionedNamespace{
			name:   ns,
			keyLen: uint32(len(key)),
		}).Serialize(), "failed to create metadata")
		keyLen = len(key)
	}
	if len(key) != keyLen {
		return errors.Wrapf(ErrInvalidKeyLength, "expecting %d, got %d", keyLen, len(key))
	}
	// check key's metadata
	km, err := b.checkVersionedKey(ns, key)
	if err != nil {
		return err
	}
	km, exit := updateKeyMeta(km, b.version, b.hasher(value))
	if exit {
		return nil
	}
	buf.Put(ns, append(key, 0), km.Serialize(), fmt.Sprintf("failed to put key %x's meta", key))
	buf.Put(ns, versionedKey(key, b.version), value, fmt.Sprintf("failed to put key %x", key))
	return b.BoltDB.WriteBatch(buf)
}

// Get retrieves the most recent version
func (b *BoltDBVersioned) Get(ns string, key []byte) ([]byte, error) {
	versioned, keyLen, err := b.isVersioned(ns, key)
	if err != nil {
		return nil, err
	}
	if !versioned {
		return b.BoltDB.Get(ns, key)
	}
	if len(key) != keyLen {
		return nil, errors.Wrapf(ErrInvalidKeyLength, "expecting %d, got %d", keyLen, len(key))
	}
	return b.getVersion(ns, key)
}

// Version returns the key's most recent version
func (b *BoltDBVersioned) Version(ns string, key []byte) (uint64, error) {
	versioned, keyLen, err := b.isVersioned(ns, key)
	if err != nil {
		return 0, err
	}
	if !versioned {
		_, err := b.BoltDB.Get(ns, key)
		return 0, err
	}
	if len(key) != keyLen {
		return 0, errors.Wrapf(ErrInvalidKeyLength, "expecting %d, got %d", keyLen, len(key))
	}
	km, err := b.checkVersionedKey(ns, key)
	if err != nil {
		return 0, err
	}
	if km == nil {
		// key not yet written
		return 0, ErrNotExist
	}
	return km.lastVersion, nil
}

// SetVersion sets the version which a Get()/Put() wants to access
func (b *BoltDBVersioned) SetVersion(v uint64) KvVersioned {
	b.version = v
	return b
}

// CreateVersionedNamespace creates a namespace to store versioned keys
func (b *BoltDBVersioned) CreateVersionedNamespace(ns string, n uint32) error {
	if _, ok := b.nonversioned[ns]; ok {
		return errors.Wrapf(ErrInvalidWrite, "namespace %s is non-versioned", ns)
	}
	vn, err := b.checkVersionedNS(ns)
	if err != nil {
		return err
	}
	if vn != nil {
		return errors.Wrapf(ErrInvalidWrite, "namespace %s already exist", ns)
	}
	if err = b.BoltDB.Put(ns, _minKey, (&versionedNamespace{ns, n}).Serialize()); err != nil {
		return err
	}
	b.versioned[ns] = int(n)
	return nil
}

// WriteBatch commits a batch
func (b *BoltDBVersioned) WriteBatch(kvsb batch.KVStoreBatch) (err error) {
	if b.db == nil {
		return ErrDBNotStarted
	}

	kvsb.Lock()
	defer kvsb.Unlock()
	var (
		newVNS  = make(map[string]int)
		buckets = make(map[string]*bbolt.Bucket)
		cause   error
		meta    []byte
		vns     *versionedNamespace
		km      *keyMeta
		exit    bool
	)
	for c := uint8(0); c < b.config.NumRetries; c++ {
		err = b.db.Update(func(tx *bolt.Tx) error {
			for i := 0; i < kvsb.Size(); i++ {
				write, e := kvsb.Entry(i)
				if e != nil {
					return e
				}
				ns := write.Namespace()
				key := write.Key()
				switch write.WriteType() {
				case batch.Put:
					bucket, ok := buckets[ns]
					if !ok {
						bucket, e = tx.CreateBucketIfNotExists([]byte(ns))
						if e != nil {
							return errors.Wrapf(e, write.Error())
						}
						buckets[ns] = bucket
					}
					val := write.Value()
					if b.nonversioned[ns] {
						if e := bucket.Put(key, val); e != nil {
							return errors.Wrapf(e, write.Error())
						}
					} else if bytes.Compare(_minKey, key) == 0 {
						if b.versioned[ns] > 0 || newVNS[ns] > 0 {
							return errors.Wrapf(ErrInvalidWrite, write.Error())
						}
						if vns, e = DeserializeVersionedNamespace(val); e != nil {
							return errors.Wrap(e, write.Error())
						}
						newVNS[ns] = int(vns.keyLen)
						if e := bucket.Put(key, val); e != nil {
							return errors.Wrapf(e, write.Error())
						}
					} else {
						// check namespace's key length
						n := b.versioned[ns]
						if n == 0 {
							n = newVNS[ns]
						}
						if n == 0 {
							if meta = bucket.Get(_minKey); meta == nil {
								return errors.Wrapf(ErrDBCorruption, write.Error())
							}
							if vns, e = DeserializeVersionedNamespace(meta); e != nil {
								return errors.Wrap(e, write.Error())
							}
							n = int(vns.keyLen)
							newVNS[ns] = int(vns.keyLen)
						}
						if n == 0 {
							return errors.Wrap(ErrDBCorruption, write.Error())
						}
						if len(key) != n {
							return errors.Wrapf(ErrInvalidKeyLength, write.Error())
						}
						// check key's metadata
						hash := b.hasher(val)
						if meta = bucket.Get(append(key, 0)); meta == nil {
							// key not yet written
							km = &keyMeta{
								lastWriteHash: hash,
								firstVersion:  b.version,
								lastVersion:   b.version,
								deleteVersion: math.MaxUint64,
							}
						} else {
							if km, e = DeserializeKeyMeta(meta); e != nil {
								return errors.Wrap(e, write.Error())
							}
							if km, exit = updateKeyMeta(km, b.version, hash); exit {
								continue
							}
						}
						if e = bucket.Put(append(key, 0), km.Serialize()); e != nil {
							return errors.Wrapf(e, write.Error())
						}
						if e := bucket.Put(versionedKey(key, b.version), val); e != nil {
							return errors.Wrapf(e, write.Error())
						}
					}
				case batch.Delete:
					bucket, ok := buckets[ns]
					if !ok {
						bucket = tx.Bucket([]byte(ns))
						if bucket == nil {
							continue
						}
						buckets[ns] = bucket
					}
					if e := bucket.Delete(key); e != nil {
						return errors.Wrapf(e, write.Error())
					}
				}
			}
			return nil
		})
		cause = errors.Cause(err)
		if err == nil || cause == ErrDBCorruption || cause == ErrInvalidWrite || cause == ErrInvalidKeyLength {
			break
		} else {
			newVNS = make(map[string]int)
			buckets = make(map[string]*bbolt.Bucket)
		}
	}
	if err == nil {
		for k, v := range newVNS {
			b.versioned[k] = v
		}
		return
	}
	if cause != ErrDBCorruption && cause != ErrInvalidWrite && cause != ErrInvalidKeyLength {
		err = errors.Wrap(ErrIO, err.Error())
	}
	return
}

func (b *BoltDBVersioned) getAllKeys(ns string) ([][]byte, error) {
	keys := make([][]byte, 0)
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(ns))
		if bucket == nil {
			return errors.Wrapf(ErrNotExist, "bucket = %x doesn't exist", []byte(ns))
		}
		c := bucket.Cursor()
		k, _ := c.First()
		for k != nil {
			key := make([]byte, len(k))
			copy(key, k)
			keys = append(keys, key)
			k, _ = c.Next()
		}
		return nil
	})
	if err == nil {
		return keys, nil
	}
	return nil, err
}

// getVersion retrieves the <k, v> at certain version
func (b *BoltDBVersioned) getVersion(ns string, key []byte) ([]byte, error) {
	key = versionedKey(key, b.version)
	var value []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(ns))
		if bucket == nil {
			return errors.Wrapf(ErrNotExist, "bucket = %x doesn't exist", []byte(ns))
		}
		c := bucket.Cursor()
		k, v := c.Seek(key)
		if k == nil || bytes.Compare(k, key) == 1 {
			k, v = c.Prev()
			key = append(key[:len(key)-8], 0)
			if k == nil || bytes.Compare(k, key) <= 0 {
				// cursor is at the beginning of the bucket or smaller than minimum key
				return errors.Wrapf(ErrNotExist, "key = %x doesn't exist", key[:len(key)-1])
			}
		}
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})
	if err == nil {
		return value, nil
	}
	if errors.Cause(err) == ErrNotExist {
		return nil, err
	}
	return nil, errors.Wrap(ErrIO, err.Error())
}

func versionedKey(key []byte, v uint64) []byte {
	return append(key, byteutil.Uint64ToBytesBigEndian(v)...)
}

func (b *BoltDBVersioned) isVersioned(ns string, key []byte) (bool, int, error) {
	if _, ok := b.nonversioned[ns]; ok {
		return false, 0, nil
	}
	if keyLen, ok := b.versioned[ns]; ok {
		if len(key) != keyLen {
			return true, keyLen, errors.Wrapf(ErrInvalidKeyLength, "expecting %d, got %d", keyLen, len(key))
		}
		return true, keyLen, nil

	}
	// check if the namespace already exist in DB
	vn, err := b.checkVersionedNS(ns)
	if err != nil {
		return false, 0, err
	}
	if vn != nil {
		b.versioned[ns] = int(vn.keyLen)
		return true, int(vn.keyLen), nil
	}
	// namespace not yet created
	return true, 0, ErrNotExist
}

func (b *BoltDBVersioned) checkVersionedNS(ns string) (*versionedNamespace, error) {
	data, err := b.BoltDB.Get(ns, _minKey)
	switch errors.Cause(err) {
	case nil:
		vn, err := DeserializeVersionedNamespace(data)
		if err != nil {
			return nil, err
		}
		return vn, nil
	case ErrNotExist, ErrBucketNotExist:
		return nil, nil
	default:
		return nil, err
	}
}

func (b *BoltDBVersioned) checkVersionedKey(ns string, key []byte) (*keyMeta, error) {
	data, err := b.BoltDB.Get(ns, append(key, 0))
	switch errors.Cause(err) {
	case nil:
		km, err := DeserializeKeyMeta(data)
		if err != nil {
			return nil, err
		}
		return km, nil
	case ErrNotExist, ErrBucketNotExist:
		return nil, nil
	default:
		return nil, err
	}
}

func updateKeyMeta(km *keyMeta, version uint64, hash []byte) (*keyMeta, bool) {
	if km == nil {
		// key not yet written
		return &keyMeta{
			lastWriteHash: hash,
			firstVersion:  version,
			lastVersion:   version,
			deleteVersion: math.MaxUint64,
		}, false
	}
	if version >= km.deleteVersion || version < km.lastVersion {
		// 1. key has been deleted, do nothing
		// 2. writing to an earlier version complicates things, for now it is not allowed
		return km, true
	}
	if bytes.Equal(km.lastWriteHash, hash) {
		// value has no change, do nothing
		return km, true
	}
	km.lastWriteHash = hash
	km.lastVersion = version
	return km, false
}
