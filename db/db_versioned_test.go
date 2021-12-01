// Copyright (c) 2021 IoTeX Foundation
// This is an alpha (internal) release and is not suitable for production. This source code is provided 'as is' and no
// warranties are given as to title or non-infringement, merchantability or fitness for purpose and, to the extent
// permitted by law, all liability for your use of the code is disclaimed. This source code is governed by Apache
// License 2.0 that can be found in the LICENSE file.

package db

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/iotexproject/go-pkgs/hash"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/iotexproject/iotex-core/db/batch"
	"github.com/iotexproject/iotex-core/testutil"
)

const (
	_nonversioned = "nvd"
	_versioned0   = "vsn0"
	_versioned1   = "vsn1"
)

type versionTest struct {
	ns     string
	k, v   []byte
	height uint64
	err    error
}

var (
	hasher = func(in []byte) []byte {
		h := hash.Hash160b(in)
		return h[:]
	}
)

func TestVersionedDB(t *testing.T) {
	r := require.New(t)
	testPath, err := testutil.PathOfTempFile("test-version")
	r.NoError(err)
	defer func() {
		testutil.CleanupPath(testPath)
	}()

	cfg := DefaultConfig
	cfg.DbPath = testPath
	kv := NewBoltDBVersioned(cfg, hasher, NonversionedNamespaceOption(_nonversioned))
	ctx := context.Background()
	r.NoError(kv.Start(ctx))
	defer func() {
		kv.Stop(ctx)
	}()

	r.NoError(kv.Put(_nonversioned, _k1, _v1))
	b, err := kv.Get(_nonversioned, _k1)
	r.NoError(err)
	r.Equal(_v1, b)

	// cannot create non-versioned as versioned
	r.Equal(ErrInvalidWrite, errors.Cause(kv.CreateVersionedNamespace(_nonversioned, 5)))
	// create versioned namespace
	r.NoError(kv.CreateVersionedNamespace(_versioned0, 5))
	// cannot create an existing namespace
	r.Equal(ErrInvalidWrite, errors.Cause(kv.CreateVersionedNamespace(_versioned0, 5)))

	// test versioned keys
	for i, v := range []versionTest{
		{_versioned0, _k1, _v1, 0, nil},
		{_versioned0, _k1, _v2, 3, nil},
		{_versioned0, _k1, _v3, 6, nil},
		{_versioned0, _k1, _v4, 9, nil},
		{_versioned0, _k2, _v2, 1, nil},
		{_versioned0, _k2, _v3, 4, nil},
		{_versioned0, _k2, _v4, 7, nil},
		{_versioned0, _k2, _v1, 10, nil},
		{_versioned0, _k3, _v3, 2, nil},
		{_versioned0, _k3, _v4, 5, nil},
		{_versioned0, _k3, _v1, 8, nil},
		{_versioned0, _k3, _v2, 11, nil},
		{_versioned0, _k1, _v4, 9, nil},  // write <k, v> at same height again
		{_versioned0, _k2, _v1, 11, nil}, // write <k, v> at later height
		{_versioned0, _k3, _v4, 7, nil},  // write <k, v> at earlier height

		{_versioned0, _k1, _v1, 1, nil},
		{_versioned0, _k1, _v2, 4, nil},
		{_versioned0, _k1, _v3, 7, nil},
		{_versioned0, _k1, _v4, 10, nil},
		{_versioned0, _k2, nil, 0, ErrNotExist},
		{_versioned0, _k2, _v2, 2, nil},
		{_versioned0, _k2, _v3, 5, nil},
		{_versioned0, _k2, _v4, 8, nil},
		{_versioned0, _k2, _v1, 11, nil},
		{_versioned0, _k3, nil, 1, ErrNotExist},
		{_versioned0, _k3, _v3, 3, nil},
		{_versioned0, _k3, _v4, 6, nil},
		{_versioned0, _k3, _v1, 9, nil},
		{_versioned0, _k3, _v2, 12, nil},
		{_versioned0, []byte("key_0"), nil, 0, ErrNotExist},
		{_versioned0, _k4, nil, 0, ErrNotExist},
	} {
		if i < 15 {
			r.NoError(kv.SetVersion(v.height).Put(v.ns, v.k, v.v))
		}
		b, err := kv.SetVersion(v.height).Get(v.ns, v.k)
		if err == nil {
			r.Equal(v.v, b)
		} else {
			r.Equal(v.err, errors.Cause(err))
		}
	}

	keys, err := kv.getAllKeys(_versioned0)
	r.NoError(err)
	for i := range keys {
		fmt.Println("key = ", hex.EncodeToString(keys[i]))
	}

	// create namespace on first Put()
	r.NoError(kv.SetVersion(3).Put(_versioned1, _k1, _v1))
	b, err = kv.Get(_versioned1, _k1)
	r.NoError(err)
	r.Equal(_v1, b)
	_, err = kv.Get(_versioned1, _k2)
	r.Equal(ErrNotExist, errors.Cause(err))

	// test key's version
	for _, v := range []versionTest{
		{_nonversioned, _k1, _v1, 0, nil},
		{_versioned0, _k1, _v4, 9, nil},
		{_versioned0, _k2, _v1, 10, nil},
		{_versioned0, _k3, _v2, 11, nil},
		{_versioned0, _k4, nil, 0, ErrNotExist},
		{_versioned1, _k1, _v1, 3, nil},
		{_versioned1, _k2, nil, 0, ErrNotExist},
	} {
		h, err := kv.Version(v.ns, v.k)
		if err == nil {
			r.Equal(v.height, h)
		} else {
			r.Equal(v.err, errors.Cause(err))
		}
		b, err := kv.SetVersion(h+1).Get(v.ns, v.k)
		if err == nil {
			r.Equal(v.v, b)
		} else {
			r.Equal(v.err, errors.Cause(err))
		}
	}

	// checkVersionedNS
	for _, v := range []struct {
		ns     string
		keyLen int
	}{
		{_nonversioned, 0},
		{_versioned0, 5},
		{_versioned1, 5},
		{"notexist", 0},
	} {
		vn, err := kv.checkVersionedNS(v.ns)
		r.NoError(err)
		if v.keyLen > 0 {
			r.Equal(v.ns, vn.name)
			r.EqualValues(v.keyLen, vn.keyLen)
		} else {
			r.Nil(vn)
		}
	}

	// isVersioned
	for _, v := range []versionTest{
		{_nonversioned, _k4, nil, 0, nil},
		{_versioned0, _k1, _v4, 0, nil},
		{_versioned0, _k2, _v1, 10, nil},
		{_versioned1, _k3, _v2, 3, nil},
		{"notexist", _k4, _v2, 3, ErrNotExist},
	} {
		ok, _, err := kv.isVersioned(v.ns, v.k)
		r.Equal(v.err, err)
		r.Equal(v.v != nil, ok)
	}

	// test non-existing bucket and wrong-length key
	_, err = kv.Get("notexist", _k1)
	r.Equal(ErrNotExist, errors.Cause(err))
	_, err = kv.Version("notexist", _k1)
	r.Equal(ErrNotExist, errors.Cause(err))
	_, err = kv.Get(_versioned0, []byte("notexist"))
	r.Equal(ErrInvalidKeyLength, errors.Cause(err))
	err = kv.Put(_versioned0, []byte("notexist"), nil)
	r.Equal(ErrInvalidKeyLength, errors.Cause(err))

	// test claim versioned namespace as non-versioned
	r.NoError(kv.Stop(ctx))
	kv = NewBoltDBVersioned(cfg, hasher, NonversionedNamespaceOption(_versioned0))
	r.Equal(ErrDBCorruption, errors.Cause(kv.Start(ctx)))
	r.NoError(kv.Stop(ctx))

	// restart the DB and check cold keys
	kv = NewBoltDBVersioned(cfg, hasher, NonversionedNamespaceOption(_nonversioned))
	r.NoError(kv.Start(ctx))
	_, err = kv.Get("notexist", _k1)
	r.Equal(ErrNotExist, errors.Cause(err))
	_, err = kv.Version("notexist", _k1)
	r.Equal(ErrNotExist, errors.Cause(err))
	_, err = kv.Get(_versioned0, []byte("notexist"))
	r.Equal(ErrInvalidKeyLength, errors.Cause(err))
	err = kv.Put(_versioned0, []byte("notexist"), nil)
	r.Equal(ErrInvalidKeyLength, errors.Cause(err))
}

func TestPb(t *testing.T) {
	r := require.New(t)

	vn := &versionedNamespace{
		name:   "3jfsp5@(%)EW*#)_#¡ªº–ƒ˚œade∆…",
		keyLen: 5}
	data := vn.Serialize()
	r.Equal("0a29336a667370354028252945572a23295f23c2a1c2aac2bae28093c692cb9ac593616465e28886e280a61005", hex.EncodeToString(data))
	vn1, err := DeserializeVersionedNamespace(data)
	r.NoError(err)
	r.Equal(vn, vn1)
}

func TestDigest(t *testing.T) {
	r := require.New(t)
	testPath, err := testutil.PathOfTempFile("test-digest")
	r.NoError(err)
	defer func() {
		testutil.CleanupPath(testPath)
	}()

	cfg := DefaultConfig
	cfg.DbPath = testPath
	dba := NewBoltDBVersioned(cfg, hasher)
	ctx := context.Background()
	r.NoError(dba.Start(ctx))
	defer dba.Stop(ctx)

	for _, kv := range []KVStore{
		NewMemKVStore(),
		dba,
	} {
		kvf, err := NewKVStoreFlusher(kv, batch.NewCachedBatch(),
			SerializeOption(func(wi *batch.WriteInfo) []byte {
				return wi.Serialize()
			}))
		r.NoError(err)

		r.NoError(kvf.KVStoreWithBuffer().Put(_versioned0, _k1, _v2))
		r.NoError(kvf.KVStoreWithBuffer().Put(_versioned0, _k2, _v1))
		r.NoError(kvf.KVStoreWithBuffer().Delete(_versioned0, _k1))
		h := hash.Hash256b(kvf.SerializeQueue())
		r.Equal("efe1accb011e4f1e3d7c12d32917870152d6ac939d95fcd79f7f97e51fb22fb6", hex.EncodeToString(h[:]))
	}
}
