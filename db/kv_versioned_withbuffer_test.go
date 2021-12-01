// Copyright (c) 2023 IoTeX Foundation
// This is an alpha (internal) release and is not suitable for production. This source code is provided 'as is' and no
// warranties are given as to title or non-infringement, merchantability or fitness for purpose and, to the extent
// permitted by law, all liability for your use of the code is disclaimed. This source code is governed by Apache
// License 2.0 that can be found in the LICENSE file.

package db

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/iotexproject/iotex-core/db/batch"
	"github.com/iotexproject/iotex-core/testutil"
)

func TestKvVersionedWithBuffer(t *testing.T) {
	r := require.New(t)
	testPath, err := testutil.PathOfTempFile("test-kvversion")
	r.NoError(err)
	defer func() {
		testutil.CleanupPath(testPath)
	}()

	cfg := DefaultConfig
	cfg.DbPath = testPath
	kv := NewBoltDBVersioned(cfg, hasher,
		NonversionedNamespaceOption(_nonversioned),
		VersionedNamespaceOption(_versioned0, 5))
	kvb, err := NewKvVersionedWithBuffer(kv, batch.NewCachedBatch(), 3)
	r.NoError(err)
	ctx := context.Background()
	r.NoError(kvb.Start(ctx))
	defer func() {
		kvb.Stop(ctx)
	}()

	// cannot create non-versioned as versioned
	err = kvb.CreateVersionedNamespace(_nonversioned, 5)
	r.Equal(ErrInvalidWrite, errors.Cause(err))

	// cannot create an existing namespace
	err = kvb.CreateVersionedNamespace(_versioned0, 5)
	r.Equal(ErrInvalidWrite, errors.Cause(err))

	// cannot write wrong-length key to existing
	kvb.MustPut(_versioned0, []byte("wronglength"), _v1)
	r.Equal(ErrInvalidKeyLength, errors.Cause(kvb.Commit()))

	// create and write 2 keys
	kvb.MustPut(_nonversioned, _k1, _v4)
	kvb.MustPut(_versioned0, _k1, _v1)
	kvb.MustPut(_versioned0, _k2, _v2)
	r.NoError(kvb.Commit())
	for _, v := range []versionTest{
		{_nonversioned, _k1, _v4, 0, nil},
		{_versioned0, _k1, _v1, 3, nil},
		{_versioned0, _k2, _v2, 3, nil},
	} {
		b, err := kvb.Get(v.ns, v.k)
		r.NoError(err)
		r.Equal(v.v, b)
		h, err := kv.Version(v.ns, v.k)
		r.NoError(err)
		r.Equal(v.height, h)
	}

	// create another namespace and write keys at height 6
	kvb, err = NewKvVersionedWithBuffer(kv, batch.NewCachedBatch(), 6)
	r.NoError(err)
	kvb.MustPut(_versioned1, _k4, _v1)
	r.NoError(kvb.CreateVersionedNamespace(_versioned1, 5))
	err = kvb.CreateVersionedNamespace(_versioned1, 5)
	r.Equal(ErrInvalidWrite, errors.Cause(err)) // cannot create same ns 2 times
	kvb.MustPut(_nonversioned, _k1, _v3)
	kvb.MustPut(_versioned0, _k1, _v2)
	kvb.MustPut(_versioned0, _k2, _v1)
	kvb.MustPut(_versioned1, _k1, _v4)
	kvb.MustPut(_versioned1, _k2, _v3)
	kvb.MustPut(_versioned1, _k3, _v2)
	r.NoError(kvb.Commit())
	for _, v := range []versionTest{
		{_nonversioned, _k1, _v3, 0, nil},
		{_versioned0, _k1, _v2, 6, nil},
		{_versioned0, _k2, _v1, 7, nil},
		{_versioned1, _k1, _v4, 6, nil},
		{_versioned1, _k2, _v3, 7, nil},
		{_versioned1, _k3, _v2, 8, nil},
		{_versioned1, _k4, _v1, 8, nil},
		{_versioned0, _k1, _v1, 4, nil},
		{_versioned0, _k2, _v2, 3, nil},
	} {
		b, err := kv.SetVersion(v.height).Get(v.ns, v.k)
		if err == nil {
			r.Equal(v.v, b)
		} else {
			r.Equal(v.err, errors.Cause(err))
		}
	}

	kvb.MustPut("vsn2", _k1, _v1)
	r.NoError(kvb.Commit())
}
