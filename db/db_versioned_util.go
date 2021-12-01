// Copyright (c) 2023 IoTeX Foundation
// This is an alpha (internal) release and is not suitable for production. This source code is provided 'as is' and no
// warranties are given as to title or non-infringement, merchantability or fitness for purpose and, to the extent
// permitted by law, all liability for your use of the code is disclaimed. This source code is governed by Apache
// License 2.0 that can be found in the LICENSE file.

package db

import (
	"github.com/iotexproject/go-pkgs/byteutil"
	"google.golang.org/protobuf/proto"

	"github.com/iotexproject/iotex-core/db/versionpb"
)

// versionedNamespace is the metadata for versioned namespace
type versionedNamespace struct {
	name   string
	keyLen uint32
}

// Serialize to bytes
func (vn *versionedNamespace) Serialize() []byte {
	return byteutil.Must(proto.Marshal(vn.toProto()))
}

func (vn *versionedNamespace) toProto() *versionpb.VersionedNamespace {
	return &versionpb.VersionedNamespace{
		Name:   vn.name,
		KeyLen: vn.keyLen,
	}
}

func fromProtoVN(pb *versionpb.VersionedNamespace) *versionedNamespace {
	return &versionedNamespace{
		name:   pb.Name,
		keyLen: pb.KeyLen,
	}
}

// DeserializeVersionedNamespace deserializes byte-stream to VersionedNamespace
func DeserializeVersionedNamespace(buf []byte) (*versionedNamespace, error) {
	var vn versionpb.VersionedNamespace
	if err := proto.Unmarshal(buf, &vn); err != nil {
		return nil, err
	}
	return fromProtoVN(&vn), nil
}

// keyMeta is the metadata for key's index
type keyMeta struct {
	lastWriteHash []byte
	firstVersion  uint64
	lastVersion   uint64
	deleteVersion uint64
}

// Serialize to bytes
func (k *keyMeta) Serialize() []byte {
	return byteutil.Must(proto.Marshal(k.toProto()))
}

func (k *keyMeta) toProto() *versionpb.KeyMeta {
	return &versionpb.KeyMeta{
		LastWriteHash: k.lastWriteHash,
		FirstVersion:  k.firstVersion,
		LastVersion:   k.lastVersion,
		DeleteVersion: k.deleteVersion,
	}
}

func fromProtoKM(pb *versionpb.KeyMeta) *keyMeta {
	return &keyMeta{
		lastWriteHash: pb.LastWriteHash,
		firstVersion:  pb.FirstVersion,
		lastVersion:   pb.LastVersion,
		deleteVersion: pb.DeleteVersion,
	}
}

// DeserializeKeyMeta deserializes byte-stream to key meta
func DeserializeKeyMeta(buf []byte) (*keyMeta, error) {
	var km versionpb.KeyMeta
	if err := proto.Unmarshal(buf, &km); err != nil {
		return nil, err
	}
	return fromProtoKM(&km), nil
}
