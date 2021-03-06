// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

// DO NOT EDIT.
// FILE GENERATED BY BSONGEN.

import (
	"bytes"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
)

// MarshalBson bson-encodes BoundShardQuery.
func (boundShardQuery *BoundShardQuery) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Sql", boundShardQuery.Sql)
	// map[string]interface{}
	{
		bson.EncodePrefix(buf, bson.Object, "BindVariables")
		lenWriter := bson.NewLenWriter(buf)
		for _k, _v1 := range boundShardQuery.BindVariables {
			bson.EncodeInterface(buf, _k, _v1)
		}
		lenWriter.Close()
	}
	bson.EncodeString(buf, "Keyspace", boundShardQuery.Keyspace)
	// []string
	{
		bson.EncodePrefix(buf, bson.Array, "Shards")
		lenWriter := bson.NewLenWriter(buf)
		for _i, _v2 := range boundShardQuery.Shards {
			bson.EncodeString(buf, bson.Itoa(_i), _v2)
		}
		lenWriter.Close()
	}

	lenWriter.Close()
}

// UnmarshalBson bson-decodes into BoundShardQuery.
func (boundShardQuery *BoundShardQuery) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	switch kind {
	case bson.EOO, bson.Object:
		// valid
	case bson.Null:
		return
	default:
		panic(bson.NewBsonError("unexpected kind %v for BoundShardQuery", kind))
	}
	bson.Next(buf, 4)

	for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
		switch bson.ReadCString(buf) {
		case "Sql":
			boundShardQuery.Sql = bson.DecodeString(buf, kind)
		case "BindVariables":
			// map[string]interface{}
			if kind != bson.Null {
				if kind != bson.Object {
					panic(bson.NewBsonError("unexpected kind %v for boundShardQuery.BindVariables", kind))
				}
				bson.Next(buf, 4)
				boundShardQuery.BindVariables = make(map[string]interface{})
				for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
					_k := bson.ReadCString(buf)
					var _v1 interface{}
					_v1 = bson.DecodeInterface(buf, kind)
					boundShardQuery.BindVariables[_k] = _v1
				}
			}
		case "Keyspace":
			boundShardQuery.Keyspace = bson.DecodeString(buf, kind)
		case "Shards":
			// []string
			if kind != bson.Null {
				if kind != bson.Array {
					panic(bson.NewBsonError("unexpected kind %v for boundShardQuery.Shards", kind))
				}
				bson.Next(buf, 4)
				boundShardQuery.Shards = make([]string, 0, 8)
				for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
					bson.SkipIndex(buf)
					var _v2 string
					_v2 = bson.DecodeString(buf, kind)
					boundShardQuery.Shards = append(boundShardQuery.Shards, _v2)
				}
			}
		default:
			bson.Skip(buf, kind)
		}
	}
}
