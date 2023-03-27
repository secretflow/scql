// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package codec

import (
	"bytes"
	"math"
	"testing"

	. "github.com/pingcap/check"

	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testCodecSuite{})

type testCodecSuite struct {
}

func (s *testCodecSuite) TestCodecKey(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  []types.Datum
		Expect []types.Datum
	}{
		{
			types.MakeDatums(int64(1)),
			types.MakeDatums(int64(1)),
		},

		{
			types.MakeDatums(float32(1), float64(3.15), []byte("123"), "123"),
			types.MakeDatums(float64(1), float64(3.15), []byte("123"), []byte("123")),
		},
		{
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
		},

		{
			types.MakeDatums(true, false),
			types.MakeDatums(int64(1), int64(0)),
		},

		{
			types.MakeDatums(nil),
			types.MakeDatums(nil),
		},
	}
	sc := &stmtctx.StatementContext{}
	for i, t := range table {
		comment := Commentf("%d %v", i, t)
		b, err := EncodeKey(sc, nil, t.Input...)
		c.Assert(err, IsNil, comment)
		args, err := Decode(b, 1)
		c.Assert(err, IsNil)
		c.Assert(args, DeepEquals, t.Expect)

		b, err = EncodeValue(sc, nil, t.Input...)
		c.Assert(err, IsNil)
		size, err := estimateValuesSize(sc, t.Input)
		c.Assert(err, IsNil)
		c.Assert(len(b), Equals, size)
		args, err = Decode(b, 1)
		c.Assert(err, IsNil)
		c.Assert(args, DeepEquals, t.Expect)
	}
}

func estimateValuesSize(sc *stmtctx.StatementContext, vals []types.Datum) (int, error) {
	size := 0
	for _, val := range vals {
		length, err := EstimateValueSize(sc, val)
		if err != nil {
			return 0, err
		}
		size += length
	}
	return size, nil
}

func (s *testCodecSuite) TestCodecKeyCompare(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Left   []types.Datum
		Right  []types.Datum
		Expect int
	}{
		{
			types.MakeDatums(1),
			types.MakeDatums(1),
			0,
		},
		{
			types.MakeDatums(-1),
			types.MakeDatums(1),
			-1,
		},
		{
			types.MakeDatums(3.15),
			types.MakeDatums(3.12),
			1,
		},
		{
			types.MakeDatums("abc"),
			types.MakeDatums("abcd"),
			-1,
		},
		{
			types.MakeDatums("abcdefgh"),
			types.MakeDatums("abcdefghi"),
			-1,
		},
		{
			types.MakeDatums(1, "abc"),
			types.MakeDatums(1, "abcd"),
			-1,
		},
		{
			types.MakeDatums(1, "abc", "def"),
			types.MakeDatums(1, "abcd", "af"),
			-1,
		},
		{
			types.MakeDatums(3.12, "ebc", "def"),
			types.MakeDatums(2.12, "abcd", "af"),
			1,
		},
		{
			types.MakeDatums([]byte{0x01, 0x00}, []byte{0xFF}),
			types.MakeDatums([]byte{0x01, 0x00, 0xFF}),
			-1,
		},
		{
			types.MakeDatums([]byte{0x01}, uint64(0xFFFFFFFFFFFFFFF)),
			types.MakeDatums([]byte{0x01, 0x10}, 0),
			-1,
		},
		{
			types.MakeDatums(0),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums([]byte{0x00}),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums(math.SmallestNonzeroFloat64),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums(int64(math.MinInt64)),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums(1, int64(math.MinInt64), nil),
			types.MakeDatums(1, nil, uint64(math.MaxUint64)),
			1,
		},
		{
			types.MakeDatums(1, []byte{}, nil),
			types.MakeDatums(1, nil, 123),
			1,
		},
	}
	sc := &stmtctx.StatementContext{}
	for _, t := range table {
		b1, err := EncodeKey(sc, nil, t.Left...)
		c.Assert(err, IsNil)

		b2, err := EncodeKey(sc, nil, t.Right...)
		c.Assert(err, IsNil)

		c.Assert(bytes.Compare(b1, b2), Equals, t.Expect, Commentf("%v - %v - %v - %v - %v", t.Left, t.Right, b1, b2, t.Expect))
	}
}

func (s *testCodecSuite) TestNumberCodec(c *C) {
	defer testleak.AfterTest(c)()
	tblInt64 := []int64{
		math.MinInt64,
		math.MinInt32,
		math.MinInt16,
		math.MinInt8,
		0,
		math.MaxInt8,
		math.MaxInt16,
		math.MaxInt32,
		math.MaxInt64,
		1<<47 - 1,
		-1 << 47,
		1<<23 - 1,
		-1 << 23,
		1<<55 - 1,
		-1 << 55,
		1,
		-1,
	}

	for _, t := range tblInt64 {
		b := EncodeInt(nil, t)
		_, v, err := DecodeInt(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeIntDesc(nil, t)
		_, v, err = DecodeIntDesc(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeVarint(nil, t)
		_, v, err = DecodeVarint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeComparableVarint(nil, t)
		_, v, err = DecodeComparableVarint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)
	}

	tblUint64 := []uint64{
		0,
		math.MaxUint8,
		math.MaxUint16,
		math.MaxUint32,
		math.MaxUint64,
		1<<24 - 1,
		1<<48 - 1,
		1<<56 - 1,
		1,
		math.MaxInt16,
		math.MaxInt8,
		math.MaxInt32,
		math.MaxInt64,
	}

	for _, t := range tblUint64 {
		b := EncodeUint(nil, t)
		_, v, err := DecodeUint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeUintDesc(nil, t)
		_, v, err = DecodeUintDesc(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeUvarint(nil, t)
		_, v, err = DecodeUvarint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeComparableUvarint(nil, t)
		_, v, err = DecodeComparableUvarint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)
	}
	var b []byte
	b = EncodeComparableVarint(b, -1)
	b = EncodeComparableUvarint(b, 1)
	b = EncodeComparableVarint(b, 2)
	b, i, err := DecodeComparableVarint(b)
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(-1))
	b, u, err := DecodeComparableUvarint(b)
	c.Assert(err, IsNil)
	c.Assert(u, Equals, uint64(1))
	_, i, err = DecodeComparableVarint(b)
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(2))
}

func (s *testCodecSuite) TestNumberOrder(c *C) {
	defer testleak.AfterTest(c)()
	tblInt64 := []struct {
		Arg1 int64
		Arg2 int64
		Ret  int
	}{
		{-1, 1, -1},
		{math.MaxInt64, math.MinInt64, 1},
		{math.MaxInt64, math.MaxInt32, 1},
		{math.MinInt32, math.MaxInt16, -1},
		{math.MinInt64, math.MaxInt8, -1},
		{0, math.MaxInt8, -1},
		{math.MinInt8, 0, -1},
		{math.MinInt16, math.MaxInt16, -1},
		{1, -1, 1},
		{1, 0, 1},
		{-1, 0, -1},
		{0, 0, 0},
		{math.MaxInt16, math.MaxInt16, 0},
	}

	for _, t := range tblInt64 {
		b1 := EncodeInt(nil, t.Arg1)
		b2 := EncodeInt(nil, t.Arg2)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)

		b1 = EncodeIntDesc(nil, t.Arg1)
		b2 = EncodeIntDesc(nil, t.Arg2)

		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, -t.Ret)

		b1 = EncodeComparableVarint(nil, t.Arg1)
		b2 = EncodeComparableVarint(nil, t.Arg2)
		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)
	}

	tblUint64 := []struct {
		Arg1 uint64
		Arg2 uint64
		Ret  int
	}{
		{0, 0, 0},
		{1, 0, 1},
		{0, 1, -1},
		{math.MaxInt8, math.MaxInt16, -1},
		{math.MaxUint32, math.MaxInt32, 1},
		{math.MaxUint8, math.MaxInt8, 1},
		{math.MaxUint16, math.MaxInt32, -1},
		{math.MaxUint64, math.MaxInt64, 1},
		{math.MaxInt64, math.MaxUint32, 1},
		{math.MaxUint64, 0, 1},
		{0, math.MaxUint64, -1},
	}

	for _, t := range tblUint64 {
		b1 := EncodeUint(nil, t.Arg1)
		b2 := EncodeUint(nil, t.Arg2)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)

		b1 = EncodeUintDesc(nil, t.Arg1)
		b2 = EncodeUintDesc(nil, t.Arg2)

		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, -t.Ret)

		b1 = EncodeComparableUvarint(nil, t.Arg1)
		b2 = EncodeComparableUvarint(nil, t.Arg2)
		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)
	}
}

func (s *testCodecSuite) TestFloatCodec(c *C) {
	defer testleak.AfterTest(c)()
	tblFloat := []float64{
		-1,
		0,
		1,
		math.MaxFloat64,
		math.MaxFloat32,
		math.SmallestNonzeroFloat32,
		math.SmallestNonzeroFloat64,
		math.Inf(-1),
		math.Inf(1),
	}

	for _, t := range tblFloat {
		b := EncodeFloat(nil, t)
		_, v, err := DecodeFloat(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeFloatDesc(nil, t)
		_, v, err = DecodeFloatDesc(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)
	}

	tblCmp := []struct {
		Arg1 float64
		Arg2 float64
		Ret  int
	}{
		{1, -1, 1},
		{1, 0, 1},
		{0, -1, 1},
		{0, 0, 0},
		{math.MaxFloat64, 1, 1},
		{math.MaxFloat32, math.MaxFloat64, -1},
		{math.MaxFloat64, 0, 1},
		{math.MaxFloat64, math.SmallestNonzeroFloat64, 1},
		{math.Inf(-1), 0, -1},
		{math.Inf(1), 0, 1},
		{math.Inf(-1), math.Inf(1), -1},
	}

	for _, t := range tblCmp {
		b1 := EncodeFloat(nil, t.Arg1)
		b2 := EncodeFloat(nil, t.Arg2)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)

		b1 = EncodeFloatDesc(nil, t.Arg1)
		b2 = EncodeFloatDesc(nil, t.Arg2)

		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, -t.Ret)
	}
}

func (s *testCodecSuite) TestBytes(c *C) {
	defer testleak.AfterTest(c)()
	tblBytes := [][]byte{
		{},
		{0x00, 0x01},
		{0xff, 0xff},
		{0x01, 0x00},
		[]byte("abc"),
		[]byte("hello world"),
	}

	for _, t := range tblBytes {
		b := EncodeBytes(nil, t)
		_, v, err := DecodeBytes(b, nil)
		c.Assert(err, IsNil)
		c.Assert(t, DeepEquals, v, Commentf("%v - %v - %v", t, b, v))

		b = EncodeBytesDesc(nil, t)
		_, v, err = DecodeBytesDesc(b, nil)
		c.Assert(err, IsNil)
		c.Assert(t, DeepEquals, v, Commentf("%v - %v - %v", t, b, v))
	}

	tblCmp := []struct {
		Arg1 []byte
		Arg2 []byte
		Ret  int
	}{
		{[]byte{}, []byte{0x00}, -1},
		{[]byte{0x00}, []byte{0x00}, 0},
		{[]byte{0xFF}, []byte{0x00}, 1},
		{[]byte{0xFF}, []byte{0xFF, 0x00}, -1},
		{[]byte("a"), []byte("b"), -1},
		{[]byte("a"), []byte{0x00}, 1},
		{[]byte{0x00}, []byte{0x01}, -1},
		{[]byte{0x00, 0x01}, []byte{0x00, 0x00}, 1},
		{[]byte{0x00, 0x00, 0x00}, []byte{0x00, 0x00}, 1},
		{[]byte{0x00, 0x00, 0x00}, []byte{0x00, 0x00}, 1},
		{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, -1},
		{[]byte{0x01, 0x02, 0x03, 0x00}, []byte{0x01, 0x02, 0x03}, 1},
		{[]byte{0x01, 0x03, 0x03, 0x04}, []byte{0x01, 0x03, 0x03, 0x05}, -1},
		{[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, -1},
		{[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, 1},
		{[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00}, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, 1},
	}

	for _, t := range tblCmp {
		b1 := EncodeBytes(nil, t.Arg1)
		b2 := EncodeBytes(nil, t.Arg2)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)

		b1 = EncodeBytesDesc(nil, t.Arg1)
		b2 = EncodeBytesDesc(nil, t.Arg2)

		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, -t.Ret)
	}
}
