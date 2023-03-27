// Copyright 2023 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ccl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLookupCC(t *testing.T) {
	r := require.New(t)

	r.Equal(LookUpVis(Plain, Plain), Plain)
	r.Equal(LookUpVis(Plain, Join), Join)
	r.Equal(LookUpVis(Plain, GroupBy), GroupBy)
	r.Equal(LookUpVis(Plain, Compare), Compare)
	r.Equal(LookUpVis(Plain, Encrypt), Encrypt)
	r.Equal(LookUpVis(Compare, Plain), Compare)
	r.Equal(LookUpVis(Compare, Join), Unknown)
	r.Equal(LookUpVis(Compare, GroupBy), Unknown)
	r.Equal(LookUpVis(Compare, Compare), Compare)
	r.Equal(LookUpVis(Compare, Encrypt), Encrypt)
	r.Equal(LookUpVis(Encrypt, Plain), Encrypt)
	r.Equal(LookUpVis(Encrypt, Join), Encrypt)
	r.Equal(LookUpVis(Encrypt, GroupBy), Encrypt)
	r.Equal(LookUpVis(Encrypt, Compare), Encrypt)
	r.Equal(LookUpVis(Encrypt, Encrypt), Encrypt)

	r.Equal(inferCompareFuncCCL(Plain, Plain), Plain)
	r.Equal(inferCompareFuncCCL(Plain, Join), Join)
	r.Equal(inferCompareFuncCCL(Plain, GroupBy), GroupBy)
	r.Equal(inferCompareFuncCCL(Plain, Compare), Plain)
	r.Equal(inferCompareFuncCCL(Plain, Encrypt), Encrypt)
	r.Equal(inferCompareFuncCCL(Compare, Plain), Plain)
	r.Equal(inferCompareFuncCCL(Compare, Compare), Plain)
	r.Equal(inferCompareFuncCCL(Compare, Join), Unknown)
	r.Equal(inferCompareFuncCCL(Compare, GroupBy), Unknown)
	r.Equal(inferCompareFuncCCL(Compare, Encrypt), Encrypt)
	r.Equal(inferCompareFuncCCL(Encrypt, Plain), Encrypt)
	r.Equal(inferCompareFuncCCL(Encrypt, Compare), Encrypt)
	r.Equal(inferCompareFuncCCL(Encrypt, Join), Encrypt)
	r.Equal(inferCompareFuncCCL(Encrypt, GroupBy), Encrypt)
	r.Equal(inferCompareFuncCCL(Encrypt, Encrypt), Encrypt)
}

func TestInferBinaryOpOutputVisibility(t *testing.T) {
	r := require.New(t)
	left := NewCCL()
	right := NewCCL()
	// set all level plain and for compare function
	left.SetLevelForParty("alice", Plain)
	left.SetLevelForParty("bob", Plain)
	left.SetLevelForParty("carol", Plain)
	right.SetLevelForParty("alice", Plain)
	right.SetLevelForParty("bob", Plain)
	right.SetLevelForParty("carol", Plain)
	left.InferCompareFuncCCL(right)
	r.Equal(Plain, left.LevelFor("alice"))
	r.Equal(Plain, left.LevelFor("bob"))
	r.Equal(Plain, left.LevelFor("carol"))

	// set all level plain and for other function
	left.SetLevelForParty("alice", Plain)
	left.SetLevelForParty("bob", Plain)
	left.SetLevelForParty("carol", Plain)
	right.SetLevelForParty("alice", Plain)
	right.SetLevelForParty("bob", Plain)
	right.SetLevelForParty("carol", Plain)
	left.UpdateMoreRestrictedCCLFrom(right)
	r.Equal(Plain, left.LevelFor("alice"))
	r.Equal(Plain, left.LevelFor("bob"))
	r.Equal(Plain, left.LevelFor("carol"))

	// set level reduce and for other function
	left.SetLevelForParty("alice", Plain)
	left.SetLevelForParty("bob", Compare)
	left.SetLevelForParty("carol", Plain)
	right.SetLevelForParty("alice", Plain)
	right.SetLevelForParty("bob", Compare)
	right.SetLevelForParty("carol", Plain)
	left.UpdateMoreRestrictedCCLFrom(right)
	r.Equal(Plain, left.LevelFor("alice"))
	r.Equal(Compare, left.LevelFor("bob"))
	r.Equal(Plain, left.LevelFor("carol"))

	// set level reduce and for compare function
	left.SetLevelForParty("alice", Compare)
	left.SetLevelForParty("bob", Plain)
	left.SetLevelForParty("carol", Plain)
	right.SetLevelForParty("alice", Compare)
	right.SetLevelForParty("bob", Plain)
	right.SetLevelForParty("carol", Plain)
	left.InferCompareFuncCCL(right)
	r.Equal(Plain, left.LevelFor("alice"))
	r.Equal(Plain, left.LevelFor("bob"))
	r.Equal(Plain, left.LevelFor("carol"))

	// set level reduce and for compare function
	left.SetLevelForParty("alice", Compare)
	left.SetLevelForParty("bob", Compare)
	left.SetLevelForParty("carol", Plain)
	right.SetLevelForParty("alice", Compare)
	right.SetLevelForParty("bob", Compare)
	right.SetLevelForParty("carol", Plain)
	left.InferCompareFuncCCL(right)
	r.Equal(Plain, left.LevelFor("alice"))
	r.Equal(Plain, left.LevelFor("bob"))
	r.Equal(Plain, left.LevelFor("carol"))

	// set level group and for compare function
	left.SetLevelForParty("alice", Plain)
	left.SetLevelForParty("bob", Plain)
	left.SetLevelForParty("carol", GroupBy)
	right.SetLevelForParty("alice", Plain)
	right.SetLevelForParty("bob", Plain)
	right.SetLevelForParty("carol", GroupBy)
	left.InferCompareFuncCCL(right)
	r.Equal(Plain, left.LevelFor("alice"))
	r.Equal(Plain, left.LevelFor("bob"))
	r.Equal(GroupBy, left.LevelFor("carol"))

	// set level group and for other function
	left.SetLevelForParty("alice", Plain)
	left.SetLevelForParty("bob", Plain)
	left.SetLevelForParty("carol", GroupBy)
	right.SetLevelForParty("alice", Plain)
	right.SetLevelForParty("bob", Plain)
	right.SetLevelForParty("carol", GroupBy)
	left.InferCompareFuncCCL(right)
	r.Equal(Plain, left.LevelFor("alice"))
	r.Equal(Plain, left.LevelFor("bob"))
	r.Equal(GroupBy, left.LevelFor("carol"))

	// set level encrypt and for other function
	left.SetLevelForParty("alice", Encrypt)
	left.SetLevelForParty("bob", Plain)
	left.SetLevelForParty("carol", Plain)
	right.SetLevelForParty("alice", Encrypt)
	right.SetLevelForParty("bob", Plain)
	right.SetLevelForParty("carol", Plain)
	left.InferCompareFuncCCL(right)
	r.Equal(Encrypt, left.LevelFor("alice"))
	r.Equal(Plain, left.LevelFor("bob"))
	r.Equal(Plain, left.LevelFor("carol"))

	// set level encrypt and for compare function
	left.SetLevelForParty("alice", Encrypt)
	left.SetLevelForParty("bob", Plain)
	left.SetLevelForParty("carol", Plain)
	right.SetLevelForParty("alice", Encrypt)
	right.SetLevelForParty("bob", Plain)
	right.SetLevelForParty("carol", Plain)
	left.UpdateMoreRestrictedCCLFrom(right)
	r.Equal(Encrypt, left.LevelFor("alice"))
	r.Equal(Plain, left.LevelFor("bob"))
	r.Equal(Plain, left.LevelFor("carol"))

	// set level encrypt and for compare function
	left.SetLevelForParty("alice", Encrypt)
	left.SetLevelForParty("bob", Plain)
	left.SetLevelForParty("carol", Plain)
	right.SetLevelForParty("alice", Encrypt)
	right.SetLevelForParty("bob", Plain)
	right.SetLevelForParty("carol", Plain)
	left.InferCompareFuncCCL(right)
	r.Equal(Encrypt, left.LevelFor("alice"))
	r.Equal(Plain, left.LevelFor("bob"))
	r.Equal(Plain, left.LevelFor("carol"))

	// set hybrid level and for compare function
	left.SetLevelForParty("alice", Encrypt)
	left.SetLevelForParty("bob", Plain)
	left.SetLevelForParty("carol", GroupBy)
	right.SetLevelForParty("alice", GroupBy)
	right.SetLevelForParty("bob", GroupBy)
	right.SetLevelForParty("carol", Compare)
	left.InferCompareFuncCCL(right)
	r.Equal(Encrypt, left.LevelFor("alice"))
	r.Equal(GroupBy, left.LevelFor("bob"))
	r.Equal(Unknown, left.LevelFor("carol"))

	// set hybrid level and for compare function
	left.SetLevelForParty("alice", Encrypt)
	left.SetLevelForParty("bob", Join)
	left.SetLevelForParty("carol", Join)
	right.SetLevelForParty("alice", GroupBy)
	right.SetLevelForParty("bob", Plain)
	right.SetLevelForParty("carol", Compare)
	left.InferCompareFuncCCL(right)
	r.Equal(Encrypt, left.LevelFor("alice"))
	r.Equal(Join, left.LevelFor("bob"))
	r.Equal(Unknown, left.LevelFor("carol"))

	// set hybrid level and for compare function
	left.SetLevelForParty("alice", Encrypt)
	left.SetLevelForParty("bob", Plain)
	left.SetLevelForParty("carol", GroupBy)
	right.SetLevelForParty("alice", GroupBy)
	right.SetLevelForParty("bob", GroupBy)
	right.SetLevelForParty("carol", Compare)
	left.InferCompareFuncCCL(right)
	r.Equal(Encrypt, left.LevelFor("alice"))
	r.Equal(GroupBy, left.LevelFor("bob"))
	r.Equal(Unknown, left.LevelFor("carol"))
}
