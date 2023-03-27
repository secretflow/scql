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

package translator

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/interpreter/ccl"
)

func TestLogicalNode(t *testing.T) {
	r := require.New(t)
	parties := []string{"alice", "bob"}
	ccls := map[int64]*ccl.CCL{
		1: ccl.CreateAllPlainCCL(parties),
		2: ccl.CreateAllPlainCCL(parties),
		3: ccl.CreateAllPlainCCL(parties),
	}
	node := baseNode{
		ccl: ccls,
	}
	sort.Strings(parties)
	res := node.VisibleParty()
	sort.Strings(res)
	r.Equal(parties, res)

	ccls[1].SetLevelForParty("alice", ccl.GroupBy)
	node = baseNode{
		ccl: ccls,
	}
	res = node.VisibleParty()
	sort.Strings(res)
	r.Equal([]string{"bob"}, res)

	ccls[2].SetLevelForParty("alice", ccl.Encrypt)
	node = baseNode{
		ccl: ccls,
	}
	res = node.VisibleParty()
	sort.Strings(res)
	r.Equal([]string{"bob"}, res)

	ccls[3].SetLevelForParty("bob", ccl.Join)
	node = baseNode{
		ccl: ccls,
	}
	res = node.VisibleParty()
	sort.Strings(res)
	var expected []string
	r.Equal(expected, res)
}
