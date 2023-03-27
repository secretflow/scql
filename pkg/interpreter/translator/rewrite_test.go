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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRewrite(t *testing.T) {
	a := require.New(t)
	partyInfo, err := NewPartyInfo([]string{"alice"}, []string{"alice.com"}, []string{"alice_credential"})
	assert.Nil(t, err)

	infos := NewEnginesInfo(partyInfo, map[string][]DbTable{
		"alice": []DbTable{NewDbTable("d1", "t1")},
	})
	infos.UpdateTableToRefs(map[DbTable]DbTable{
		NewDbTable("d1", "t1"): NewDbTable("d2", "t2"),
	})
	testCases := [][2]string{
		{"select * from d1.t1", "select * from `d2`.`t2`"},
		{"select * from d1.t1 where c > 2", "select * from `d2`.`t2` where `c`>2"},
		{"select * from d1.t1 where c = 'xxxx'", "select * from `d2`.`t2` where `c`='xxxx'"},
		{"select d1.t1.x from d1.t1 where d1.t1.c = 'xxxx'", "select `d2`.`t2`.`x` from `d2`.`t2` where `d2`.`t2`.`c`='xxxx'"},
		{"select case when (d1.t1.prob2<0.5) then 0 when (d1.t1.prob2<1) then 1 else 2 end from t1",
			"select case when (`d2`.`t2`.`prob2`<0.5) then 0 when (`d2`.`t2`.`prob2`<1) then 1 else 2 end from `t1`"},
		{"select case when ((d1.t1.prob2<0.5) and (d1.t1.prob2>0)) then 0 when (d1.t1.prob2<1) then 1 else 2 end from t1",
			"select case when ((`d2`.`t2`.`prob2`<0.5) and (`d2`.`t2`.`prob2`>0)) then 0 when (`d2`.`t2`.`prob2`<1) then 1 else 2 end from `t1`"},
	}
	for _, tc := range testCases {
		after, newTableRefs, err := rewrite(tc[0], []string{"d1.t1"}, infos, false)
		a.NoError(err)
		a.Equal(tc[1], after)
		a.ElementsMatch(newTableRefs, []string{"d2.t2"})
	}

	testCases2 := [][2]string{
		{"select * from d1.t1", "select * from `d2`.`t2`"},
		{"select * from d1.t1 where c > 2", "select * from `d2`.`t2` where `c`>2"},
		{"select * from d1.t1 where c = 'xxxx'", "select * from `d2`.`t2` where `c`='xxxx'"},
		{"select d1.t1.x from d1.t1 where d1.t1.c = 'xxxx'", "select `t2`.`x` from `d2`.`t2` where `t2`.`c`='xxxx'"},
		{"select case when (d1.t1.prob2<0.5) then 0 when (d1.t1.prob2<1) then 1 else 2 end from t1",
			"select case when (`t2`.`prob2`<0.5) then 0 when (`t2`.`prob2`<1) then 1 else 2 end from `t1`"},
		{"select d1.t1.x from d1.t1", "select `t2`.`x` from `d2`.`t2`"},
	}
	for _, tc := range testCases2 {
		after, newTableRefs, err := rewrite(tc[0], []string{"d1.t1"}, infos, true)
		a.NoError(err)
		a.Equal(tc[1], after)
		a.ElementsMatch(newTableRefs, []string{"d2.t2"})
	}
}
