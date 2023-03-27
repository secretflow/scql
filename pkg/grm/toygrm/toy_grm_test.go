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

package toygrm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetToyGRMInfo(t *testing.T) {
	r := require.New(t)
	{
		info, err := New("testdata/toy_grm.json")
		r.Nil(err)
		r.Equal(3, len(info.engine.enginesInfo))
		r.Equal(3, len(info.schema.tableSchemas))
		engines, err := info.GetEngines([]string{"alice"}, "my_alice")
		r.Nil(err)
		r.Equal(1, len(engines))
		r.Equal("alice.com", engines[0].Endpoints[0])
		r.Equal("alice_credential", engines[0].Credential[0])
		tableSchema, err := info.GetTableMeta("tid1", "", "my_alice")
		r.Nil(err)
		r.Equal("d1", tableSchema.DbName)
		r.Equal("t1", tableSchema.TableName)
		r.Equal(2, len(tableSchema.Columns))
		r.Equal("c1", tableSchema.Columns[0].Name)
		r.Equal("long", tableSchema.Columns[0].Type)
		r.Equal("c2", tableSchema.Columns[1].Name)
		r.Equal("long", tableSchema.Columns[1].Type)

		engines, err = info.GetEngines([]string{"bob"}, "my_bob")
		r.Nil(err)
		r.Equal(1, len(engines))
		r.Equal("bob.com", engines[0].Endpoints[0])
		r.Equal("bob_credential", engines[0].Credential[0])
		_, err = info.GetTableMeta("tid2", "", "my_carol")
		r.NotNil(err)

		_, err = info.GetTableMeta("tid1", "", "my_carol")
		r.NotNil(err)

		isOwner, err := info.VerifyTableOwnership("tid1", "my_alice")
		r.Nil(err)
		r.Equal(true, isOwner)
		isOwner, err = info.VerifyTableOwnership("tid1", "my_bob")
		r.Nil(err)
		r.Equal(false, isOwner)
	}
}
