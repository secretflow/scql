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

package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/util/mock"
)

func TestCollectCCLForParty(t *testing.T) {
	r := require.New(t)

	store, err := storage.NewMemoryStorage()
	r.NoError(err)
	r.NoError(mock.MockStorage(store)) // user already create here

	tableSchemas := []*scql.TableEntry{
		{
			TableName: "test.table_1",
			Columns: []*scql.TableEntry_Column{
				{
					Name: "column1_1",
					Type: "int",
				},
				{
					Name: "column1_2",
					Type: "int",
				},
			},
		},
	}
	ccl, err := collectCCLForParty(store, "alice", tableSchemas)
	r.NoError(err)
	r.Equal(len(ccl), 2)
	r.Equal(ccl[0].PartyCode, "alice")
	r.Equal(ccl[0].ColumnName, "column1_1")
	// not found in column level, use table level
	r.Equal(ccl[0].Visibility, scql.SecurityConfig_ColumnControl_PLAINTEXT)

	r.Equal(ccl[1].PartyCode, "alice")
	r.Equal(ccl[1].ColumnName, "column1_2")
	// use column level
	r.Equal(ccl[1].Visibility, scql.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_COMPARE)
}

func TestQueryTablesOwner(t *testing.T) {
	r := require.New(t)

	store, err := storage.NewMemoryStorage()
	r.NoError(err)
	r.NoError(mock.MockStorage(store)) // user already create here

	owners, err := storage.QueryTablesOwner(store, "test", []string{"table_1", "table_2", "table_3"})
	r.NoError(err)
	r.ElementsMatch(owners, []string{"alice", "bob"})
}

func TestQueryUserPartyCode(t *testing.T) {
	t.Run("userNotExists", func(t *testing.T) {
		r := require.New(t)

		store, err := storage.NewMemoryStorage()
		r.NoError(err)
		r.NoError(mock.MockStorage(store)) // user already create here

		_, err = storage.QueryUserPartyCode(store, "some_user_not_exists", "%")
		r.Error(err)
	})
	t.Run("normalCases", func(t *testing.T) {
		r := require.New(t)

		store, err := storage.NewMemoryStorage()
		r.NoError(err)
		r.NoError(mock.MockStorage(store)) // user already create here

		partyCode, err := storage.QueryUserPartyCode(store, "root", "%")
		r.Equal(err.Error(), "there is no party code for user root@%")
		r.Equal(partyCode, "")

		partyCode, err = storage.QueryUserPartyCode(store, "alice", "%")
		r.NoError(err)
		r.Equal(partyCode, "alice")
	})
}

func TestQueryTableSchemas(t *testing.T) {
	t.Run("tableNotExists", func(t *testing.T) {
		r := require.New(t)

		store, err := storage.NewMemoryStorage()
		r.NoError(err)
		r.NoError(mock.MockStorage(store)) // user already create here

		_, err = storage.QueryTableSchemas(store, "da", []string{"t1"})
		r.EqualError(err, "queryTableSchemas: table da.t1 not exists")
	})

	t.Run("normalCases", func(t *testing.T) {
		r := require.New(t)

		store, err := storage.NewMemoryStorage()
		r.NoError(err)
		r.NoError(mock.MockStorage(store)) // user already create here

		tableSchemas, err := storage.QueryTableSchemas(store, "test", []string{"table_1", "table_2"})
		r.NoError(err)
		r.Equal(len(tableSchemas), 2)
		r.Equal(tableSchemas[0].DbName, "test")
		r.Equal(tableSchemas[1].DbName, "test")
		r.NotEqual(tableSchemas[0].TableName, tableSchemas[1].TableName)
		r.Contains([]string{"table_1", "table_2"}, tableSchemas[0].TableName)
		r.Contains([]string{"table_1", "table_2"}, tableSchemas[1].TableName)
		r.Equal(3, len(tableSchemas[0].Columns))
		r.Equal(2, len(tableSchemas[1].Columns))
	})

}
