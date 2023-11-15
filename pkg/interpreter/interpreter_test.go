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

package interpreter

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

// TODO: rewrite it to make test cases more maintainable
func TestCompile(t *testing.T) {
	assert := require.New(t)
	intr := NewInterpreter()
	for _, tc := range testCases {
		compiledPlan, err := intr.Compile(context.Background(), tc.req)

		if tc.ok {
			assert.NoError(err)
			assert.Equal(tc.workPartyNum, len(compiledPlan.Parties))
			fmt.Printf("%s\n", compiledPlan.String())
			// TODO: check plan
		} else {
			assert.Error(err)
		}
	}
}

var testCases = []compileTestCase{
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT ta.credit_rank, COUNT(*) as cnt, AVG(ta.income) as avg_income, AVG(tb.order_amount) as avg_amount FROM ta INNER JOIN tb ON ta.ID = tb.ID WHERE ta.age >= 20 AND ta.age <= 30 AND tb.is_active = 1 GROUP BY ta.credit_rank",
			DbName: "",
			Issuer: &proto.PartyId{
				Code: "alice",
			},
			IssuerAsParticipant: true,
			SecurityConf: &proto.SecurityConfig{
				ColumnControlList: []*proto.SecurityConfig_ColumnControl{
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "ID",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "credit_rank",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "income",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "age",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "ID",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_GROUP_BY,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "credit_rank",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_AGGREGATE,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "income",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_COMPARE,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "age",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "ID",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "order_amount",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "is_active",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "ID",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_AGGREGATE,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "order_amount",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_COMPARE,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "is_active",
					},
				},
			},
			Catalog: &proto.Catalog{
				Tables: []*proto.TableEntry{
					{
						TableName: "ta",
						Columns: []*proto.TableEntry_Column{
							{
								Name: "ID",
								Type: "string",
							},
							{
								Name: "credit_rank",
								Type: "int",
							},
							{
								Name: "income",
								Type: "int",
							},
							{
								Name: "age",
								Type: "int",
							},
						},
						IsView:   false,
						RefTable: "alice.user_credit",
						DbType:   "mysql",
						Owner: &proto.PartyId{
							Code: "alice",
						},
					},
					{
						TableName: "tb",
						Columns: []*proto.TableEntry_Column{
							{
								Name: "ID",
								Type: "string",
							},
							{
								Name: "order_amount",
								Type: "double",
							},
							{
								Name: "is_active",
								Type: "int",
							},
						},
						IsView:   false,
						RefTable: "bob.user_stats",
						DbType:   "mysql",
						Owner: &proto.PartyId{
							Code: "bob",
						},
					},
				},
			},
			// TODO: add RuntimeConfig
		},
		ok:           true,
		workPartyNum: 2,
	},
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT tb.data as avg_amount FROM tb into outfile party_code 'alice' '/data/output11.txt' fields terminated BY ','",
			DbName: "",
			Issuer: &proto.PartyId{
				Code: "alice",
			},
			IssuerAsParticipant: false,
			SecurityConf: &proto.SecurityConfig{
				ColumnControlList: []*proto.SecurityConfig_ColumnControl{
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "data",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "data",
					},
				},
			},
			Catalog: &proto.Catalog{
				Tables: []*proto.TableEntry{
					{
						TableName: "tb",
						Columns: []*proto.TableEntry_Column{
							{
								Name: "data",
								Type: "string",
							},
						},
						IsView:   false,
						RefTable: "bob.user_stats",
						DbType:   "mysql",
						Owner: &proto.PartyId{
							Code: "bob",
						},
					},
				},
			},
			// TODO: add RuntimeConfig
		},
		ok:           true,
		workPartyNum: 2,
	},
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT tb.data as avg_amount FROM tb into outfile party_code 'bob' '/data/output11.txt' fields terminated BY ','",
			DbName: "",
			Issuer: &proto.PartyId{
				Code: "bob",
			},
			IssuerAsParticipant: false,
			SecurityConf: &proto.SecurityConfig{
				ColumnControlList: []*proto.SecurityConfig_ColumnControl{
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "data",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "data",
					},
				},
			},
			Catalog: &proto.Catalog{
				Tables: []*proto.TableEntry{
					{
						TableName: "tb",
						Columns: []*proto.TableEntry_Column{
							{
								Name: "data",
								Type: "string",
							},
						},
						IsView:   false,
						RefTable: "bob.user_stats",
						DbType:   "mysql",
						Owner: &proto.PartyId{
							Code: "bob",
						},
					},
				},
			},
			// TODO: add RuntimeConfig
		},
		ok:           true,
		workPartyNum: 1,
	},
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT ta.credit_rank, COUNT(*) as cnt, AVG(ta.income) as avg_income, AVG(tb.order_amount) as avg_amount FROM ta INNER JOIN tb ON ta.ID = tb.ID WHERE ta.age >= 20 AND ta.age <= 30 AND tb.is_active = 1 GROUP BY ta.credit_rank",
			DbName: "demo",
			Issuer: &proto.PartyId{
				Code: "alice",
			},
			IssuerAsParticipant: true,
			SecurityConf: &proto.SecurityConfig{
				ColumnControlList: []*proto.SecurityConfig_ColumnControl{
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "demo",
						TableName:    "ta",
						ColumnName:   "ID",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "demo",
						TableName:    "ta",
						ColumnName:   "credit_rank",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "demo",
						TableName:    "ta",
						ColumnName:   "income",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "demo",
						TableName:    "ta",
						ColumnName:   "age",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "demo",
						TableName:    "ta",
						ColumnName:   "ID",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_GROUP_BY,
						DatabaseName: "demo",
						TableName:    "ta",
						ColumnName:   "credit_rank",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_AGGREGATE,
						DatabaseName: "demo",
						TableName:    "ta",
						ColumnName:   "income",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_COMPARE,
						DatabaseName: "demo",
						TableName:    "ta",
						ColumnName:   "age",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "demo",
						TableName:    "tb",
						ColumnName:   "ID",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "demo",
						TableName:    "tb",
						ColumnName:   "order_amount",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "demo",
						TableName:    "tb",
						ColumnName:   "is_active",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "demo",
						TableName:    "tb",
						ColumnName:   "ID",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_AGGREGATE,
						DatabaseName: "demo",
						TableName:    "tb",
						ColumnName:   "order_amount",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_COMPARE,
						DatabaseName: "demo",
						TableName:    "tb",
						ColumnName:   "is_active",
					},
				},
			},
			Catalog: &proto.Catalog{
				Tables: []*proto.TableEntry{
					{
						TableName: "demo.ta",
						Columns: []*proto.TableEntry_Column{
							{
								Name: "ID",
								Type: "string",
							},
							{
								Name: "credit_rank",
								Type: "int",
							},
							{
								Name: "income",
								Type: "int",
							},
							{
								Name: "age",
								Type: "int",
							},
						},
						IsView:   false,
						RefTable: "alice.user_credit",
						DbType:   "mysql",
						Owner: &proto.PartyId{
							Code: "alice",
						},
					},
					{
						TableName: "demo.tb",
						Columns: []*proto.TableEntry_Column{
							{
								Name: "ID",
								Type: "string",
							},
							{
								Name: "order_amount",
								Type: "double",
							},
							{
								Name: "is_active",
								Type: "int",
							},
						},
						IsView:   false,
						RefTable: "bob.user_stats",
						DbType:   "mysql",
						Owner: &proto.PartyId{
							Code: "bob",
						},
					},
				},
			},
			// TODO: add RuntimeConfig
		},
		ok:           true,
		workPartyNum: 2,
	},
}

type compileTestCase struct {
	req          *proto.CompileQueryRequest
	workPartyNum int
	ok           bool
}
