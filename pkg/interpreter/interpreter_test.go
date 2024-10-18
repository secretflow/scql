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

var commonSecurityConf = &proto.SecurityConfig{
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
			PartyCode:    "bob",
			Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
			DatabaseName: "",
			TableName:    "ta",
			ColumnName:   "ID",
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
			Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
			DatabaseName: "",
			TableName:    "tb",
			ColumnName:   "order_amount",
		},
		{
			PartyCode:    "alice",
			Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AS_JOIN_PAYLOAD,
			DatabaseName: "",
			TableName:    "tb",
			ColumnName:   "is_active",
		},
	},
}

var commonCatalog = &proto.Catalog{
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
}

var testCases = []compileTestCase{
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT ROW_NUMBER() OVER(PARTITION BY tb.is_active ORDER BY ta.age, tb.order_amount) as num FROM ta INNER JOIN tb ON ta.ID = tb.ID",
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
						Visibility:   proto.SecurityConfig_ColumnControl_REVEAL_RANK,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "order_amount",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_REVEAL_RANK,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "is_active",
					},
				},
			},
			Catalog:     commonCatalog,
			CompileOpts: &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
		},
		ok:           true,
		workPartyNum: 2,
	},
	{
		req: &proto.CompileQueryRequest{
			Query: `SELECT t.num, t.num1, t.num2, t.num3 FROM (SELECT
				ROW_NUMBER() OVER(PARTITION BY ta.ID ORDER BY tb.order_amount) as num,
					ROW_NUMBER() OVER(PARTITION BY CASE WHEN ta.ID = 1 THEN 1 ELSE 0 END ORDER BY CASE WHEN tb.order_amount > 1 THEN 1 ELSE 0 END) as num1,
					ROW_NUMBER() OVER(PARTITION BY tb.ID ORDER BY tb.order_amount) as num2,
					ROW_NUMBER() OVER(PARTITION BY ta.ID, ta.credit_rank ORDER BY tb.order_amount) as num3
					FROM ta INNER JOIN tb ON ta.ID = tb.ID) AS t WHERE t.num = 1`,
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
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AS_JOIN_PAYLOAD,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "credit_rank",
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
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "ID",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_REVEAL_RANK,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "order_amount",
					},
				},
			},
			Catalog:     commonCatalog,
			CompileOpts: &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
		},
		ok:           true,
		workPartyNum: 2,
	},
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT ROW_NUMBER() OVER(PARTITION BY tb.ID ORDER BY tb.order_amount) as num, tb.order_amount FROM ta INNER JOIN tb ON ta.ID = tb.ID",
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
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "ID",
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
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "ID",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_REVEAL_RANK,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "order_amount",
					},
				},
			},
			Catalog:     commonCatalog,
			CompileOpts: &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
		},
		ok:           false,
		workPartyNum: 2,
	},
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT ROW_NUMBER() OVER(PARTITION BY tb.ID ORDER BY tb.order_amount) as num FROM ta INNER JOIN tb ON ta.ID = tb.ID",
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
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "ID",
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
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "ID",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_REVEAL_RANK,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "order_amount",
					},
				},
			},
			Catalog:     commonCatalog,
			CompileOpts: &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
		},
		ok:           true,
		workPartyNum: 2,
	},
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT ta.credit_rank, tb.order_amount, geodist(ta.credit_rank, ta.credit_rank, tb.order_amount, tb.order_amount) FROM ta INNER JOIN tb ON ta.ID = tb.ID",
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
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "ID",
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
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "ID",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_COMPARE,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "order_amount",
					},
				},
			},
			Catalog:     commonCatalog,
			CompileOpts: &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
		},
		ok:           false,
		workPartyNum: 2,
	},
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT ta.credit_rank, geodist(ta.credit_rank, ta.credit_rank, tb.order_amount, tb.order_amount) FROM ta INNER JOIN tb ON ta.ID = tb.ID",
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
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "ID",
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
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "ID",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_COMPARE,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "order_amount",
					},
				},
			},
			Catalog:     commonCatalog,
			CompileOpts: &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
		},
		ok:           true,
		workPartyNum: 2,
	},
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT geodist(ta.credit_rank, ta.credit_rank, ta.credit_rank, ta.credit_rank, 6300) FROM ta INNER JOIN tb ON ta.ID = tb.ID",
			DbName: "",
			Issuer: &proto.PartyId{
				Code: "alice",
			},
			IssuerAsParticipant: true,
			SecurityConf:        commonSecurityConf,
			Catalog:             commonCatalog,
			CompileOpts:         &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
		},
		ok:           true,
		workPartyNum: 2,
	},
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT geodist(ta.credit_rank, ta.credit_rank, ta.credit_rank, ta.credit_rank) FROM ta INNER JOIN tb ON ta.ID = tb.ID",
			DbName: "",
			Issuer: &proto.PartyId{
				Code: "alice",
			},
			IssuerAsParticipant: true,
			SecurityConf:        commonSecurityConf,
			Catalog:             commonCatalog,
			CompileOpts:         &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
		},
		ok:           true,
		workPartyNum: 2,
	},
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT COS(ta.credit_rank) AS credit_rank_cos, SIN(tb.order_amount) AS order_amount_sin, ACOS(ta.credit_rank) AS credit_rank_acos FROM ta INNER JOIN tb ON ta.ID = tb.ID",
			DbName: "",
			Issuer: &proto.PartyId{
				Code: "alice",
			},
			IssuerAsParticipant: true,
			SecurityConf:        commonSecurityConf,
			Catalog:             commonCatalog,
			CompileOpts:         &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
			// TODO: add RuntimeConfig
		},
		ok:           true,
		workPartyNum: 2,
	},
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT SIN(credit_rank) AS sin_credit_rank FROM ta",
			DbName: "",
			Issuer: &proto.PartyId{
				Code: "alice",
			},
			IssuerAsParticipant: true,
			SecurityConf:        commonSecurityConf,
			Catalog:             commonCatalog,
			CompileOpts:         &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
			// TODO: add RuntimeConfig
		},
		ok:           true,
		workPartyNum: 1,
	},
	{
		req: &proto.CompileQueryRequest{
			// is_active is not visible for alice
			Query:  "SELECT SIN(tb.is_active) AS sin_is_active FROM ta ON tb ON ta.ID = tb.ID",
			DbName: "",
			Issuer: &proto.PartyId{
				Code: "alice",
			},
			IssuerAsParticipant: true,
			SecurityConf:        commonSecurityConf,
			Catalog:             commonCatalog,
			CompileOpts:         &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
			// TODO: add RuntimeConfig
		},
		ok:           false,
		workPartyNum: 2,
	},
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
			Catalog:     commonCatalog,
			CompileOpts: &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
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
			CompileOpts: &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
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
			CompileOpts: &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
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
			CompileOpts: &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
			// TODO: add RuntimeConfig
		},
		ok:           true,
		workPartyNum: 2,
	},
	// VIEW test
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT credit_rank from view1",
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
						ColumnName:   "credit_rank",
					},
				},
			},
			Catalog: &proto.Catalog{
				Tables: []*proto.TableEntry{
					{
						TableName: "demo.ta",
						Columns: []*proto.TableEntry_Column{
							{
								Name: "credit_rank",
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
						TableName: "demo.view1",
						Columns: []*proto.TableEntry_Column{
							{
								Name:            "credit_rank",
								Type:            "int",
								OrdinalPosition: 0,
							},
						},
						IsView:       true,
						SelectString: "SELECT credit_rank from demo.ta",
					},
				},
			},
			CompileOpts: &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
			// TODO: add RuntimeConfig
		},
		ok:           true,
		workPartyNum: 1,
	},
	// ref table without db_name
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
						RefTable: "user_credit",
						DbType:   "csvdb",
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
						RefTable: "user_stats",
						DbType:   "csvdb",
						Owner: &proto.PartyId{
							Code: "bob",
						},
					},
				},
			},
			CompileOpts: &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
			// TODO: add RuntimeConfig
		},
		ok:           true,
		workPartyNum: 2,
	},
	{
		req: &proto.CompileQueryRequest{
			Query:  "SELECT tb.id, tb.tag FROM ta inner join tb on ta.id = tb.id",
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
						ColumnName:   "id",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "",
						TableName:    "ta",
						ColumnName:   "id",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "id",
					},
					{
						PartyCode:    "bob",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "tag",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "id",
					},
					{
						PartyCode:    "alice",
						Visibility:   proto.SecurityConfig_ColumnControl_PLAINTEXT_AS_JOIN_PAYLOAD,
						DatabaseName: "",
						TableName:    "tb",
						ColumnName:   "tag",
					},
				},
			},
			Catalog: &proto.Catalog{
				Tables: []*proto.TableEntry{
					{
						TableName: "ta",
						Columns: []*proto.TableEntry_Column{
							{
								Name: "id",
								Type: "string",
							},
						},
						IsView:   false,
						RefTable: "alice.user_info",
						DbType:   "mysql",
						Owner: &proto.PartyId{
							Code: "alice",
						},
					},
					{
						TableName: "tb",
						Columns: []*proto.TableEntry_Column{
							{
								Name: "id",
								Type: "string",
							},
							{
								Name: "tag",
								Type: "string",
							},
						},
						IsView:   false,
						RefTable: "bob.client_info",
						DbType:   "mysql",
						Owner: &proto.PartyId{
							Code: "bob",
						},
					},
				},
			},
			CompileOpts: &proto.CompileOptions{SecurityCompromise: &proto.SecurityCompromiseConfig{GroupByThreshold: 4}},
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
