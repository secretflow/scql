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
	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/parser/ast"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

type CCLLevel int32

const (
	Plain         CCLLevel = CCLLevel(proto.SecurityConfig_ColumnControl_PLAINTEXT)
	Join          CCLLevel = CCLLevel(proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN)
	GroupBy       CCLLevel = CCLLevel(proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_GROUP_BY)
	Aggregate     CCLLevel = CCLLevel(proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_AGGREGATE)
	Compare       CCLLevel = CCLLevel(proto.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_COMPARE)
	Encrypt       CCLLevel = CCLLevel(proto.SecurityConfig_ColumnControl_ENCRYPTED_ONLY)
	AsJoinPayload CCLLevel = CCLLevel(proto.SecurityConfig_ColumnControl_PLAINTEXT_AS_JOIN_PAYLOAD)
	// if CCLLevel of column/Tensor was setting unknown just meaning it can't be used in ccl infer
	Unknown CCLLevel = CCLLevel(proto.SecurityConfig_ColumnControl_UNKNOWN)
	Rank    CCLLevel = CCLLevel(proto.SecurityConfig_ColumnControl_REVEAL_RANK)
)

var isCompareOpMap = map[string]bool{
	operator.OpNameGreater:      true,
	operator.OpNameGreaterEqual: true,
	operator.OpNameLess:         true,
	operator.OpNameLessEqual:    true,
	operator.OpNameEqual:        true,
	operator.OpNameNotEqual:     true,
}

var isCompareAstFuncMap = map[string]bool{
	ast.GT:      true,
	ast.GE:      true,
	ast.LT:      true,
	ast.LE:      true,
	ast.EQ:      true,
	ast.NE:      true,
	ast.GeoDist: true,
}

var isRankWindowFuncMap = map[string]bool{
	ast.WindowFuncRowNumber: true,
}
