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
	"strings"

	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/format"
)

type queryKind int

const (
	queryKindOther queryKind = iota
	queryKindDQL
	queryKindExplainDQL
)

type queryClassification struct {
	kind          queryKind
	explainSQL    string
	explainFormat string
}

func (qc *queryClassification) isDQL() bool {
	return qc.kind == queryKindDQL
}

func (qc *queryClassification) isExplainDQL() bool {
	return qc.kind == queryKindExplainDQL
}

func classifyQuery(sql string) (*queryClassification, error) {
	result := &queryClassification{kind: queryKindOther}
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, err
	}

	switch node := stmt.(type) {
	case *ast.SelectStmt, *ast.UnionStmt:
		result.kind = queryKindDQL
		return result, nil
	case *ast.ExplainStmt:
		switch node.Stmt.(type) {
		case *ast.SelectStmt, *ast.UnionStmt:
			builder := strings.Builder{}
			restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &builder)
			if err := node.Stmt.Restore(restoreCtx); err != nil {
				return nil, err
			}
			result.kind = queryKindExplainDQL
			result.explainSQL = builder.String()
			result.explainFormat = node.Format
			return result, nil
		default:
			return result, nil
		}
	default:
		return result, nil
	}
}
