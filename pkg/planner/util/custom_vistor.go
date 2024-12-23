// Copyright 2024 Ant Group Co., Ltd.
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

package util

import (
	"github.com/secretflow/scql/pkg/parser/ast"

	"github.com/secretflow/scql/pkg/parser"
)

// selectIntoPartyCollector collects into parties from SelectStmt which has INTO clause.
type selectIntoPartyCollector struct {
	parties []string
}

// Enter implements Visitor interface.
func (collector *selectIntoPartyCollector) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.SelectStmt:
		if node.SelectIntoOpt != nil {
			for _, partyFile := range node.SelectIntoOpt.PartyFiles {
				if partyFile.PartyCode != "" {
					collector.parties = append(collector.parties, partyFile.PartyCode)
				}
			}
		}
		return in, true
	}
	return in, false
}

// Leave implements Visitor interface.
func (collector *selectIntoPartyCollector) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

// NOTE: collect parties in SELECT INTO clause, but the empty party will be ignored.
//
//	e.g: no party will be collected from "select xxx into outfile '/path/to/file';"
func CollectIntoParties(sql string) ([]string, error) {
	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	stmt := stmts[0]
	c := &selectIntoPartyCollector{}
	stmt.Accept(c)

	return c.parties, nil

}
