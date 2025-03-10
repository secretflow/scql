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
	"fmt"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser/ast"
)

func extractEQColumns(exp *expression.ScalarFunction) ([]*expression.Column, error) {
	if exp.FuncName.L != ast.EQ || len(exp.GetArgs()) != 2 {
		return nil, fmt.Errorf("extractEQColumns: unsupported eq condition %v", exp)
	}
	left, right := exp.GetArgs()[0], exp.GetArgs()[1]
	leftCol, ok1 := left.(*expression.Column)
	rightCol, ok2 := right.(*expression.Column)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("extractEQColumns: unsupported eq condition %v", exp)
	}
	return []*expression.Column{leftCol, rightCol}, nil
}

// stable merge
func mergeTensors(ts1, ts2 []*graph.Tensor) []*graph.Tensor {
	result := make([]*graph.Tensor, 0, len(ts1)+len(ts2))
	result = append(result, ts1...)
	tsMap := make(map[int]interface{}, len(ts1))
	for _, t := range ts1 {
		tsMap[t.ID] = struct{}{}
	}
	for _, t := range ts2 {
		if _, ok := tsMap[t.ID]; !ok {
			result = append(result, t)
		}
	}
	return result
}
