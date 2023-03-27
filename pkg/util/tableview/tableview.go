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

package tableview

import (
	"fmt"

	"github.com/olekukonko/tablewriter"

	"github.com/secretflow/scql/pkg/proto-gen/scql"
)

// ConvertToTable convert tensors to table
func ConvertToTable(tensors []*scql.Tensor, table *tablewriter.Table) error {
	var headerNames []string
	for _, t := range tensors {
		headerNames = append(headerNames, t.Name)
	}
	table.SetHeader(headerNames)
	if len(tensors) == 0 {
		return nil
	}
	var rows int64
	switch x := tensors[0].Shape.Dim[0].Value.(type) {
	case *scql.TensorShape_Dimension_DimValue:
		rows = x.DimValue
		break
	case *scql.TensorShape_Dimension_DimParam:
		return fmt.Errorf("unexpected type:%T", x)
	}
	for i := int64(0); i < rows; i++ {
		var curRow []string
		for _, t := range tensors {
			switch x := t.Value.(type) {
			case *scql.Tensor_Ss:
				curRow = append(curRow, t.GetSs().Ss[i])
				break
			case *scql.Tensor_Fs:
				curRow = append(curRow, fmt.Sprint(t.GetFs().Fs[i]))
				break
			case *scql.Tensor_Bs:
				curRow = append(curRow, fmt.Sprint(t.GetBs().Bs[i]))
				break
			case *scql.Tensor_Is:
				curRow = append(curRow, fmt.Sprint(t.GetIs().Is[i]))
				break
			case *scql.Tensor_I64S:
				curRow = append(curRow, fmt.Sprint(t.GetI64S().I64S[i]))
				break
			default:
				return fmt.Errorf("unsupported type:%T", x)
			}
		}
		table.Append(curRow)
	}
	return nil
}
