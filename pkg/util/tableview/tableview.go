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
	case *scql.TensorShape_Dimension_DimParam:
		return fmt.Errorf("unexpected type:%T", x)
	}
	for i := int64(0); i < rows; i++ {
		var curRow []string
		for _, t := range tensors {
			if len(t.DataValidity) > 0 && !t.DataValidity[i] {
				curRow = append(curRow, "null")
				continue
			}
			switch t.ElemType {
			case scql.PrimitiveDataType_STRING, scql.PrimitiveDataType_DATETIME:
				curRow = append(curRow, t.GetStringData()[i])
			case scql.PrimitiveDataType_FLOAT32:
				curRow = append(curRow, fmt.Sprint(t.GetFloatData()[i]))
			case scql.PrimitiveDataType_FLOAT64:
				curRow = append(curRow, fmt.Sprint(t.GetDoubleData()[i]))
			case scql.PrimitiveDataType_BOOL:
				curRow = append(curRow, fmt.Sprint(t.GetBoolData()[i]))
			case scql.PrimitiveDataType_INT8, scql.PrimitiveDataType_INT16, scql.PrimitiveDataType_INT32:
				curRow = append(curRow, fmt.Sprint(t.GetInt32Data()[i]))
			case scql.PrimitiveDataType_INT64, scql.PrimitiveDataType_TIMESTAMP:
				curRow = append(curRow, fmt.Sprint(t.GetInt64Data()[i]))
			default:
				return fmt.Errorf("unsupported type:%T", t.ElemType)
			}
		}
		table.Append(curRow)
	}
	return nil
}
