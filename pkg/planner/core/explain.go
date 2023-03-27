// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"fmt"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/expression/aggregation"
)

func formatWindowFuncDescs(buffer *bytes.Buffer, descs []*aggregation.WindowFuncDesc, schema *expression.Schema) *bytes.Buffer {
	winFuncStartIdx := len(schema.Columns) - len(descs)
	for i, desc := range descs {
		if i != 0 {
			buffer.WriteString(", ")
		}
		fmt.Fprintf(buffer, "%v->%v", desc, schema.Columns[winFuncStartIdx+i])
	}
	return buffer
}
