// Copyright 2025 Ant Group Co., Ltd.
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

package compiler

import (
	"fmt"

	"github.com/secretflow/scql/pkg/interpreter/graph"
)

// TODO: add flag to mark temporary tensor
type TensorMeta struct {
	ID            int
	Name          string
	DType         *graph.DataType
	IsConstScalar bool
}

func (t *TensorMeta) UniqueName() string {
	return fmt.Sprintf("%s.%d", t.Name, t.ID)
}

func (t *TensorMeta) String() string {
	return t.UniqueName()
}
