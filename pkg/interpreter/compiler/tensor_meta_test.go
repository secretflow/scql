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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/secretflow/scql/pkg/interpreter/graph"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestTensorMeta(t *testing.T) {
	meta := &TensorMeta{
		ID:            1,
		Name:          "test_tensor",
		DType:         graph.NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT32),
		IsConstScalar: true,
	}

	// Test UniqueName
	assert.Equal(t, "test_tensor.1", meta.UniqueName())

	// Test String
	assert.Equal(t, "test_tensor.1", meta.String())
}
