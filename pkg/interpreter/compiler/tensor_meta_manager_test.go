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
	"github.com/stretchr/testify/mock"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/types"
)

// Mock object definitions
type mockLogicalPlan struct {
	mock.Mock

	// dummy fields to satisfy the interface
	core.LogicalPlan
}

func (m *mockLogicalPlan) Schema() *expression.Schema {
	args := m.Called()
	return args.Get(0).(*expression.Schema)
}

func TestTensorManagerBasic(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()

	// Test initial state
	assert.Empty(t, tensorMetaManager.Tensors())
	assert.Equal(t, 0, tensorMetaManager.tensorNum)

	// Test tensor creation
	tensor1 := tensorMetaManager.CreateTensorMeta("test1", graph.NewPrimitiveDataType(pb.PrimitiveDataType_INT64))
	assert.NotNil(t, tensor1)
	assert.Equal(t, "test1", tensor1.Name)
	assert.Equal(t, pb.PrimitiveDataType_INT64, tensor1.DType.DType)
	assert.Len(t, tensorMetaManager.Tensors(), 1)

	// Test CreateTensorAs
	tensor2 := tensorMetaManager.CreateTensorMetaAs(tensor1)
	assert.NotNil(t, tensor2)
	assert.Equal(t, tensor1.DType, tensor2.DType)
	assert.Len(t, tensorMetaManager.Tensors(), 2)
}

func TestSetTensorsEquivalent(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()

	// Create similar tensors
	tensor1 := tensorMetaManager.CreateTensorMeta("test1", graph.NewPrimitiveDataType(pb.PrimitiveDataType_INT64))
	tensor2 := tensorMetaManager.CreateTensorMeta("test2", graph.NewPrimitiveDataType(pb.PrimitiveDataType_INT64))

	// Test setting equivalent tensors
	err := tensorMetaManager.setTensorsEquivalent(tensor1, tensor2)
	assert.NoError(t, err)
	assert.Equal(t, tensor1.ID, tensor2.ID)

	// Test error case with dissimilar tensors
	tensor3 := tensorMetaManager.CreateTensorMeta("test3", graph.NewPrimitiveDataType(pb.PrimitiveDataType_STRING))
	err = tensorMetaManager.setTensorsEquivalent(tensor1, tensor3)
	assert.Error(t, err)
}

func TestLPResultTableBasic(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	tensor := tensorMetaManager.CreateTensorMeta("test", graph.NewPrimitiveDataType(pb.PrimitiveDataType_INT64))

	// Mock LogicalPlan and Schema
	mockLP := &mockLogicalPlan{}
	mockSchema := &expression.Schema{
		Columns: []*expression.Column{
			{UniqueID: 123, RetType: &types.FieldType{}}},
	}
	mockLP.On("Schema").Return(mockSchema)

	// Test getting non-existent LogicalPlan result table
	_, err := tensorMetaManager.getLPResultTable(mockLP)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "has not set result tensors")

	resultTable := ResultTable{123: tensor}
	// Test setting result table with type check and wrong type
	err = tensorMetaManager.setLPResultTable(mockLP, resultTable, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "doesn't support type")
	// Test setting result table
	err = tensorMetaManager.setLPResultTable(mockLP, resultTable, false) // Skip type check
	assert.NoError(t, err)
	// Test getting result table
	retrieved, err := tensorMetaManager.getLPResultTable(mockLP)
	assert.NoError(t, err)
	assert.Equal(t, resultTable, retrieved)

	// Test duplicate setting error
	err = tensorMetaManager.setLPResultTable(mockLP, resultTable, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "has already set result tensors")
}

func TestTensorManagerGetLPSchemaTensors(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	tensor1 := tensorMetaManager.CreateTensorMeta("test1", graph.NewPrimitiveDataType(pb.PrimitiveDataType_INT64))
	tensor2 := tensorMetaManager.CreateTensorMeta("test2", graph.NewPrimitiveDataType(pb.PrimitiveDataType_STRING))

	// Mock LogicalPlan
	mockLP := &mockLogicalPlan{}
	mockSchema := &expression.Schema{
		Columns: []*expression.Column{{UniqueID: 123},
			{UniqueID: 456},
		},
	}
	mockLP.On("Schema").Return(mockSchema)
	resultTable := ResultTable{
		123: tensor1, 456: tensor2,
	}

	// Test getting tensors before setting result table
	_, err := tensorMetaManager.getLPSchemaTensors(mockLP)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "has not set result tensors")

	// Set result table
	err = tensorMetaManager.setLPResultTable(mockLP, resultTable, false)
	assert.NoError(t, err)

	// Test getting tensors in schema order
	tensors, err := tensorMetaManager.getLPSchemaTensors(mockLP)
	assert.NoError(t, err)
	assert.Len(t, tensors, 2)
	assert.Equal(t, tensor1, tensors[0])
	assert.Equal(t, tensor2, tensors[1])

	// Test missing column error
	mockSchema.Columns = []*expression.Column{{UniqueID: 789}}
	_, err = tensorMetaManager.getLPSchemaTensors(mockLP)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to find columnID")
}

func TestGetLPColumnTensor(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	tensor := tensorMetaManager.CreateTensorMeta("test", graph.NewPrimitiveDataType(pb.PrimitiveDataType_INT64))
	mockLP := &mockLogicalPlan{}
	resultTable := ResultTable{123: tensor}

	// Test getting tensor before setting result table
	_, err := tensorMetaManager.getLPColumnTensor(mockLP, &expression.Column{UniqueID: 123})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "has not set result tensors")

	// Set result table
	err = tensorMetaManager.setLPResultTable(mockLP, resultTable, false)
	assert.NoError(t, err)

	// Test getting tensor for specific column
	col := &expression.Column{UniqueID: 123}
	result, err := tensorMetaManager.getLPColumnTensor(mockLP, col)
	assert.NoError(t, err)
	assert.Equal(t, tensor, result)

	// Test getting non-existent column
	col2 := &expression.Column{UniqueID: 999}
	result, err = tensorMetaManager.getLPColumnTensor(mockLP, col2)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestCheckLPResultTable(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()

	mockLP := &mockLogicalPlan{}
	mockSchema := &expression.Schema{
		Columns: []*expression.Column{
			{UniqueID: 123, RetType: &types.FieldType{Tp: mysql.TypeLonglong}},
		},
	}
	mockLP.On("Schema").Return(mockSchema)

	tensor := tensorMetaManager.CreateTensorMeta("test", graph.NewPrimitiveDataType(pb.PrimitiveDataType_INT64))
	resultTable := ResultTable{123: tensor}

	// Test with valid result table
	err := tensorMetaManager.setLPResultTable(mockLP, resultTable, true)
	assert.NoError(t, err, "Should not error when setting result table")
	retrievedTable, err := tensorMetaManager.getLPResultTable(mockLP)
	assert.NoError(t, err)
	assert.Equal(t, resultTable, retrievedTable)
	mockLP.AssertExpectations(t)

	// Test check with invalid result table
	tensor2 := tensorMetaManager.CreateTensorMeta("test", graph.NewPrimitiveDataType(pb.PrimitiveDataType_STRING))
	resultTable[123] = tensor2
	err = tensorMetaManager.setLPResultTable(mockLP, resultTable, true)
	assert.Error(t, err)

	// Test with missing column
	mockSchema.Columns = []*expression.Column{
		{UniqueID: 456, RetType: &types.FieldType{Tp: mysql.TypeLonglong}},
	}
	err = tensorMetaManager.checkLPResultTable(mockLP, resultTable)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not contain column")

	// Test with unsupported type
	mockSchema.Columns = []*expression.Column{
		{UniqueID: 123, RetType: &types.FieldType{Tp: mysql.TypeBlob}},
	}
	err = tensorMetaManager.checkLPResultTable(mockLP, resultTable)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "doesn't support type")

	// Test with unexpected type
	mockSchema.Columns = []*expression.Column{
		{UniqueID: 123, RetType: &types.FieldType{Tp: mysql.TypeLonglong}},
	}
	err = tensorMetaManager.checkLPResultTable(mockLP, resultTable)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "doesn't match scheme type")
}

func TestGetTensorFromColumn(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	tensor := tensorMetaManager.CreateTensorMeta("test", graph.NewPrimitiveDataType(pb.PrimitiveDataType_INT64))
	rt := ResultTable{
		123: tensor,
	}

	// Test successful retrieval
	col := &expression.Column{UniqueID: 123}
	result, err := rt.getColumnTensorMeta(col)
	assert.NoError(t, err)
	assert.Equal(t, tensor, result)

	// Test column not found case
	col2 := &expression.Column{UniqueID: 456}
	result, err = rt.getColumnTensorMeta(col2)
	assert.Error(t, err)
	assert.Nil(t, result)
}
