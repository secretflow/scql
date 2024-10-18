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

package operator

import (
	"fmt"

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

const (
	version = 1
)

var AllOpDef []*OperatorDef

var BinaryOps = []string{
	OpNameLess,
	OpNameLessEqual,
	OpNameGreater,
	OpNameGreaterEqual,
	OpNameEqual,
	OpNameNotEqual,
	OpNameLogicalAnd,
	OpNameLogicalOr,
	OpNameAdd,
	OpNameMinus,
	OpNameMul,
	OpNameDiv,
	OpNameIntDiv,
	OpNameMod,
}

var UnaryOps = []string{
	OpNameNot,
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func registerAllOpDef() {
	const (
		T  = "T"
		T1 = "T1"
		T2 = "T2"
		T3 = "T3"
	)

	var statusPrivate = []proto.TensorStatus{proto.TensorStatus_TENSORSTATUS_PRIVATE}
	var statusSecret = []proto.TensorStatus{proto.TensorStatus_TENSORSTATUS_SECRET}
	var statusPublic = []proto.TensorStatus{proto.TensorStatus_TENSORSTATUS_PUBLIC}
	var statusPrivateOrPublic = []proto.TensorStatus{proto.TensorStatus_TENSORSTATUS_PUBLIC, proto.TensorStatus_TENSORSTATUS_PRIVATE}
	var statusPrivateOrSecret = []proto.TensorStatus{proto.TensorStatus_TENSORSTATUS_PRIVATE, proto.TensorStatus_TENSORSTATUS_SECRET}
	var statusSecretOrPublic = []proto.TensorStatus{proto.TensorStatus_TENSORSTATUS_SECRET, proto.TensorStatus_TENSORSTATUS_PUBLIC}
	var statusPrivateOrSecretOrPublic = []proto.TensorStatus{proto.TensorStatus_TENSORSTATUS_PUBLIC, proto.TensorStatus_TENSORSTATUS_PRIVATE, proto.TensorStatus_TENSORSTATUS_SECRET}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameRunSQL)
		opDef.SetStreamingType(SinkOp)
		opDef.AddOutput("Out", "Result tensors of the SQL statement.", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddAttribute(SqlAttr, "SQL statement")
		opDef.AddAttribute(TableRefsAttr, "tables referenced by query")
		opDef.SetDefinition("Definition: Run a SQL statement and return a list of tensors in private status")
		opDef.SetParamTypeConstraint(T, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNamePublish)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("In", "Tensors to be published.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddOutput("Out", "Published name of input tensors. Tensors are in TensorOption VALUE.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.SetDefinition("Definition: This operator publishes the DAG results.")
		opDef.SetParamTypeConstraint(T, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	for _, opName := range BinaryOps {
		opDef := &OperatorDef{}
		opDef.SetName(opName)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("Left", "First operand.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddInput("Right", "Second operand.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T1)
		opDef.AddOutput("Out", "Output Tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T2)
		opDef.SetDefinition(fmt.Sprintf("Definition: Out = Left `%s` Right", opName))
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecretOrPublic)
		opDef.SetParamTypeConstraint(T1, statusPrivateOrSecretOrPublic)
		opDef.SetParamTypeConstraint(T2, statusPrivateOrSecret)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameMakePublic)
		// make public is not a sink op, but for now, we don't support concat share tensors
		// TODO: fix sink op type
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("In", "Input tensors.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T1)
		opDef.AddOutput("Out", "Output tensors.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T2)
		opDef.SetDefinition("Definition: Convert In tensor from share/private status to public status.")
		opDef.SetParamTypeConstraint(T1, statusPrivateOrSecret)
		opDef.SetParamTypeConstraint(T2, statusPublic)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameMakePrivate)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("In", "Input tensors.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T1)
		opDef.AddOutput("Out", "Output tensors.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T2)
		opDef.AddAttribute(RevealToAttr, "List of parties to see the private data. If it is revealed to one party only, the other party also needs to run the op, but does not have an output. Only the reveal_to party gets the output.")
		opDef.SetDefinition("Definition: Convert In tensor from share status to private status.")
		opDef.SetParamTypeConstraint(T1, statusSecretOrPublic)
		opDef.SetParamTypeConstraint(T2, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameMakeShare)
		// make share is not a sink op, but for now, we don't support concat share tensors
		// TODO: fix sink op type
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("In", "Input tensors.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T1)
		opDef.AddOutput("Out", "Output tensors.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T2)
		opDef.SetDefinition("Definition: Convert In tensor from private status to share status.")
		opDef.SetParamTypeConstraint(T1, statusPrivate)
		opDef.SetParamTypeConstraint(T2, statusSecret)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameJoin)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("Left", "Left vector(shape [M][1])",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T1)
		opDef.AddInput("Right", "Right vector(shape [N][1])",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T1)
		opDef.AddOutput("LeftJoinIndex", "Joined rows index for left vector(shape [K][1])",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T2)
		opDef.AddOutput("RightJoinIndex", "Joined rows index for right vector(shape [K][1])",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T2)
		opDef.AddAttribute(InputPartyCodesAttr, "List of parties the inputs belong to([PartyCodeLeft, PartyCodeRight]).")
		// TODO(xiaoyuan) support outer join later
		opDef.AddAttribute(JoinTypeAttr, "Int64. 0: inner join; 1: left join; 2: right join;")
		opDef.AddDefaultAttributeValue(JoinTypeAttr, CreateIntAttribute(0))
		opDef.AddAttribute(PsiAlgorithmAttr, "Choose PSI join algorithm, Int64. 0: Auto; 1: Ecdh; 2: Oprf;")
		opDef.AddDefaultAttributeValue(PsiAlgorithmAttr, CreateIntAttribute(0))
		opDef.SetDefinition(`Definition: Create Join Index based on EQ-Join, return result's corresponding rows index in the original input.
Example:
` + "\n```python" + `
// inner join example
Left = {4,4,3,2,1} // shape:[M=5]
Right = {1,3,4,5} // shape: [N=4]
join_type = 0
LeftJoinIndex = {4,2,0,1}  // shape:[K=4], rows after applied filter eq-join-list={1,3,4,4}
RightJoinIndex = {0,1,2,2} // shape:[K=4], rows after applied filter eq-join-list={1,3,4,4}

// Left join example
Left = {4,4,3,2,1} // shape:[M=5]
Right = {1,3,4,5} // shape: [N=4]
join_type = 1
LeftJoinIndex = {4,2,0,1,3}  // shape:[K=5], rows after applied filter eq-join-list={1,3,4,4,2}
RightJoinIndex = {0,1,2,2,null} // shape:[K=5], rows after applied filter eq-join-list={1,3,4,4,null}

// Right join example
Left = {4,4,3,2,1} // shape:[M=5]
Right = {1,3,4,5} // shape: [N=4]
join_type = 2
LeftJoinIndex = {4,2,0,1,null}  // shape:[K=5], rows after applied filter eq-join-list={1,3,4,4,null}
RightJoinIndex = {0,1,2,2,3} // shape:[K=5], rows after applied filter eq-join-list={1,3,4,4,5}

` + "```\n")
		opDef.SetParamTypeConstraint(T1, statusPrivate)
		opDef.SetParamTypeConstraint(T2, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameFilterByIndex)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("RowsIndexFilter", "Rows index filter vector(shape [K][1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddInput("Data", "Input data tensor(shape [M][N]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddOutput("Out", "Output data tensor(shape [X][N]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.SetDefinition(`Definition: Filter by rows index.
Example:
` + "\n```python" + `
RowsIndexFilter = {3,1,0}
Data = [{"a", "b", "c", "d"}, {0, 1, 2, 3}]
Out = [{"d", "b", "a"}, {3, 1, 0}]
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameCopy)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("In", "source tensor", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T1)
		opDef.AddOutput("Out", "target tensor", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T1)
		opDef.AddAttribute(InputPartyCodesAttr, "Input tensor `In` belongs to")
		opDef.AddAttribute(OutputPartyCodesAttr, "Output tensor `Out` belongs to")
		opDef.SetDefinition(`Definition: Copy source tensor "In" to new tensor "Out" on target party`)
		opDef.SetParamTypeConstraint(T1, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameFilter)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("Filter", "Filter tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T1)
		opDef.AddInput("In", "Tensors to be filtered.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddOutput("Out", "Output tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.SetDefinition(`Definition: Given a boolean tensor Filter (its shape is [M]), and a number of tensors In
(variadic, each tensor's shape must be [M]), for i in [0, M-1], keep the In tensors' element if and only if Filter[i]
is True, output the filter result tensors Out (variadic). Example:
` + "\n```python" + `
Filter = {True, False, False, True, False}
In = {a, b, c, d, e}
Out = {a, d}
` + "```\n")
		opDef.SetParamTypeConstraint(T1, statusPrivateOrPublic)
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecret)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameConstant)
		opDef.SetStreamingType(SinkOp)
		opDef.AddOutput("Out", "output tensor(shape [M]) from constant.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddAttribute(ScalarAttr, "scalar attribute(with shape [M])")
		opDef.AddAttribute(ToStatusAttr, "int64. to status, 0: to private, 1: to public.")
		opDef.AddDefaultAttributeValue(ToStatusAttr, CreateIntAttribute(0))
		opDef.SetDefinition(`Definition: Make constant from attribute.
Example:
` + "\n```python" + `
scalar = [{"a", "b", "c"}]
to_status = 0
Out = [{"a", "b", "c"}]
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivateOrPublic)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameDumpFile)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("In", "Tensors to be dumped.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddOutput("Out", "Tensors have been dumped.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.SetDefinition(`Definition: Dump the input tensor. Note: This op will change the affected rows in the session`)
		opDef.AddAttribute(FilePathAttr, "String. Absolute file path to dump the tensors.")
		opDef.AddAttribute(FieldDeliminatorAttr, "String. Column deliminator, e.g. `\\t`")
		opDef.AddDefaultAttributeValue(FieldDeliminatorAttr, CreateStringAttribute("\\t"))
		opDef.AddAttribute(QuotingStyleAttr, "Int64. Strategies for using quotes, 0: do not use quotes; 1: use quotes for strings; 2: use quotes for all valid data")
		opDef.AddDefaultAttributeValue(QuotingStyleAttr, CreateIntAttribute(0))
		opDef.AddAttribute(LineTerminatorAttr, "String. Line terminator, e.g. `\\n`")
		opDef.AddDefaultAttributeValue(LineTerminatorAttr, CreateStringAttribute("\\n"))
		opDef.SetParamTypeConstraint(T, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameInsertTable)
		opDef.AddInput("In", "Tensors to be inserted to DB table.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddOutput("Out", "Tensors have been inserted to DB table.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.SetDefinition(`Definition: Insert the input tensor to existing table in Database. Note: This op will change the affected rows in the session`)
		opDef.AddAttribute(TableNameAttr, "String. table to insert the tensors.")
		opDef.AddAttribute(ColumnNamesAttr, "String array. column names of table.")
		opDef.SetParamTypeConstraint(T, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameIn)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("Left", "First operand.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddInput("Right", "Second operand.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T1)
		opDef.AddOutput("Out", "Output Tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddAttribute(InTypeAttr, "Int64. 0: PSI In, 1: Share In, 2: Local In")
		opDef.AddDefaultAttributeValue(InTypeAttr, CreateIntAttribute(0))
		opDef.AddAttribute(PsiAlgorithmAttr, "Int64. PSI Algorithm for In. 0: Auto, 1: Ecdh, 2: Oprf;")
		opDef.AddDefaultAttributeValue(PsiAlgorithmAttr, CreateIntAttribute(0))
		opDef.AddAttribute(InputPartyCodesAttr, "List of parties the inputs belong to. This attribute is required if algorithm = PSI.")
		opDef.AddAttribute(RevealToAttr, "A party can see the result. This attribute is required if algorithm = PSI.")
		opDef.SetDefinition(`Definition: Given an input tensor Left (its shape is [M]), and another input tensor Right (its shape is [N]),
check whether Left's element exists in Right's elements and output a boolean tensor Out (its shape is [M]). Left and Right must be the same type.
Example:
` + "\n```python" + `
Left = {a, b, c, d}
Right = {b, d, e, f, g, h}
Out = {False, True, False, True}
` + "```\n")
		// for psi in, status must be private
		// support share/local in later
		opDef.SetParamTypeConstraint(T, statusPrivate)
		opDef.SetParamTypeConstraint(T1, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		// Following definition of reduce_sum in
		// - TensorFlow: https://www.tensorflow.org/api_docs/python/tf/math/reduce_sum
		// - ONNX: https://github.com/onnx/onnx/blob/master/docs/Operators.md#ReduceSum
		opDef := &OperatorDef{}
		opDef.SetName(OpNameReduceSum)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("In", "Tensor to be summed (shape [M]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddOutput("Out", "The summed Tensor (shape [1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.SetDefinition(`Definition: Given an input tensor In, return the sum of input tensor's elements.
Example:
` + "\n```python" + `
In = {1, 2, 3, 4, 5, 6}
Out = {21}
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecret)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameReduceCount)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("In", "Tensor to be counted (shape [M]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddOutput("Out", "The counted Tensor (shape [1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.SetDefinition(`Definition: Given an input tensor In, return the count of input tensor's elements.
Example:
` + "\n```python" + `
In = {1, 2, 3, 4, 5, 6}
Out = {6}
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecret)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		// Following definition of reduce_max in
		// - TensorFlow: https://www.tensorflow.org/api_docs/python/tf/math/reduce_max
		// - ONNX: https://github.com/onnx/onnx/blob/master/docs/Operators.md#reducemax
		opDef := &OperatorDef{}
		opDef.SetName(OpNameReduceMax)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("In", "Tensor to be maxed (shape [M]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddOutput("Out", "The maxed Tensor (shape [1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.SetDefinition(`Definition: Given a input tensor In, return the max of input tensor's elements.
Example:
` + "\n```python" + `
In = {1, 2, 3, 4, 5, 6}
Out = {6}
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecret)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		// Following definition of reduce_min in
		// - TensorFlow: https://www.tensorflow.org/api_docs/python/tf/math/reduce_min
		// - ONNX: https://github.com/onnx/onnx/blob/master/docs/Operators.md#reducemin
		opDef := &OperatorDef{}
		opDef.SetName(OpNameReduceMin)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("In", "Tensor to be mined (shape [M]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddOutput("Out", "The mined Tensor (shape [1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.SetDefinition(`Definition: Given a input tensor In, return the min of input tensor's elements.
Example:
` + "\n```python" + `
In = {1, 2, 3, 4, 5, 6}
Out = {1}
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecret)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameReduceAvg)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("In", "Tensor to be reduced (shape [M]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddOutput("Out", "The average Tensor (shape [1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.SetDefinition(`Definition: Given a input tensor In, return the average of input tensor's elements.
Example:
` + "\n```python" + `
In = {1, 2, 3, 4, 5}
Out = {3}

In = {1, 2, 3, 4}
Out = {2.5}
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecret)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameShape)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("In", "Input Tensors",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddOutput("Out", "Shape Tensors",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T1)
		opDef.AddAttribute(AxisAttr, "Int64. Specific dimension of the shape.")
		opDef.AddDefaultAttributeValue(AxisAttr, CreateIntAttribute(-1))
		opDef.SetDefinition(`Definition: Given tensors In, return shapes of each tensor. Axis starts from 0. If axis is set, dimensions of each shape are returned. If axis is not set(default -1), shapes are returned.
Example:
` + "\n```python" + `
In = { {1, 2}, {2, 3}, {4, 3, 3} } # {1, 2} here is a column vector
Out = { {2, 1}, {2, 1}, {3, 1} }
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecretOrPublic)
		opDef.SetParamTypeConstraint(T1, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameUnique)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("Key", "Input key tensors(shape [M][1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddOutput("UniqueKey", "Output unique key tensor(shape [K][1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.SetDefinition(`Definition: Unique of Key tensor.
Example:
` + "\n```python" + `
Key = {"a", "b", "a", "d"}
UniqueKey = {"a", "b", "d"}
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameSort)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("Key", "Sort Key(shape [M][1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddInput("In", "Sort Value(shape [M][1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddOutput("Out", "Sorted Value(shape [M][1])",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddAttribute(ReverseAttr, "Bool. If True, the sorted tensor in descending order.")
		opDef.AddDefaultAttributeValue(ReverseAttr, CreateBoolAttribute(false))
		opDef.SetDefinition("Definition: sort `In` using `Key`." + `
Example:
` + "\n```python" + `
Key = {3, 1, 2, 4}
In = [{3, 1, 2, 4}, {1, 2, 3, 4}, {9, 8, 7, 6}]
Out = [{1, 2, 3, 4}, {2, 3, 1, 4}, {8, 7, 9, 6}]
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecret)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameObliviousGroupMark)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("Key", "Pre-sorted group keys (shape [M][1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddOutput("Group",
			"End of group indicator(shape [M][1]). Element 1 means the row is the last element of the group, 0 is not.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.SetDefinition("Definition: generate end of group indicator `Group` based on `Key`. The operator calculates Group[i] = not_eq(Key[i+1], Key[i])." + `
Example:
` + "\n```python" + `
Key = [{0, 0, 0, 1}, {0, 1, 1, 1}]
Group = {1, 0, 1, 1}

Key = [{0, 0, 1, 2, 2}]
Group = {0, 1, 1, 0, 1}
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecret)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		type tmpl struct {
			opName    string
			aggResult string
		}
		for _, t := range []tmpl{
			{opName: OpNameObliviousGroupSum, aggResult: `[{1, 3, 5, 9, 0}, {9, 8, 15, 21, 5}]`},
			{opName: OpNameObliviousGroupCount, aggResult: `[{1, 1, 2, 3, 1}, {1, 1, 2, 3, 1}]`},
			{opName: OpNameObliviousGroupMax, aggResult: `[{1, 3, 3, 4, 0}, {9, 8, 8, 8, 5}]`},
			{opName: OpNameObliviousGroupMin, aggResult: `[{1, 3, 2, 2, 0}, {9, 8, 7, 6, 5}]`},
			{opName: OpNameObliviousGroupAvg, aggResult: `[{1, 3, 2.5, 3, 0}, {9, 8, 7.5, 7, 5}]`},
		} {
			opDef := &OperatorDef{}
			opDef.SetName(t.opName)
			opDef.SetStreamingType(SinkOp)
			opDef.AddInput("Group",
				"End of group indicator(shape [M][1]). Element 1 means the row is the last element of the group, 0 is not.",
				proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
			opDef.AddInput("In", "Values to be aggregated (shape [M][1]).",
				proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
			opDef.AddOutput("Out", "Partially aggregated values (shape [M][1]).",
				proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
			opDef.SetDefinition("Definition: partially aggregate `In` according to end of group indicator." + fmt.Sprintf(`
Example:
`+"\n```python"+`
Group = {1, 0, 0, 1, 1}
In = [{1, 3, 2, 4, 0}, {9, 8, 7, 6, 5}]
Out = %s
`, t.aggResult) + "```\n")
			opDef.SetParamTypeConstraint(T, statusSecret)
			check(opDef.err)
			AllOpDef = append(AllOpDef, opDef)
		}
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameShuffle)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("In", "Input Value(shape [M][1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddOutput("Out", "Output Value(shape [M][1])",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.SetDefinition("Definition: Shuffle `In`." + `
Example:
` + "\n```python" + `
In = [{1, 2, 3, 4}, {9, 8, 7, 6}]
Out = [{4, 3, 2, 1}, {6, 7, 8, 9}]
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusSecret)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameBroadcastTo)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("In", "Input tensor", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddInput("ShapeRefTensor", "Shape reference tensor", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T1)
		opDef.AddOutput("Out", "Result tensor", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T2)
		opDef.SetDefinition("Definition: Broadcast Input tensor `In` to the same shape as `ShapeRefTensor`. \nExample:\n```Python" + `
In = [1]
ShapeRefTensor = [a, b, c]
# ShapeRefTensor's shape is (3, 1), broadcast In to shape (3, 1)
Out = BroadcastTo(In, ShapeRefTensor) = [1, 1, 1]
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPublic)
		opDef.SetParamTypeConstraint(T1, statusPrivateOrSecretOrPublic)
		opDef.SetParamTypeConstraint(T2, statusPrivateOrPublic)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	for _, opName := range UnaryOps {
		opDef := &OperatorDef{}
		opDef.SetName(opName)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("In", "Input tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddOutput("Out", "Output tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.SetDefinition(fmt.Sprintf("Definition:  Out = %s In", opName))
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecretOrPublic)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameConcat)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("In", "Tensors to be concat.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddOutput("Out", "Concated Tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddAttribute("axis", "Int64. Dimension along which to concatenate.")
		opDef.AddDefaultAttributeValue("axis", CreateIntAttribute(0))
		opDef.SetDefinition(`Definition: Given a number of tensors In (variadic, each tensor's shape must be the same in every dimension except for the axis), concat the In tensors along the axis.
Example:
` + "\n```python" + `
In = { {1, 2}, {2, 3, 4}, {3, 4, 5, 6} }
Out = {1, 2, 2, 3, 4, 3, 4, 5, 6}
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusSecret)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameGroup)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("Key", "input key tensors(shape [M][1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddOutput("GroupId", "group id vector(shape [M][1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddOutput("GroupNum", "number of groups vector(shape [1][1])",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.SetDefinition(`Definition: Assign a group id(start from 0) for each input element.
Example:
` + "\n```python" + `
Key = [{"a", "c", "a", "d"}, {0, 2, 0, 3}]
GroupId = {0, 1, 0, 2}
GroupNum = {3}
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameIf)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("Condition", "Condition tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddInput("ValueIfTrue", "Value if true tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T1)
		opDef.AddInput("ValueIfFalse", "Value if false tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T2)
		opDef.AddOutput("Out", "Result tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T3)
		opDef.SetDefinition(`The IF operator returns a value if a condition is TRUE, or another value if a condition is FALSE.
Example:
` + "\n```python" + `
Condition = [true, false, true, true]
ValueIfTrue = [0, 0, 0, 0]
ValueIfFalse = [1, 1, 1, 1]
Out = [0, 1, 0, 0]
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecretOrPublic)
		opDef.SetParamTypeConstraint(T1, statusPrivateOrSecretOrPublic)
		opDef.SetParamTypeConstraint(T2, statusPrivateOrSecretOrPublic)
		opDef.SetParamTypeConstraint(T3, statusPrivateOrSecretOrPublic)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameCaseWhen)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("Condition", "Condition tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddInput("Value", "Value if condition tensor is true and all previous conditions are false.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T1)
		opDef.AddInput("ValueElse", "Value if all condition tensors are false.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T2)
		opDef.AddOutput("Out", "Result tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T3)
		opDef.SetDefinition(`The CaseWhen operator goes through conditions and returns a value when the first condition is met (like an if-then-else statement)

Example:
` + "\n```python" + `
Condition = [[true, false, false, false], [true, true, false, false]]
Value = [[0, 0, 0, 0], [1, 1, 1, 1]]
ValueElse = [2, 2, 2, 2]
Out = [0, 1, 2, 2]
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecretOrPublic)
		opDef.SetParamTypeConstraint(T1, statusPrivateOrSecretOrPublic)
		opDef.SetParamTypeConstraint(T2, statusPrivateOrSecretOrPublic)
		opDef.SetParamTypeConstraint(T3, statusPrivateOrSecretOrPublic)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		type tmpl struct {
			opName    string
			aggResult string
		}
		for _, t := range []tmpl{
			{opName: OpNameGroupFirstOf, aggResult: `[{0, 1, 4}, {9, 8, 5}]`},
			{opName: OpNameGroupCount, aggResult: `[{2, 2, 1}, {2, 2, 1}]`},
			{opName: OpNameGroupCountDistinct, aggResult: `[{2, 2, 1}, {2, 2, 1}]`},
			{opName: OpNameGroupSum, aggResult: `[{2, 4, 4}, {16, 14, 5}]`},
			{opName: OpNameGroupAvg, aggResult: `[{1, 2, 4}, {8, 7, 5}]`},
			{opName: OpNameGroupMin, aggResult: `[{0, 1, 4}, {7, 6, 5}]`},
			{opName: OpNameGroupMax, aggResult: `[{2, 3, 4}, {9, 8, 5}]`},
		} {
			opDef := &OperatorDef{}
			opDef.SetName(t.opName)
			opDef.SetStreamingType(SinkOp)
			opDef.AddInput("GroupId", "Input group id vector(shape [M][1]).",
				proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
			opDef.AddInput("GroupNum", "Input number of groups vector(shape [1][1]).",
				proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
			opDef.AddInput("In", "Input data tensor(shape [M][1]).",
				proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
			opDef.AddOutput("Out", "Output data tensors(shape [K][1], K equals to number of groups), Out[i] is the agg result for i-th group.",
				proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
			opDef.SetDefinition("Definition: Aggregate `In` for each group." + fmt.Sprintf(`
Example:
`+"\n```python"+`
GroupId = {0, 1, 0, 1, 2}
GroupNum = {3}
In = [{0, 1, 2, 3, 4}, {9, 8, 7, 6, 5}]
Out = %s
`, t.aggResult) + "```\n")
			opDef.SetParamTypeConstraint(T, statusPrivate)
			check(opDef.err)
			AllOpDef = append(AllOpDef, opDef)
		}
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameGroupHeSum)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("GroupId", "Input group id vector(shape [M][1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddInput("GroupNum", "Input number of groups vector(shape [1][1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddInput("In", "Input data tensor(shape [M][1]).",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddOutput("Out", "Output data tensors(shape [K][1], K equals to number of groups), Out[i] is the agg result for i-th group.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddAttribute(InputPartyCodesAttr, "List of parties the inputs belong to([PartyCodeForGroupId/Num, PartyCodeForIn]).")
		opDef.SetDefinition(`Definition: Using HE to sum 'In' for each group.
Example:
` + "\n```python" + `
GroupId = {0, 1, 0, 1, 2}
GroupNum = {3}
In = {0, 1, 2, 3, 4}
Out = {2, 4, 4}
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameCast)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("In", "Input tensor.", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddOutput("Out", "Output tensor.", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.SetDefinition(`Definition: Cast Input tensor's data type to Output tensor's.`)
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecretOrPublic)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameLimit)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("In", "Tensors to be limited.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddOutput("Out", "Output tensor.",
			proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.SetDefinition(`Limit return part of data, the amount of data depends on limit attr, the offset of data depends on offset attr.
Example:
` + "\n```python" + `
offset = 1
count = 2
In = {a, b, c, d, e}
Out = {b, c}
` + "```\n")
		opDef.AddAttribute(LimitOffsetAttr, "offset in limit")
		opDef.AddAttribute(LimitCountAttr, "count in limit")
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecretOrPublic)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameIsNull)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("In", "Input tensor.", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddOutput("Out", "Output tensor.", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.SetDefinition(`Definition: Test if Input tensor's data contains NULL.
Example:
` + "\n```python" + `
In = {0, 1, NULL}
Out = {false, false, true}
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameIfNull)
		opDef.SetStreamingType(StreamingOp)
		opDef.AddInput("Expr", "The expression to test whether is NULL", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddInput("AltValue", "The value to return if Expr is NULL", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddOutput("Out", "Result", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.SetDefinition(`Definition: If Expr is NULL, return AltValue. Otherwise, return Expr.
Example:
` + "\n```python" + `
Expr = {0, 1, NULL}
AltValue = {10, 10, 10}
Out = {0, 1, 10}
` + "```\n")
		opDef.SetParamTypeConstraint(T, statusPrivate)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)

		{
			opDef := &OperatorDef{}
			opDef.SetName(OpNameCoalesce)
			opDef.SetStreamingType(StreamingOp)
			opDef.AddInput("Exprs", "The expressions to coalesce", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
			opDef.AddOutput("Out", "Result", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
			opDef.SetDefinition(`Definition: Coalesce returns the first value of Exprs that is not NULL. NULL is returned only if Exprs are all NULL.
Example:
` + "\n```python" + `
Exprs[0] = {0, NULL, NULL}
Exprs[1] = {0, 1, NULL}
Out = {0, 1, NULL}
` + "```\n")
			opDef.SetParamTypeConstraint(T, statusPrivate)
			check(opDef.err)
			AllOpDef = append(AllOpDef, opDef)
		}

		{
			opDef := &OperatorDef{}
			opDef.SetName(OpNameBucket)
			opDef.SetStreamingType(SinkOp)
			opDef.AddInput("Key", "Join Key Tensors", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
			opDef.AddInput("In", "Input Tensors", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
			opDef.AddOutput("Out", "Result", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
			opDef.AddAttribute(InputPartyCodesAttr, "List of parties the inputs belong to([PartyCodeLeft, PartyCodeRight]).")
			opDef.SetDefinition(`Definition: Put the data into buckets based on the hash value of the join key.`)
			opDef.SetParamTypeConstraint(T, statusPrivate)
			check(opDef.err)
			AllOpDef = append(AllOpDef, opDef)
		}

	}
	{
		{
			opDef := &OperatorDef{}
			opDef.SetName(OpNameCos)
			opDef.SetStreamingType(StreamingOp)
			opDef.AddInput("In", "the expression pass to cosine function", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
			opDef.AddOutput("Out", "Result", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
			opDef.SetDefinition("Definition: return the value of cosine function")
			opDef.SetParamTypeConstraint(T, statusPrivateOrSecretOrPublic)
			check(opDef.err)
			AllOpDef = append(AllOpDef, opDef)
		}

		{
			opDef := &OperatorDef{}
			opDef.SetName(OpNameSin)
			opDef.SetStreamingType(StreamingOp)
			opDef.AddInput("In", "the expression pass to sine function", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
			opDef.AddOutput("Out", "Result", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
			opDef.SetDefinition("Definition: return the value of sine function")
			opDef.SetParamTypeConstraint(T, statusPrivateOrSecretOrPublic)
			check(opDef.err)
			AllOpDef = append(AllOpDef, opDef)
		}

		{
			opDef := &OperatorDef{}
			opDef.SetName(OpNameACos)
			opDef.SetStreamingType(StreamingOp)
			opDef.AddInput("In", "the expression pass to arc cosine function", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
			opDef.AddOutput("Out", "Result", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
			opDef.SetDefinition("Definition: return the value of arc cosine function")
			opDef.SetParamTypeConstraint(T, statusPrivateOrSecretOrPublic)
			check(opDef.err)
			AllOpDef = append(AllOpDef, opDef)
		}
	}

	{
		opDef := &OperatorDef{}
		opDef.SetName(OpNameRowNumber)
		opDef.SetStreamingType(SinkOp)
		opDef.AddInput("Key", "the tensors which used for sorting in partition, e.g. [2,0,4,2,3,7]", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, T)
		opDef.AddInput("PartitionId", "the partitioned id, e.g. [0,0,0,1,1,1], the first 3 in a group and the others are in another group", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddInput("PartitionNum", "the partitioned num, e.g. [2]", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.AddOutput("Out", "row number output, e.g. [2,1,3,1,2,3]", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, T)
		opDef.SetDefinition("Definition: return the row number in each partition")
		opDef.AddAttribute(ReverseAttr, `string array consits of "0" and "1", "0" means this input tensor sort by ascending, "1" means this tensor sort by descending.
		e.g. ["0","1"] means the first input key sort by ascending, the second sort by descending`)
		opDef.SetParamTypeConstraint(T, statusPrivateOrSecretOrPublic)
		check(opDef.err)
		AllOpDef = append(AllOpDef, opDef)
	}
}

func GetAllOpDef() ([]*proto.OperatorDef, int) {
	rval := make([]*proto.OperatorDef, 0)
	for _, op := range AllOpDef {
		rval = append(rval, op.GetOperatorDefProto())
	}
	return rval, version
}

func init() {
	registerAllOpDef()
}

func FindOpDef(opType string) (*OperatorDef, error) {
	for _, op := range AllOpDef {
		if opType == op.GetName() {
			return op, nil
		}
	}
	return nil, fmt.Errorf("findOpDef: failed to find opType %v", opType)
}
