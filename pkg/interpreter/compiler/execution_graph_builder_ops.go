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
	"encoding/base64"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/planner/core"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/types"
)

func (builder *ExecutionGraphBuilder) addOpRunSQL(outs []*graph.Tensor, sourceParty string, sql string, tableRefs []string) error {
	attrs := map[string]*graph.Attribute{
		operator.SqlAttr:       graph.NewStringAttr(sql),
		operator.TableRefsAttr: graph.NewStringsAttr(tableRefs),
	}
	return builder.addEngineNode("run_sql", operator.OpNameRunSQL, nil, map[string][]*graph.Tensor{"Out": outs}, attrs, []string{sourceParty})
}

func (builder *ExecutionGraphBuilder) addOpSecretJoin(leftKeys, rightKeys, leftPayloads, rightPayloads, leftOutputs, rightOutputs []*graph.Tensor) error {
	inputs := map[string][]*graph.Tensor{
		"LeftKey":  leftKeys,
		"RightKey": rightKeys,
		"Left":     leftPayloads,
		"Right":    rightPayloads,
	}
	outputs := map[string][]*graph.Tensor{
		"LeftOutput":  leftOutputs,
		"RightOutput": rightOutputs,
	}
	return builder.addEngineNode("secret_eq_join", operator.OpNameSecretJoin, inputs, outputs, nil, builder.GetAllParties())
}

func (builder *ExecutionGraphBuilder) addOpPsiJoin(left, right []*graph.Tensor, leftIndex, rightIndex *graph.Tensor, partyCodes []string, joinType int, psiAlg proto.PsiAlgorithmType) error {
	attrs := map[string]*graph.Attribute{
		operator.InputPartyCodesAttr: graph.NewStringsAttr(partyCodes),
		operator.JoinTypeAttr:        graph.NewInt64Attr(int64(joinType)),
		operator.PsiAlgorithmAttr:    graph.NewInt64Attr(int64(psiAlg)),
	}
	inputs := map[string][]*graph.Tensor{
		"Left":  left,
		"Right": right,
	}
	outputs := map[string][]*graph.Tensor{
		"LeftJoinIndex":  nil,
		"RightJoinIndex": nil,
	}
	if leftIndex != nil {
		outputs["LeftJoinIndex"] = []*graph.Tensor{leftIndex}
	}
	if rightIndex != nil {
		outputs["RightJoinIndex"] = []*graph.Tensor{rightIndex}
	}
	return builder.addEngineNode("psi_join", operator.OpNameJoin, inputs, outputs, attrs, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpFilterByIndex(filter *graph.Tensor, payloads, outs []*graph.Tensor, partyCode string) error {
	inputs := map[string][]*graph.Tensor{
		"RowsIndexFilter": {filter},
		"Data":            payloads,
	}
	outputs := map[string][]*graph.Tensor{"Out": outs}
	return builder.addEngineNode("filter_by_index", operator.OpNameFilterByIndex, inputs, outputs, nil, []string{partyCode})
}

func (builder *ExecutionGraphBuilder) addOpReduce(aggName string, in, out *graph.Tensor, attrs map[string]*graph.Attribute) error {
	opType, ok := operator.ReduceAggOp[aggName]
	if !ok {
		return fmt.Errorf("addOpReduce: %s is not supported", aggName)
	}

	partyCodes := builder.GetAllParties()
	if in.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		partyCodes = []string{in.OwnerPartyCode}
	}

	inputs := map[string][]*graph.Tensor{"In": {in}}
	outputs := map[string][]*graph.Tensor{"Out": {out}}

	return builder.addEngineNode("reduce["+aggName+"]", opType, inputs, outputs, attrs, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpUnique(in, out *graph.Tensor) error {
	inputs := map[string][]*graph.Tensor{"Key": {in}}
	outputs := map[string][]*graph.Tensor{"UniqueKey": {out}}
	return builder.addEngineNode("unique", operator.OpNameUnique, inputs, outputs, nil, []string{in.OwnerPartyCode})
}

func (builder *ExecutionGraphBuilder) addOpSort(keys, payloads, outs []*graph.Tensor, directions []bool) error {
	if len(keys) != len(directions) {
		return fmt.Errorf("addOpSort: keys length %d not match directions length %d", len(keys), len(directions))
	}
	partyCodes := builder.GetAllParties()
	if keys[0].Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		partyCodes = []string{keys[0].OwnerPartyCode}
	}

	attrs := map[string]*graph.Attribute{operator.ReverseAttr: graph.NewBoolsAttr(directions)}
	inputs := map[string][]*graph.Tensor{
		"Key": keys,
		"In":  payloads,
	}
	outputs := map[string][]*graph.Tensor{"Out": outs}

	return builder.addEngineNode("sort", operator.OpNameSort, inputs, outputs, attrs, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpObliviousGroupMark(keys []*graph.Tensor, out *graph.Tensor) error {
	inputs := map[string][]*graph.Tensor{"Key": keys}
	outputs := map[string][]*graph.Tensor{"Group": {out}}
	return builder.addEngineNode("oblivious_group_mark", operator.OpNameObliviousGroupMark, inputs, outputs, nil, builder.GetAllParties())
}

func (builder *ExecutionGraphBuilder) addOpShape(in, out *graph.Tensor) error {
	inputs := map[string][]*graph.Tensor{"In": {in}}
	outputs := map[string][]*graph.Tensor{"Out": {out}}
	attrs := map[string]*graph.Attribute{operator.AxisAttr: graph.NewInt64Attr(0)}
	return builder.addEngineNode("shape", operator.OpNameShape, inputs, outputs, attrs, []string{out.OwnerPartyCode})
}

func (builder *ExecutionGraphBuilder) addOpBroadcastTo(shapeRef *graph.Tensor, in, out []*graph.Tensor) error {
	partyCodes := builder.GetAllParties()
	if shapeRef.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		partyCodes = []string{shapeRef.OwnerPartyCode}
	}

	inputs := map[string][]*graph.Tensor{
		"ShapeRefTensor": {shapeRef},
		"In":             in,
	}
	outputs := map[string][]*graph.Tensor{"Out": out}

	return builder.addEngineNode("broadcast_to", operator.OpNameBroadcastTo, inputs, outputs, nil, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpPrivateGroup(keys []*graph.Tensor, groupId, groupNum *graph.Tensor, partyCode string) error {
	inputs := map[string][]*graph.Tensor{"Key": keys}
	outputs := map[string][]*graph.Tensor{
		"GroupId":  {groupId},
		"GroupNum": {groupNum},
	}
	return builder.addEngineNode("private_group", operator.OpNameGroup, inputs, outputs, nil, []string{partyCode})
}

func (builder *ExecutionGraphBuilder) addOpPrivateGroupAgg(name string, opType string, groupId, groupNum *graph.Tensor, ins, outs []*graph.Tensor, attrs map[string]*graph.Attribute, partyCode string) error {
	inputs := map[string][]*graph.Tensor{
		"GroupId":  {groupId},
		"GroupNum": {groupNum},
		"In":       ins,
	}
	outputs := map[string][]*graph.Tensor{"Out": outs}

	return builder.addEngineNode(name, opType, inputs, outputs, attrs, []string{partyCode})
}

func (builder *ExecutionGraphBuilder) addOpGroupSecretAgg(funcName string, groupId, groupNum, in, out *graph.Tensor) error {
	opName, ok := operator.GroupSecretAggOp[funcName]
	if !ok {
		return fmt.Errorf("addOpGroupSecretAgg: unsupported funcName %s", funcName)
	}
	inputs := map[string][]*graph.Tensor{
		"GroupId":  {groupId},
		"GroupNum": {groupNum},
		"In":       {in},
	}
	outputs := map[string][]*graph.Tensor{"Out": {out}}

	return builder.addEngineNode(fmt.Sprintf("private_group_secret_%s", funcName), opName, inputs, outputs, nil, builder.GetAllParties())
}

func (builder *ExecutionGraphBuilder) addOpShuffle(ins, outs []*graph.Tensor) error {
	inputs := map[string][]*graph.Tensor{"In": ins}
	outputs := map[string][]*graph.Tensor{"Out": outs}
	return builder.addEngineNode("shuffle", operator.OpNameShuffle, inputs, outputs, nil, builder.GetAllParties())
}

func (builder *ExecutionGraphBuilder) addOpObliviousGroupAgg(funcName string, group, in, out *graph.Tensor, attrs map[string]*graph.Attribute) error {
	opName, ok := operator.ObliviousGroupAggOp[funcName]
	if !ok {
		return fmt.Errorf("addOpObliviousGroupAgg: unsupported funcName %s", funcName)
	}

	inputs := map[string][]*graph.Tensor{"Group": {group}, "In": {in}}
	outputs := map[string][]*graph.Tensor{"Out": {out}}

	return builder.addEngineNode(funcName, opName, inputs, outputs, attrs, builder.GetAllParties())
}

func (builder *ExecutionGraphBuilder) addOpFilter(mask *graph.Tensor, ins, outs []*graph.Tensor, partyCodes []string) error {
	inputs := map[string][]*graph.Tensor{"Filter": {mask}, "In": ins}
	outputs := map[string][]*graph.Tensor{"Out": outs}
	return builder.addEngineNode("filter", operator.OpNameFilter, inputs, outputs, nil, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpReplicate(leftinputs, rightinputs, leftoutputs, rightoutputs []*graph.Tensor, partyCodes []string) error {
	attrs := map[string]*graph.Attribute{operator.InputPartyCodesAttr: graph.NewStringsAttr(partyCodes)}
	inputs := map[string][]*graph.Tensor{
		"Left":  leftinputs,
		"Right": rightinputs,
	}
	outputs := map[string][]*graph.Tensor{
		"LeftOut":  leftoutputs,
		"RightOut": rightoutputs,
	}
	return builder.addEngineNode("replicate", operator.OpNameReplicate, inputs, outputs, attrs, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpLimit(ins, outs []*graph.Tensor, offset, count int64, partyCodes []string) error {
	attrs := map[string]*graph.Attribute{
		operator.LimitOffsetAttr: graph.NewInt64Attr(offset),
		operator.LimitCountAttr:  graph.NewInt64Attr(count),
	}
	inputs := map[string][]*graph.Tensor{"In": ins}
	outputs := map[string][]*graph.Tensor{"Out": outs}
	return builder.addEngineNode("limit", operator.OpNameLimit, inputs, outputs, attrs, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpConcat(ins []*graph.Tensor, out *graph.Tensor) error {
	inputs := map[string][]*graph.Tensor{"In": ins}
	outputs := map[string][]*graph.Tensor{"Out": {out}}
	return builder.addEngineNode("concat", operator.OpNameConcat, inputs, outputs, nil, builder.GetAllParties())
}

func (builder *ExecutionGraphBuilder) addOpConstant(value *types.Datum, out *graph.Tensor) error {
	valueAttr := &graph.Attribute{}
	switch value.Kind() {
	case types.KindFloat32:
		valueAttr.SetFloat(float32(value.GetFloat64()))
	case types.KindFloat64:
		valueAttr.SetDouble(value.GetFloat64())
	case types.KindInt64:
		valueAttr.SetInt64(value.GetInt64())
	case types.KindMysqlDecimal:
		// NOTE(shunde.csd): SCQL Internal does not distinguish decimal and float,
		// It handles decimal as float.
		// If users have requirements for precision, they should use integers.
		v, err := value.GetMysqlDecimal().ToFloat64()
		if err != nil {
			return fmt.Errorf("addOpConstant: convert decimal to float error: %v", err)
		}
		valueAttr.SetDouble(v)
	case types.KindString:
		valueAttr.SetString(value.GetString())
	case types.KindMysqlTime:
		time := value.GetMysqlTime()
		timestamp := time.CoreTime().ToUnixTimestamp()
		valueAttr.SetInt64(timestamp)
	default:
		return fmt.Errorf("addOpConstant: unsupported data{%+v}", value)
	}
	attrs := map[string]*graph.Attribute{
		operator.ScalarAttr:   valueAttr,
		operator.ToStatusAttr: graph.NewInt64Attr(1), // public
	}
	outputs := map[string][]*graph.Tensor{"Out": {out}}
	return builder.addEngineNode("constant", operator.OpNameConstant, nil, outputs, attrs, builder.GetAllParties())
}

func (builder *ExecutionGraphBuilder) addOpIn(left, right, out *graph.Tensor, psiAlg proto.PsiAlgorithmType) error {
	partyCodes := []string{left.OwnerPartyCode, right.OwnerPartyCode}

	attrs := map[string]*graph.Attribute{
		operator.PsiAlgorithmAttr:    graph.NewInt64Attr(int64(psiAlg)),
		operator.InputPartyCodesAttr: graph.NewStringsAttr([]string{left.OwnerPartyCode, right.OwnerPartyCode}),
		operator.RevealToAttr:        graph.NewStringsAttr([]string{out.OwnerPartyCode}),
	}
	if left.OwnerPartyCode == right.OwnerPartyCode {
		attrs[operator.InTypeAttr] = graph.NewInt64Attr(operator.LocalIn)
		partyCodes = []string{left.OwnerPartyCode}
	}

	inputs := map[string][]*graph.Tensor{
		graph.Left:  {left},
		graph.Right: {right},
	}
	outputs := map[string][]*graph.Tensor{graph.Out: {out}}

	return builder.addEngineNode("in", operator.OpNameIn, inputs, outputs, attrs, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpArrowFunc(funcName string, funcOpt compute.FunctionOptions, ins []*graph.Tensor, out *graph.Tensor) error {
	attrs := map[string]*graph.Attribute{
		operator.FuncNameAttr: graph.NewStringAttr(funcName),
	}
	if funcOpt != nil {
		attrs[operator.FuncOptTypeAttr] = graph.NewStringAttr(funcOpt.TypeName())

		buf, err := compute.SerializeOptions(funcOpt, memory.DefaultAllocator)
		if err != nil {
			return fmt.Errorf("addOpArrowFunc: serialize func options failed: %v", err)
		}
		// encode to base64, attribute not support bytes yet.
		attrs[operator.FuncOptAttr] = graph.NewStringAttr(base64.StdEncoding.EncodeToString(buf.Bytes()))
	}

	inputs := map[string][]*graph.Tensor{"In": ins}
	outputs := map[string][]*graph.Tensor{"Out": {out}}
	nodeName := "arrow_func[" + funcName + "]"

	return builder.addEngineNode(nodeName, operator.OpNameArrowFunc, inputs, outputs, attrs, []string{out.OwnerPartyCode})
}

func (builder *ExecutionGraphBuilder) addOpBasicFunc(funcName string, ins []*graph.Tensor, output *graph.Tensor) error {
	opName, ok := astName2NodeName[funcName]
	if !ok {
		return fmt.Errorf("addOpBasicFunc: unsupported func name: %s", funcName)
	}

	partyCodes := builder.GetAllParties()
	if output.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		partyCodes = []string{output.OwnerPartyCode}
	}

	inputs := map[string][]*graph.Tensor{"In": ins}
	outputs := map[string][]*graph.Tensor{"Out": {output}}

	return builder.addEngineNode(opName, opName, inputs, outputs, nil, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpBinaryFunc(funcName string, left *graph.Tensor, right *graph.Tensor, output *graph.Tensor) error {
	opName, ok := astName2NodeName[funcName]
	if !ok {
		return fmt.Errorf("addOpBinaryFunc: unsupported func name: %s", funcName)
	}

	partyCodes := builder.GetAllParties()
	if output.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		partyCodes = []string{output.OwnerPartyCode}
	}

	inputs := map[string][]*graph.Tensor{"Left": {left}, "Right": {right}}
	outputs := map[string][]*graph.Tensor{"Out": {output}}

	return builder.addEngineNode(opName, opName, inputs, outputs, nil, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpIf(cond, valueTrue, valueFalse, output *graph.Tensor) error {
	partyCodes := builder.GetAllParties()
	if output.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		partyCodes = []string{output.OwnerPartyCode}
	}

	inputs := map[string][]*graph.Tensor{
		"Condition":    {cond},
		"ValueIfTrue":  {valueTrue},
		"ValueIfFalse": {valueFalse},
	}
	outputs := map[string][]*graph.Tensor{"Out": {output}}

	return builder.addEngineNode("if", operator.OpNameIf, inputs, outputs, nil, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpCaseWhen(conds, values []*graph.Tensor, valueElse, output *graph.Tensor) error {
	partyCodes := builder.GetAllParties()
	if output.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		partyCodes = []string{output.OwnerPartyCode}
	}

	inputs := map[string][]*graph.Tensor{
		"Condition": conds,
		"Value":     values,
		"ValueElse": {valueElse},
	}
	outputs := map[string][]*graph.Tensor{"Out": {output}}

	return builder.addEngineNode("case_when", operator.OpNameCaseWhen, inputs, outputs, nil, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpIfNull(expr, altValue, output *graph.Tensor) error {
	partyCodes := builder.GetAllParties()
	if output.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		partyCodes = []string{output.OwnerPartyCode}
	}

	inputs := map[string][]*graph.Tensor{"Expr": {expr}, "AltValue": {altValue}}
	outputs := map[string][]*graph.Tensor{"Out": {output}}

	return builder.addEngineNode("if_null", operator.OpNameIfNull, inputs, outputs, nil, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpCoalece(exprs []*graph.Tensor, output *graph.Tensor) error {
	partyCodes := builder.GetAllParties()
	if output.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		partyCodes = []string{output.OwnerPartyCode}
	}

	inputs := map[string][]*graph.Tensor{"Exprs": exprs}
	outputs := map[string][]*graph.Tensor{"Out": {output}}

	return builder.addEngineNode("coalesce", operator.OpNameCoalesce, inputs, outputs, nil, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpPrivateWindow(funcName string, keys []*graph.Tensor, partitionId, partitionNum *graph.Tensor, output *graph.Tensor, orderDescs []string) error {
	var opName string
	switch funcName {
	case ast.WindowFuncRowNumber:
		opName = operator.OpNameRowNumber
	case ast.WindowFuncRank:
		opName = operator.OpNameRank
	case ast.WindowFuncPercentRank:
		opName = operator.OpNamePercentRank
	default:
		return fmt.Errorf("addOpPrivateWindow: unsupported func name: %s", funcName)
	}

	attrs := map[string]*graph.Attribute{operator.ReverseAttr: graph.NewStringsAttr(orderDescs)}
	inputs := map[string][]*graph.Tensor{
		"Key":          keys,
		"PartitionId":  {partitionId},
		"PartitionNum": {partitionNum},
	}
	outputs := map[string][]*graph.Tensor{"Out": {output}}

	return builder.addEngineNode(opName, opName, inputs, outputs, attrs, []string{output.OwnerPartyCode})
}

func (builder *ExecutionGraphBuilder) addOpPublish(results, outs []*graph.Tensor, partyCode string) error {
	for _, out := range outs {
		builder.outputNames = append(builder.outputNames, out.Name)
	}

	inputs := map[string][]*graph.Tensor{"In": results}
	outputs := map[string][]*graph.Tensor{"Out": outs}
	return builder.addEngineNode("publish_result", operator.OpNamePublish, inputs, outputs, nil, []string{partyCode})
}

func (builder *ExecutionGraphBuilder) addOpBucket(keys, ins, outs []*graph.Tensor, partyCodes []string) error {
	attrs := map[string]*graph.Attribute{operator.InputPartyCodesAttr: graph.NewStringsAttr(partyCodes)}
	inputs := map[string][]*graph.Tensor{
		"Key": keys,
		"In":  ins,
	}
	outputs := map[string][]*graph.Tensor{"Out": outs}
	return builder.addEngineNode("bucket", operator.OpNameBucket, inputs, outputs, attrs, partyCodes)
}

func (builder *ExecutionGraphBuilder) addOpInsertTable(results, outs []*graph.Tensor, partyCode string, opt *core.InsertTableOption) error {
	attrs := map[string]*graph.Attribute{
		operator.TableNameAttr:   graph.NewStringAttr(opt.TableName),
		operator.ColumnNamesAttr: graph.NewStringsAttr(opt.Columns),
	}
	inputs := map[string][]*graph.Tensor{"In": results}
	outputs := map[string][]*graph.Tensor{"Out": outs}
	return builder.addEngineNode("insert_table", operator.OpNameInsertTable, inputs, outputs, attrs, []string{partyCode})
}

// refer to Arrow QuotingStyle: https://github.com/apache/arrow/blob/apache-arrow-14.0.0/cpp/src/arrow/csv/options.h#L174
const (
	_quotingNone     int64 = 0
	_quotingNeeded   int64 = 1
	_quotingAllValid int64 = 2
)

func (builder *ExecutionGraphBuilder) addOpDumpFile(ins, outs []*graph.Tensor, intoOpt *ast.SelectIntoOption, partyFile *ast.PartyFile) error {
	qsAttr := &graph.Attribute{}
	if intoOpt.FieldsInfo.Enclosed == 0 {
		qsAttr.SetInt64(_quotingNone) // default no quotes
	} else {
		// Limitations from Arrow CSV Writer C++ API: only support (ENCLOSED BY '"') and not support (ESCAPED BY) option
		// refer to: https://github.com/apache/arrow/blob/apache-arrow-14.0.0/cpp/src/arrow/csv/options.h#L187
		if intoOpt.FieldsInfo.Enclosed != '"' {
			return fmt.Errorf("AddDumpFileNode: only support \", not support: %v", intoOpt.FieldsInfo.Enclosed)
		}
		logrus.Warn("not support 'ESCAPED BY' Option, default ignored")

		if intoOpt.FieldsInfo.OptEnclosed {
			qsAttr.SetInt64(_quotingNeeded) // optionally enclosed valid string data
		} else {
			qsAttr.SetInt64(_quotingAllValid) // enclosed all valid data
		}
	}

	attrs := map[string]*graph.Attribute{
		operator.FilePathAttr:         graph.NewStringAttr(partyFile.FileName),
		operator.LineTerminatorAttr:   graph.NewStringAttr(intoOpt.LinesInfo.Terminated),
		operator.FieldDeliminatorAttr: graph.NewStringAttr(intoOpt.FieldsInfo.Terminated),
		operator.QuotingStyleAttr:     qsAttr,
	}
	inputs := map[string][]*graph.Tensor{"In": ins}
	outputs := map[string][]*graph.Tensor{"Out": outs}
	return builder.addEngineNode("dump_file", operator.OpNameDumpFile, inputs, outputs, attrs, []string{partyFile.PartyCode})
}
