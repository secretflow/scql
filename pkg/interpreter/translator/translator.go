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
	"sort"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/planner/core"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

var astName2NodeName = map[string]string{
	ast.Greatest: operator.OpNameGreatest,
	ast.Least:    operator.OpNameLeast,
	ast.LT:       operator.OpNameLess,
	ast.LE:       operator.OpNameLessEqual,
	ast.GT:       operator.OpNameGreater,
	ast.GE:       operator.OpNameGreaterEqual,
	ast.EQ:       operator.OpNameEqual,
	ast.NE:       operator.OpNameNotEqual,
	ast.LogicOr:  operator.OpNameLogicalOr,
	ast.LogicAnd: operator.OpNameLogicalAnd,
	ast.Plus:     operator.OpNameAdd,
	ast.Minus:    operator.OpNameMinus,
	ast.Mul:      operator.OpNameMul,
	ast.Div:      operator.OpNameDiv,
	ast.IntDiv:   operator.OpNameIntDiv,
	ast.Mod:      operator.OpNameMod,
}

type translator struct {
	ep              *GraphBuilder
	issuerPartyCode string
	enginesInfo     *EnginesInfo
	sc              *proto.SecurityConfig
	// if true, table in RunSQL string will skip database name
	skipDbName         bool
	securityCompromise *SecurityCompromiseConf
}

type SecurityCompromiseConf struct {
	RevealGroupMark bool `yaml:"reveal_group_mark"`
}

func NewTranslator(
	enginesInfo *EnginesInfo,
	sc *proto.SecurityConfig,
	issuerPartyCode string,
	skipDbName bool, securityCompromise *SecurityCompromiseConf) (
	*translator, error) {
	if sc == nil {
		return nil, fmt.Errorf("translate: empty CCL")
	}
	// filter out unneeded CCL
	allPartyCodes := map[string]bool{}
	for _, p := range enginesInfo.partyInfo.parties {
		allPartyCodes[p] = true
	}
	allPartyCodes[issuerPartyCode] = true
	newSc := &proto.SecurityConfig{ColumnControlList: []*proto.SecurityConfig_ColumnControl{}}
	for _, cc := range sc.ColumnControlList {
		if allPartyCodes[cc.PartyCode] {
			newSc.ColumnControlList = append(newSc.ColumnControlList, cc)
		}
	}
	return &translator{
		ep:                 NewGraphBuilder(enginesInfo.partyInfo),
		issuerPartyCode:    issuerPartyCode,
		sc:                 newSc,
		enginesInfo:        enginesInfo,
		skipDbName:         skipDbName,
		securityCompromise: securityCompromise,
	}, nil
}

func convertOriginalCCL(sc *proto.SecurityConfig) map[string]*ccl.CCL {
	result := make(map[string]*ccl.CCL)
	for _, cc := range sc.ColumnControlList {
		fullQualifiedName := strings.Join([]string{cc.DatabaseName, cc.TableName, cc.ColumnName}, ".")
		if result[fullQualifiedName] == nil {
			result[fullQualifiedName] = ccl.NewCCL()
		}
		result[fullQualifiedName].SetLevelForParty(cc.PartyCode, ccl.CCLLevel(cc.GetVisibility()))
	}
	return result
}

func exclude(ss []string, e string) []string {
	var rval []string
	for _, s := range ss {
		if s != e {
			rval = append(rval, s)
		}
	}
	return rval
}

func (t *translator) Translate(lp core.LogicalPlan) (*Graph, error) {
	// preprocessing lp
	processor := LpPrePocessor{}
	if err := processor.process(lp); err != nil {
		return nil, err
	}
	builder, err := newLogicalNodeBuilder(t.issuerPartyCode, t.enginesInfo, convertOriginalCCL(t.sc))
	if err != nil {
		return nil, err
	}
	ln, err := builder.buildLogicalNode(lp)
	if err != nil {
		return nil, err
	}
	// Check if the result is visible to the issuerPartyCode
	for i, col := range ln.Schema().Columns {
		cc := ln.CCL()[col.UniqueID]
		if !cc.IsVisibleFor(t.issuerPartyCode) {
			return nil, fmt.Errorf("ccl check failed: the %dth column %s in the result is not visibile (%s) to party %s", i+1, col.OrigName, cc.LevelFor(t.issuerPartyCode).String(), t.issuerPartyCode)
		}
	}
	// find one of the qualified computation parties to act as the query issuer
	if !slices.Contains(t.enginesInfo.partyInfo.GetParties(), t.issuerPartyCode) {
		cclWithoutIssuer := []*proto.SecurityConfig_ColumnControl{}
		for _, cc := range t.sc.ColumnControlList {
			if cc.PartyCode != t.issuerPartyCode {
				cclWithoutIssuer = append(cclWithoutIssuer, cc)
			}
		}
		t.sc = &proto.SecurityConfig{ColumnControlList: cclWithoutIssuer}

		// substitute query issuer
		candidateParties := ln.VisibleParty()
		candidateParties = exclude(candidateParties, t.issuerPartyCode)
		sort.Strings(candidateParties) // sort to enforce determinism
		if len(candidateParties) == 0 {
			return nil, fmt.Errorf("translate: unable to find a candidate party to substitute the issuer %s in party list (%+v)", t.issuerPartyCode, ln.VisibleParty())
		}
		t.issuerPartyCode = candidateParties[0]
	}
	return t.translate(ln)
}

func (t *translator) translateInternal(ln logicalNode) error {
	if dataSourceParties := ln.DataSourceParty(); len(dataSourceParties) == 1 {
		return t.buildRunSQL(ln, dataSourceParties[0])
	}
	for _, node := range ln.Children() {
		if err := t.translateInternal(node); err != nil {
			return err
		}
	}
	switch x := ln.(type) {
	case *ProjectionNode:
		return t.buildProjection(x)
	case *SelectionNode:
		return t.buildSelection(x)
	case *JoinNode:
		return t.buildJoin(x)
	case *ApplyNode:
		return t.buildApply(x)
	case *AggregationNode:
		return t.buildAggregation(x)
	case *DataSourceNode:
		ds, ok := x.lp.(*core.DataSource)
		if !ok {
			return fmt.Errorf("assert failed while translateInternal, expected: *core.DataSource, actual: %T", x.lp)
		}
		return fmt.Errorf("translate: DataSource %s is invisible to all party. Please check your security configuration. Detailed visibility: %+v",
			fmt.Sprintf("%s.%s", ds.DBName, ds.TableInfo().Name), x.CCL())
	case *UnionAllNode:
		return t.buildUnion(x)
	case *WindowNode:
		// TODO(xiaoyuan) add more code here
		return fmt.Errorf("window node is unimplemented")
	case *LimitNode:
		// TODO(xiaoyuan) add more code here
		return fmt.Errorf("limit node is unimplemented")
	default:
		return fmt.Errorf("translate: unsupported logical node type %T", ln)
	}
}

func (t *translator) translate(ln logicalNode) (*Graph, error) {
	if err := t.translateInternal(ln); err != nil {
		return nil, err
	}

	if err := t.addResultNode(ln); err != nil {
		return nil, err
	}

	return t.ep.Build(), nil
}

func (t *translator) addResultNode(ln logicalNode) error {
	if ln.IntoOpt() == nil {
		return t.addPublishNode(ln)
	}
	return t.addDumpFileNode(ln)
}

func (t *translator) addPublishNode(ln logicalNode) error {
	input := []*Tensor{}
	output := []*Tensor{}
	for _, it := range ln.ResultTable() {
		var err error
		// Reveal tensor to issuerPartyCode
		it, err = t.ep.converter.convertTo(it, &privatePlacement{partyCode: t.issuerPartyCode})
		if err != nil {
			return err
		}
		input = append(input, it)

		ot := t.ep.AddTensorAs(it)
		ot.Option = proto.TensorOptions_VALUE
		output = append(output, ot)
	}

	// Rename output tensors to result table column names
	for i, n := range ln.OutputNames() {
		if n.ColName.String() == "" {
			output[i].Name = ln.Schema().Columns[i].String()
		} else {
			output[i].Name = n.ColName.String()
		}
	}

	// Set execution plan's output tensor
	for _, ot := range output {
		t.ep.outputName = append(t.ep.outputName, ot.UniqueName())
	}

	err := t.ep.AddPublishNode("publish", input, output, []string{t.issuerPartyCode})
	if err != nil {
		return fmt.Errorf("addPublishNode: %v", err)
	}

	return nil
}

func (t *translator) addDumpFileNode(ln logicalNode) error {
	intoOpt := ln.IntoOpt()
	// issuer party code can see all outputs
	if intoOpt.PartyCode == "" {
		intoOpt.PartyCode = t.issuerPartyCode
	}
	// if into party code is not equal to issuer, refuse this query
	if intoOpt.PartyCode != t.issuerPartyCode {
		return fmt.Errorf("failed to check select into party code (%s) which is not equal to (%s)", intoOpt.PartyCode, t.issuerPartyCode)
	}
	input := ln.ResultTable()
	var output []*Tensor
	for i, it := range input {
		ot := t.ep.AddTensorAs(it)
		ot.Option = proto.TensorOptions_VALUE
		var colName string
		if ln.OutputNames()[i].ColName.String() == "" {
			colName = ln.Schema().Columns[i].String()
		} else {
			colName = ln.OutputNames()[i].ColName.String()
		}
		ot.Name = colName
		ot.DType = proto.PrimitiveDataType_STRING
		ot.StringS = []string{colName}
		output = append(output, ot)
	}
	return t.ep.AddDumpFileNode("dump_file", input, output, intoOpt.FileName, intoOpt.FieldsInfo.Terminated, intoOpt.PartyCode)
}

func (t *translator) buildRunSQL(ln logicalNode, partyCode string) error {
	// check whether tensor is visible to the party code
	for i, col := range ln.Schema().Columns {
		cc := ln.CCL()[col.UniqueID]
		if !cc.IsVisibleFor(partyCode) {
			return fmt.Errorf("ccl check failed: the %dth column %s in the result is not visibile (%s) to party %s", i+1, col.OrigName, cc.LevelFor(partyCode).String(), partyCode)
		}
	}
	sql, newTableRefs, err := runSQLString(ln.LP(), t.enginesInfo, t.skipDbName)
	if err != nil {
		return fmt.Errorf("addRunSQLNode: failed to rewrite sql=\"%s\", err: %w", sql, err)
	}
	return t.addRunSQLNode(ln, sql, newTableRefs, partyCode)
}

func (t *translator) addRunSQLNode(ln logicalNode, sql string, tableRefs []string, partyCode string) error {
	tensors := []*Tensor{}
	for i, column := range ln.Schema().Columns {
		tp, err := convertDataType(ln.Schema().Columns[i].RetType)
		if err != nil {
			return err
		}
		name := ln.Schema().Columns[i].String()
		tensor := t.ep.AddColumn(name, proto.TensorStatus_TENSORSTATUS_PRIVATE,
			proto.TensorOptions_REFERENCE, tp)
		tensor.OwnerPartyCode = partyCode
		if ln.CCL() != nil {
			tensor.cc = ln.CCL()[column.UniqueID]
		}
		tensors = append(tensors, tensor)
	}
	err := t.ep.AddRunSQLNode("runsql", tensors, sql, tableRefs, partyCode)
	if err != nil {
		return fmt.Errorf("addRunSQLNode: %v", err)
	}
	return ln.SetResultTableWithDTypeCheck(tensors)
}

func (t *translator) addFilterByIndexNode(filter *Tensor, ts []*Tensor, partyCode string) ([]*Tensor, error) {
	// NOTE(xiaoyuan) ts must be private and its owner party code equals to partyCode when apply filter by index
	partyToLocalTensors := map[string][]int{}
	for i, t := range ts {
		if t.Status == proto.TensorStatus_TENSORSTATUS_PRIVATE && t.OwnerPartyCode != "" {
			partyToLocalTensors[t.OwnerPartyCode] = append(partyToLocalTensors[t.OwnerPartyCode], i)
		} else {
			partyToLocalTensors[partyCode] = append(partyToLocalTensors[partyCode], i)
		}
	}
	// sort for deterministic
	var partyList []string
	for code := range partyToLocalTensors {
		partyList = append(partyList, code)
	}
	sort.Strings(partyList)
	outTs := make([]*Tensor, len(ts))
	for _, code := range partyList {
		indexes := partyToLocalTensors[code]
		localFilter := filter
		if code != partyCode {
			new_filter, err := t.ep.AddCopyNode("copy", filter, partyCode, code)
			if err != nil {
				return nil, fmt.Errorf("fail to apply filter by index: %v", err)
			}
			localFilter = new_filter
		}

		inTensors := make([]*Tensor, len(indexes))
		for i, origIndex := range indexes {
			inTensors[i] = ts[origIndex]
		}

		filtered, err := t.ep.AddFilterByIndexNode("filter_by_index", localFilter, inTensors, code)
		if err != nil {
			return nil, fmt.Errorf("fail to apply filter by index: %v", err)
		}

		for i, origIndex := range indexes {
			outTs[origIndex] = filtered[i]
		}
	}
	return outTs, nil
}

func (t *translator) buildExpression(expr expression.Expression, tensors map[int64]*Tensor, isApply bool, ln logicalNode) (*Tensor, error) {
	switch x := expr.(type) {
	case *expression.Column:
		return t.getTensorFromColumn(x, tensors)
	case *expression.ScalarFunction:
		return t.buildScalarFunction(x, tensors, isApply, ln)
	default:
		return nil, fmt.Errorf("buildExpression doesn't support condition type %T", x)
	}
}

// for now, support data type: bool/int/float/double/string
func convertDataType(typ *types.FieldType) (proto.PrimitiveDataType, error) {
	switch typ.Tp {
	case mysql.TypeLonglong:
		if mysql.HasIsBooleanFlag(typ.Flag) {
			return proto.PrimitiveDataType_BOOL, nil
		}
		return proto.PrimitiveDataType_INT64, nil
	case mysql.TypeLong:
		return proto.PrimitiveDataType_INT64, nil
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		return proto.PrimitiveDataType_STRING, nil
	case mysql.TypeTiny:
		return proto.PrimitiveDataType_BOOL, nil
	case mysql.TypeFloat:
		return proto.PrimitiveDataType_FLOAT32, nil
	case mysql.TypeDouble, mysql.TypeNewDecimal:
		return proto.PrimitiveDataType_FLOAT64, nil
	}
	return proto.PrimitiveDataType_PrimitiveDataType_UNDEFINED, fmt.Errorf("convertDataType doesn't support type %v", typ.Tp)
}

func (t *translator) getTensorFromColumn(c *expression.Column, tensors map[int64]*Tensor) (*Tensor, error) {
	tensor, ok := tensors[c.UniqueID]
	if !ok {
		return nil, fmt.Errorf("getTensorFromColumn: unable to find columnID %v", c.UniqueID)
	}
	return tensor, nil
}

func (t *translator) getTensorFromExpression(arg expression.Expression, tensors map[int64]*Tensor) (*Tensor, error) {
	switch x := arg.(type) {
	case *expression.Column:
		return t.getTensorFromColumn(x, tensors)
	case *expression.ScalarFunction:
		return t.buildScalarFunction(x, tensors, false, nil)
	case *expression.Constant:
		return t.addConstantNode(&x.Value)
	}
	return nil, fmt.Errorf("getTensorFromExpression doesn't support arg type %T", arg)
}

func (t *translator) addBroadcastToNodeOndemand(inputs []*Tensor) ([]*Tensor, error) {
	if len(inputs) == 1 {
		return inputs, nil
	}
	outputTs := make([]*Tensor, len(inputs))
	// add broadcast_to node
	var shapeT *Tensor
	var constScalars []*Tensor
	for i, input := range inputs {
		if input.isConstScalar {
			constScalars = append(constScalars, input)
			continue
		}
		shapeT = input
		outputTs[i] = inputs[i]
	}
	if len(constScalars) == 0 {
		return inputs, nil
	}
	if shapeT == nil {
		return nil, fmt.Errorf("unsupported tensors: all tensors are constant")
	}

	outConstTs, err := t.ep.AddBroadcastToNode("broadcast", constScalars, shapeT)
	if err != nil {
		return nil, fmt.Errorf("addBroadcastToNode:%+v", err)
	}
	index := 0
	for i, input := range inputs {
		if input.isConstScalar {
			outputTs[i] = outConstTs[index]
			index += 1
		}
	}
	return outputTs, nil
}

func (t *translator) buildScalarFunction(f *expression.ScalarFunction, tensors map[int64]*Tensor, isApply bool, ln logicalNode) (*Tensor, error) {
	args := f.GetArgs()
	inputs := []*Tensor{}
	for _, arg := range args {
		t, err := t.getTensorFromExpression(arg, tensors)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, t)
	}
	if !isApply {
		var err error
		inputs, err = t.addBroadcastToNodeOndemand(inputs)
		if err != nil {
			return nil, err
		}
	}
	switch f.FuncName.L {
	case ast.UnaryNot:
		if len(inputs) != 1 {
			return nil, fmt.Errorf("buildScalarFunction:err input for %s expected for %d got %d", f.FuncName.L, 1, len(inputs))
		}
		var partyCodes []string
		// filter partycodes
		if inputs[0].Status == proto.TensorStatus_TENSORSTATUS_PRIVATE {
			partyCodes = []string{inputs[0].OwnerPartyCode}
		} else {
			partyCodes = t.enginesInfo.partyInfo.GetParties()
		}
		return t.ep.AddNotNode("not", inputs[0], partyCodes)
	// binary function
	case ast.LT, ast.GT, ast.GE, ast.EQ, ast.LE, ast.NE, ast.LogicOr, ast.LogicAnd, ast.Plus, ast.Minus, ast.Mul, ast.Div, ast.IntDiv, ast.Mod:
		if len(inputs) != 2 {
			return nil, fmt.Errorf("buildScalarFunction:err input for %s expected for %d got %d", f.FuncName.L, 2, len(inputs))
		}
		if isApply {
			if f.FuncName.L == ast.EQ {
				return t.addInNode(f, inputs[0], inputs[1])
			}
			return nil, fmt.Errorf("buildScalarFunction doesn't support function type %v", f.FuncName.L)
		}
		return t.addBinaryNode(astName2NodeName[f.FuncName.L], astName2NodeName[f.FuncName.L], inputs[0], inputs[1])
	case ast.Cast:
		if len(inputs) != 1 {
			return nil, fmt.Errorf("buildScalarFunction:err input for %s expected for %d got %d", f.FuncName.L, 1, len(inputs))
		}
		if inputs[0].DType == proto.PrimitiveDataType_FLOAT32 && slices.Contains([]byte{mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal}, f.RetType.Tp) {
			return inputs[0], nil
		}
		return nil, fmt.Errorf("buildScalarFunction: unimplemented %s with type %v cast as %v", f.FuncName.L, inputs[0].DType, f.RetType)
	case ast.Greatest, ast.Least:
		return t.ep.AddCompareNode(astName2NodeName[f.FuncName.L], astName2NodeName[f.FuncName.L], inputs)
	}
	return nil, fmt.Errorf("buildScalarFunction doesn't support %s", f.FuncName.L)
}

func (t *translator) addConstantNode(value *types.Datum) (*Tensor, error) {
	return t.ep.AddConstantNode("make_constant", value, t.enginesInfo.partyInfo.GetParties())
}

func (t *translator) addBinaryNode(opName string, opType string, left *Tensor, right *Tensor) (*Tensor, error) {
	outCC, err := ccl.InferBinaryOpOutputVisibility(opType, left.cc, right.cc)
	if err != nil {
		return nil, err
	}
	output, err := t.ep.AddBinaryNode(opName, opType, left, right, outCC, t.enginesInfo.partyInfo.GetParties())
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (t *translator) addFilterNode(filter *Tensor, tensorToFilter map[int64]*Tensor) (map[int64]*Tensor, error) {
	// private and share tensors filter have different tensor status
	shareTensors := []*Tensor{}
	var shareIds []int64
	// private tensors need record it's owner party
	privateTensorsMap := make(map[string][]*Tensor)
	privateIdsMap := make(map[string][]int64)
	for _, tensorId := range sliceutil.SortMapKeyForDeterminism(tensorToFilter) {
		it := tensorToFilter[tensorId]
		switch it.Status {
		case proto.TensorStatus_TENSORSTATUS_SECRET:
			shareTensors = append(shareTensors, it)
			shareIds = append(shareIds, tensorId)
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			privateTensorsMap[it.OwnerPartyCode] = append(privateTensorsMap[it.OwnerPartyCode], it)
			privateIdsMap[it.OwnerPartyCode] = append(privateIdsMap[it.OwnerPartyCode], tensorId)
		default:
			return nil, fmt.Errorf("unsupported tensor status for selection node: %+v", it)
		}
	}
	resultIdToTensor := make(map[int64]*Tensor)
	if len(shareTensors) > 0 {
		// convert filter to public here, so filter must be visible to all parties
		publicFilter, err := t.ep.converter.convertTo(filter, &publicPlacement{partyCodes: t.enginesInfo.partyInfo.GetParties()})
		if err != nil {
			return nil, fmt.Errorf("buildSelection: %v", err)
		}
		// handling share tensors here
		output, err := t.ep.AddFilterNode("apply_filter", shareTensors, publicFilter, t.enginesInfo.partyInfo.GetParties())
		if err != nil {
			return nil, fmt.Errorf("buildSelection: %v", err)
		}
		for i, id := range shareIds {
			resultIdToTensor[id] = output[i]
		}
	}
	// handling private tensors here
	if len(privateTensorsMap) > 0 {
		for _, p := range sliceutil.SortMapKeyForDeterminism(privateTensorsMap) {
			ts := privateTensorsMap[p]
			if !filter.cc.IsVisibleFor(p) {
				return nil, fmt.Errorf("failed to check ccl: filter (%+v) is not visible to %s", filter, p)
			}
			newFilter, err := t.ep.converter.convertTo(filter, &privatePlacement{partyCode: p})
			if err != nil {
				return nil, fmt.Errorf("buildSelection: %v", err)
			}
			output, err := t.ep.AddFilterNode("apply_filter", ts, newFilter, []string{p})
			if err != nil {
				return nil, fmt.Errorf("buildSelection: %v", err)
			}
			for i, id := range privateIdsMap[p] {
				resultIdToTensor[id] = output[i]
			}
		}
	}
	return resultIdToTensor, nil
}

func (t *translator) addInNode(f *expression.ScalarFunction, left, right *Tensor) (*Tensor, error) {
	if left.DType != right.DType {
		return nil, fmt.Errorf("addInNode: left type %v should be same with right %v", left.DType, right.DType)
	}
	outCc, err := ccl.InferScalarFuncCCLUsingArgCCL(f, []*ccl.CCL{left.cc, right.cc})
	if err != nil {
		return nil, fmt.Errorf("addInNode: %v", err)
	}
	return t.ep.AddInNode(left, right, outCc)
}

func (t *translator) addConcatNode(inputs []*Tensor) (*Tensor, error) {
	// infer output ccl
	newCC := inputs[0].cc.Clone()
	for _, t := range inputs[1:] {
		newCC.UpdateMoreRestrictedCCLFrom(t.cc)
	}
	for i, p := range t.enginesInfo.partyInfo.GetParties() {
		if newCC.LevelFor(p) == ccl.Unknown {
			errStr := "failed to check union ccl: "
			for _, t := range inputs {
				errStr += fmt.Sprintf(" ccl of child %d is (%v)", i, t.cc)
			}
			return nil, fmt.Errorf(errStr)
		}
	}
	outputT, err := t.ep.AddConcatNode("concat", inputs)
	if err != nil {
		return nil, err
	}
	outputT.cc = newCC
	return outputT, nil
}
