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
	"math"
	"sort"
	"time"

	"golang.org/x/exp/slices"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/planner/core"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

var astName2NodeName = map[string]string{
	ast.If:         operator.OpNameIf,
	ast.Greatest:   operator.OpNameGreatest,
	ast.Least:      operator.OpNameLeast,
	ast.LT:         operator.OpNameLess,
	ast.LE:         operator.OpNameLessEqual,
	ast.GT:         operator.OpNameGreater,
	ast.GE:         operator.OpNameGreaterEqual,
	ast.EQ:         operator.OpNameEqual,
	ast.NE:         operator.OpNameNotEqual,
	ast.LogicOr:    operator.OpNameLogicalOr,
	ast.LogicAnd:   operator.OpNameLogicalAnd,
	ast.Plus:       operator.OpNameAdd,
	ast.UnaryMinus: operator.OpNameMinus,
	ast.Minus:      operator.OpNameMinus,
	ast.Mul:        operator.OpNameMul,
	ast.Div:        operator.OpNameDiv,
	ast.IntDiv:     operator.OpNameIntDiv,
	ast.Mod:        operator.OpNameMod,
	ast.Case:       operator.OpNameCaseWhen,
	ast.DateDiff:   operator.OpNameMinus,
	ast.AddDate:    operator.OpNameAdd,
	ast.SubDate:    operator.OpNameMinus,
	ast.Sin:        operator.OpNameSin,
	ast.Cos:        operator.OpNameCos,
	ast.Acos:       operator.OpNameACos,
}

type translator struct {
	ep              *graph.GraphBuilder
	issuerPartyCode string
	enginesInfo     *graph.EnginesInfo
	sc              *proto.SecurityConfig
	CompileOpts     *proto.CompileOptions

	AffectedByGroupThreshold bool
	converter                *statusConverter
}

func NewTranslator(
	enginesInfo *graph.EnginesInfo,
	sc *proto.SecurityConfig,
	issuerPartyCode string, compileOpts *proto.CompileOptions) (
	*translator, error) {
	if sc == nil {
		return nil, fmt.Errorf("translate: empty CCL")
	}
	// filter out unneeded CCL
	allPartyCodes := map[string]bool{}
	for _, p := range enginesInfo.GetPartyInfo().GetParties() {
		allPartyCodes[p] = true
	}
	allPartyCodes[issuerPartyCode] = true
	newSc := &proto.SecurityConfig{ColumnControlList: []*proto.SecurityConfig_ColumnControl{}}
	for _, cc := range sc.ColumnControlList {
		if allPartyCodes[cc.PartyCode] {
			newSc.ColumnControlList = append(newSc.ColumnControlList, cc)
		}
	}
	builder := graph.NewGraphBuilder(enginesInfo.GetPartyInfo(), compileOpts.Batched)
	return &translator{
		ep:              builder,
		issuerPartyCode: issuerPartyCode,
		sc:              newSc,
		enginesInfo:     enginesInfo,
		CompileOpts:     compileOpts,
		converter:       newStatusConverter(builder),
	}, nil
}

func convertOriginalCCL(sc *proto.SecurityConfig) map[string]*ccl.CCL {
	result := make(map[string]*ccl.CCL)
	toFullQualifiedColumnName := func(dbName, tblName, colName string) string {
		if len(dbName) == 0 && len(tblName) == 0 {
			return colName
		}
		if len(dbName) == 0 {
			return fmt.Sprintf("%s.%s", tblName, colName)
		}
		return fmt.Sprintf("%s.%s.%s", dbName, tblName, colName)
	}
	for _, cc := range sc.ColumnControlList {
		fullQualifiedName := toFullQualifiedColumnName(cc.DatabaseName, cc.TableName, cc.ColumnName)
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

func (t *translator) Translate(lp core.LogicalPlan) (*graph.Graph, error) {
	// preprocessing lp
	processor := LpPrePocessor{}
	if err := processor.process(lp); err != nil {
		return nil, err
	}
	builder, err := newLogicalNodeBuilder(t.issuerPartyCode, t.enginesInfo, convertOriginalCCL(t.sc), t.CompileOpts.GetSecurityCompromise().GetGroupByThreshold())
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
			return nil, status.New(
				proto.Code_CCL_CHECK_FAILED,
				fmt.Sprintf("ccl check failed: the %dth column %s in the result is not visibile (%s) to party %s", i+1, col.OrigName, cc.LevelFor(t.issuerPartyCode).String(), t.issuerPartyCode))
		}
	}
	// find one of the qualified computation parties to act as the query issuer
	if !slices.Contains(t.enginesInfo.GetPartyInfo().GetParties(), t.issuerPartyCode) {
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
		if ln.InsertTableOpt() != nil {
			return nil, fmt.Errorf("translate: not allow to insert table when replaced issuer party")
		}
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
		return t.buildWindow(x)
	case *LimitNode:
		return t.buildLimit(x)
	default:
		return fmt.Errorf("translate: unsupported logical node type %T", ln)
	}
}

func (t *translator) translate(ln logicalNode) (*graph.Graph, error) {
	if err := t.translateInternal(ln); err != nil {
		return nil, err
	}

	if err := t.addResultNode(ln); err != nil {
		return nil, err
	}

	return t.ep.Build(), nil
}

func (t *translator) addResultNode(ln logicalNode) error {
	if ln.IntoOpt() != nil {
		return t.addDumpFileNode(ln)
	} else if ln.InsertTableOpt() != nil {
		return t.addInsertTableNode(ln)
	}
	return t.addPublishNode(ln)
}

func (t *translator) addInsertTableNode(ln logicalNode) error {
	input, output, err := t.prepareResultNodeIo(ln)
	if err != nil {
		return fmt.Errorf("addInsertTableNode: prepare io failed: %v", err)
	}

	return t.ep.AddInsertTableNode("insert_table", input, output, t.issuerPartyCode, ln.InsertTableOpt())
}

func (t *translator) addPublishNode(ln logicalNode) error {
	input, output, err := t.prepareResultNodeIo(ln)
	if err != nil {
		return fmt.Errorf("addPublishNode: prepare io failed: %v", err)
	}

	// Set execution plan's output tensor name
	for _, ot := range output {
		t.ep.OutputName = append(t.ep.OutputName, ot.Name)
	}

	return t.ep.AddPublishNode("publish", input, output, []string{t.issuerPartyCode})
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

	input, output, err := t.prepareResultNodeIo(ln)
	if err != nil {
		return fmt.Errorf("addDumpFileNode: prepare io failed: %v", err)
	}
	return t.ep.AddDumpFileNode("dump_file", input, output, intoOpt)
}

func (t *translator) prepareResultNodeIo(ln logicalNode) (input, output []*graph.Tensor, err error) {
	for i, it := range ln.ResultTable() {
		// Reveal tensor to into party code
		it, err = t.converter.convertTo(it, &privatePlacement{partyCode: t.issuerPartyCode})
		if err != nil {
			return
		}
		input = append(input, it)

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
	return
}

// runSQLString create sql string from lp with dialect
func runSQLString(lp core.LogicalPlan, enginesInfo *graph.EnginesInfo) (sql string, newTableRefs []string, err error) {
	needRewrite := false
	for _, party := range enginesInfo.GetParties() {
		if len(enginesInfo.GetTablesByParty(party)) > 0 {
			needRewrite = true
		}
	}
	var m map[core.DbTable]core.DbTable
	if needRewrite {
		m = enginesInfo.GetDbTableMap()
	}

	return core.RewriteSQLFromLP(lp, m, needRewrite)
}

func (t *translator) buildRunSQL(ln logicalNode, partyCode string) error {
	// check whether tensor is visible to the party code
	for i, col := range ln.Schema().Columns {
		cc := ln.CCL()[col.UniqueID]
		if !cc.IsVisibleFor(partyCode) {
			return fmt.Errorf("ccl check failed: the %dth column %s in the result is not visibile (%s) to party %s", i+1, col.OrigName, cc.LevelFor(partyCode).String(), partyCode)
		}
	}
	sql, newTableRefs, err := runSQLString(ln.LP(), t.enginesInfo)
	if err != nil {
		return fmt.Errorf("addRunSQLNode: failed to rewrite sql=\"%s\", err: %w", sql, err)
	}
	return t.addRunSQLNode(ln, sql, newTableRefs, partyCode)
}

func (t *translator) addRunSQLNode(ln logicalNode, sql string, tableRefs []string, partyCode string) error {
	tensors := []*graph.Tensor{}
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
			tensor.CC = ln.CCL()[column.UniqueID]
		}
		tensors = append(tensors, tensor)
	}
	err := t.ep.AddRunSQLNode("runsql", tensors, sql, tableRefs, partyCode)
	if err != nil {
		return fmt.Errorf("addRunSQLNode: %v", err)
	}
	return ln.SetResultTableWithDTypeCheck(tensors)
}

func assertTensorsBelongTo(tensors []*graph.Tensor, partyCode string) error {
	for _, it := range tensors {
		if it.Status() != proto.TensorStatus_TENSORSTATUS_PRIVATE || it.OwnerPartyCode != partyCode {
			return fmt.Errorf("all tensors must belong to the same party code %s, but got %s", partyCode, it.OwnerPartyCode)
		}
	}
	return nil
}

func (t *translator) addBucketNode(joinKeys []*graph.Tensor, payloads []*graph.Tensor, partyCodes []string) (bucketKeys []*graph.Tensor, bucketPayloads []*graph.Tensor, err error) {
	var joinKeyIds []int
	for _, it := range joinKeys {
		joinKeyIds = append(joinKeyIds, it.ID)
	}
	partyCode := payloads[0].OwnerPartyCode
	if err = assertTensorsBelongTo(append(payloads, joinKeys...), partyCode); err != nil {
		return nil, nil, fmt.Errorf("addBucketNode: %s", err.Error())
	}
	for _, it := range payloads {
		ot := t.ep.AddTensorAs(it)
		if slices.Contains(joinKeyIds, it.ID) {
			bucketKeys = append(bucketKeys, ot)
		}
		bucketPayloads = append(bucketPayloads, ot)
	}
	partyAttr := graph.Attribute{}
	partyAttr.SetStrings(partyCodes)
	_, err = t.ep.AddExecutionNode("bucket", operator.OpNameBucket,
		map[string][]*graph.Tensor{"In": payloads, "Key": joinKeys}, map[string][]*graph.Tensor{graph.Out: bucketPayloads}, map[string]*graph.Attribute{operator.InputPartyCodesAttr: &partyAttr}, []string{partyCode})
	return
}

func (t *translator) addFilterByIndexNode(filter *graph.Tensor, ts []*graph.Tensor, partyCode string) ([]*graph.Tensor, error) {
	// NOTE(xiaoyuan) ts must be private and its owner party code equals to partyCode when apply filter by index
	partyToLocalTensors := map[string][]int{}
	for i, tensor := range ts {
		if tensor.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE && tensor.OwnerPartyCode != "" {
			partyToLocalTensors[tensor.OwnerPartyCode] = append(partyToLocalTensors[tensor.OwnerPartyCode], i)
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
	outTs := make([]*graph.Tensor, len(ts))
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

		inTensors := make([]*graph.Tensor, len(indexes))
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

func (t *translator) buildExpression(expr expression.Expression, tensors map[int64]*graph.Tensor, isApply bool, ln logicalNode) (*graph.Tensor, error) {
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
	case mysql.TypeLong, mysql.TypeDuration:
		return proto.PrimitiveDataType_INT64, nil
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		return proto.PrimitiveDataType_STRING, nil
	case mysql.TypeTiny:
		return proto.PrimitiveDataType_BOOL, nil
	case mysql.TypeFloat:
		return proto.PrimitiveDataType_FLOAT32, nil
	case mysql.TypeDouble, mysql.TypeNewDecimal:
		return proto.PrimitiveDataType_FLOAT64, nil
	case mysql.TypeDatetime, mysql.TypeDate, mysql.TypeYear:
		return proto.PrimitiveDataType_DATETIME, nil
	case mysql.TypeTimestamp:
		return proto.PrimitiveDataType_TIMESTAMP, nil
	}
	return proto.PrimitiveDataType_PrimitiveDataType_UNDEFINED, fmt.Errorf("convertDataType doesn't support type %v", typ.Tp)
}

func (t *translator) getTensorFromColumn(c *expression.Column, tensors map[int64]*graph.Tensor) (*graph.Tensor, error) {
	tensor, ok := tensors[c.UniqueID]
	if !ok {
		return nil, fmt.Errorf("getTensorFromColumn: unable to find columnID %v", c.UniqueID)
	}
	return tensor, nil
}

func (t *translator) getTensorFromExpression(arg expression.Expression, tensors map[int64]*graph.Tensor) (*graph.Tensor, error) {
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

func (t *translator) addBroadcastToNodeOndemand(inputs []*graph.Tensor) ([]*graph.Tensor, error) {
	if len(inputs) == 1 {
		return inputs, nil
	}
	outputTs := make([]*graph.Tensor, len(inputs))
	// add broadcast_to node
	var shapeT *graph.Tensor
	var constScalars []*graph.Tensor
	for i, input := range inputs {
		if input.IsConstScalar {
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
		if input.IsConstScalar {
			outputTs[i] = outConstTs[index]
			index += 1
		}
	}
	return outputTs, nil
}

func (t *translator) convertDegreeToRadians(input *graph.Tensor) (*graph.Tensor, error) {
	coefficient := math.Pi / 180
	degreeToRadians := types.NewFloat64Datum(coefficient)

	constantTensor, err := t.addConstantNode(&degreeToRadians)
	if err != nil {
		return nil, err
	}

	outputs, err := t.addBroadcastToNodeOndemand([]*graph.Tensor{constantTensor, input})
	if err != nil {
		return nil, err
	}

	return t.addBinaryNode(astName2NodeName[ast.Mul], astName2NodeName[ast.Mul], outputs[0], outputs[1])
}

func (t *translator) buildGeoDistanceFunction(inputs []*graph.Tensor) (*graph.Tensor, error) {
	// distance = radius * arc cos(sin(latitude1) * sin(latitude2) + cos(latitude1) * cos(latitude2) * cos(longtitude1 - longtidude2))

	longtitude1Degree := inputs[0]
	latitude1Degree := inputs[1]
	longtitude2Degree := inputs[2]
	latitude2Degree := inputs[3]

	longtitude1, err := t.convertDegreeToRadians(longtitude1Degree)
	if err != nil {
		return nil, err
	}

	latitude1, err := t.convertDegreeToRadians(latitude1Degree)
	if err != nil {
		return nil, err
	}

	longtitude2, err := t.convertDegreeToRadians(longtitude2Degree)
	if err != nil {
		return nil, err
	}

	latitude2, err := t.convertDegreeToRadians(latitude2Degree)
	if err != nil {
		return nil, err
	}

	//sin(latitude1)
	sinLatitude1Tensor, err := t.ep.AddTrigonometricFunction(astName2NodeName[ast.Sin], astName2NodeName[ast.Sin], latitude1, t.extractPartyCodeFromTensor(latitude1))
	if err != nil {
		return nil, err
	}

	//sin(latitude2)
	singLatitude2Tensor, err := t.ep.AddTrigonometricFunction(astName2NodeName[ast.Sin], astName2NodeName[ast.Sin], latitude2, t.extractPartyCodeFromTensor(latitude2))
	if err != nil {
		return nil, err
	}

	// sin(latitude1) * sin(latitude2)
	leftResult, err := t.addBinaryNode(astName2NodeName[ast.Mul], astName2NodeName[ast.Mul], sinLatitude1Tensor, singLatitude2Tensor)
	if err != nil {
		return nil, err
	}

	// cos(latitude1)
	cosLatitude1Tensor, err := t.ep.AddTrigonometricFunction(astName2NodeName[ast.Cos], astName2NodeName[ast.Cos], latitude1, t.extractPartyCodeFromTensor(latitude1))
	if err != nil {
		return nil, err
	}

	// cos(latitude2)
	cosLatitude2Tensor, err := t.ep.AddTrigonometricFunction(astName2NodeName[ast.Cos], astName2NodeName[ast.Cos], latitude2, t.extractPartyCodeFromTensor(latitude2))
	if err != nil {
		return nil, err
	}

	// longtitude1 - longtitude2
	minusLongtitudeTensor, err := t.addBinaryNode(astName2NodeName[ast.Minus], astName2NodeName[ast.Minus], longtitude1, longtitude2)
	if err != nil {
		return nil, err
	}

	// cos(longtitude1 - longtitude2)
	cosMinusLongtitudeTensor, err := t.ep.AddTrigonometricFunction(astName2NodeName[ast.Cos], astName2NodeName[ast.Cos], minusLongtitudeTensor, t.extractPartyCodeFromTensor(minusLongtitudeTensor))
	if err != nil {
		return nil, err
	}

	// cos(latittude1) * cos(latitude2)
	mulTensor1, err := t.addBinaryNode(astName2NodeName[ast.Mul], astName2NodeName[ast.Mul], cosLatitude1Tensor, cosLatitude2Tensor)
	if err != nil {
		return nil, err
	}

	// cos(latitude1) * cos(latitude2) * cos(longtitude1 - longtitude2)
	rightResult, err := t.addBinaryNode(astName2NodeName[ast.Mul], astName2NodeName[ast.Mul], mulTensor1, cosMinusLongtitudeTensor)
	if err != nil {
		return nil, err
	}

	// sin(latitude1) * sin(latitude2) + cos(latitude1) * cos(latitude2) * cos(longtitude1 - longtitude2)
	plusTensor, err := t.addBinaryNode(astName2NodeName[ast.Plus], astName2NodeName[ast.Plus], leftResult, rightResult)

	if err != nil {
		return nil, err
	}

	// arc cos(sin(latitude1) * sin(latitude2) + cos(latitude1) * cos(latitude2) * cos(longtitude1 - longtitude2))
	arcCosTensor, err := t.ep.AddTrigonometricFunction(astName2NodeName[ast.Acos], astName2NodeName[ast.Acos], plusTensor, t.extractPartyCodeFromTensor(plusTensor))
	if err != nil {
		return nil, err
	}

	radius := &graph.Tensor{}
	if len(inputs) == 4 {
		averageRadius := 6371 // average radius of earth from https://simple.wikipedia.org/wiki/Earth_radius
		radiusDatum := types.NewIntDatum(int64(averageRadius))
		radius, err = t.addConstantNode(&radiusDatum)
		if err != nil {
			return nil, err
		}
	} else {
		radius = inputs[4]
	}

	// radius * arc cos(sin(latitude1) * sin(latitude2) + cos(latitude1) * cos(latitude2) * cos(longtitude1 - longtitude2))
	outputs, err := t.addBroadcastToNodeOndemand([]*graph.Tensor{radius, arcCosTensor})
	if err != nil {
		return nil, err
	}

	return t.addBinaryNode(astName2NodeName[ast.Mul], astName2NodeName[ast.Mul], outputs[0], outputs[1])
}

func (t *translator) extractPartyCodeFromTensor(input *graph.Tensor) []string {
	if input.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		return []string{input.OwnerPartyCode}
	} else {
		return t.enginesInfo.GetPartyInfo().GetParties()
	}
}

func (t *translator) buildScalarFunction(f *expression.ScalarFunction, tensors map[int64]*graph.Tensor, isApply bool, ln logicalNode) (*graph.Tensor, error) {
	if f.FuncName.L == ast.Now || f.FuncName.L == ast.Curdate {
		unix_time := time.Unix(1e9, 0)
		seconds := unix_time.Unix()
		date_seconds := time.Date(unix_time.Year(), unix_time.Month(), unix_time.Day(), 0, 0, 0, 0, time.UTC).Unix()
		time_seconds := seconds - date_seconds
		var secondsDatum types.Datum
		if f.FuncName.L == ast.Now {
			secondsDatum = types.NewIntDatum(seconds)
		} else if f.FuncName.L == ast.Curdate {
			secondsDatum = types.NewIntDatum(date_seconds)
		} else {
			secondsDatum = types.NewIntDatum(time_seconds)
		}
		return t.addConstantNode(&secondsDatum)
	}

	args := f.GetArgs()
	inputs := []*graph.Tensor{}
	if f.FuncName.L == ast.AddDate || f.FuncName.L == ast.SubDate {
		var err error
		args, err = expression.TransferDateFuncIntervalToSeconds(args)
		if err != nil {
			return nil, err
		}
	}
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

	inTensorPartyCodes := t.extractPartyCodeFromTensor(inputs[0])
	switch f.FuncName.L {
	case ast.UnaryNot:
		if len(inputs) != 1 {
			return nil, fmt.Errorf("buildScalarFunction:err input for %s expected for %d got %d", f.FuncName.L, 1, len(inputs))
		}
		return t.ep.AddNotNode("not", inputs[0], inTensorPartyCodes)
	case ast.IsNull:
		return t.ep.AddIsNullNode("is_null", inputs[0])
	// binary function
	case ast.LT, ast.GT, ast.GE, ast.EQ, ast.LE, ast.NE, ast.LogicOr, ast.LogicAnd, ast.Plus, ast.Minus, ast.Mul, ast.Div, ast.IntDiv, ast.Mod, ast.AddDate, ast.SubDate, ast.DateDiff:
		if len(inputs) != 2 {
			return nil, fmt.Errorf("buildScalarFunction:err input for %s expected for %d got %d", f.FuncName.L, 2, len(inputs))
		}
		return t.addBinaryNode(astName2NodeName[f.FuncName.L], astName2NodeName[f.FuncName.L], inputs[0], inputs[1])
	case ast.Cast:
		if len(inputs) != 1 {
			return nil, fmt.Errorf("buildScalarFunction:err input for %s expected for %d got %d", f.FuncName.L, 1, len(inputs))
		}
		tp, err := convertDataType(f.RetType)
		if err != nil {
			return nil, fmt.Errorf("buildScalarFunction: %v ", err)
		}
		return t.ep.AddCastNode("cast", tp, inputs[0], inTensorPartyCodes)
	case ast.Greatest, ast.Least:
		return t.ep.AddCompareNode(astName2NodeName[f.FuncName.L], astName2NodeName[f.FuncName.L], inputs)
	case ast.Case:
		if len(inputs)%2 != 1 {
			return nil, fmt.Errorf("missing else clause for CASE WHEN statement")
		}
		var conditions, values []*graph.Tensor
		for i := 0; i < (len(inputs) / 2); i++ {
			conditions = append(conditions, inputs[2*i])
			values = append(values, inputs[2*i+1])
		}
		return t.addCaseWhenNode(astName2NodeName[f.FuncName.L], conditions, values, inputs[len(inputs)-1])
	case ast.If:
		return t.addIfNode(astName2NodeName[f.FuncName.L], inputs[0], inputs[1], inputs[2])
	case ast.UnaryMinus:
		var zero types.Datum
		zero.SetInt64(0)
		zeroTensor, err := t.addConstantNode(&zero)
		if err != nil {
			return nil, fmt.Errorf("buildScalarFunction: unaryminus err: %s", err)
		}
		outputs, err := t.addBroadcastToNodeOndemand([]*graph.Tensor{zeroTensor, inputs[0]})
		if err != nil {
			return nil, fmt.Errorf("buildScalarFunction: unaryminus err: %s", err)
		}
		return t.addBinaryNode(astName2NodeName[f.FuncName.L], astName2NodeName[f.FuncName.L], outputs[0], outputs[1])
	case ast.UnaryPlus:
		return inputs[0], nil
	case ast.Ifnull:
		if len(inputs) != 2 {
			return nil, fmt.Errorf("buildScalarFunction:err input for %s expect %d arguments but got %d", f.FuncName.L, 2, len(inputs))
		}
		return t.ep.AddIfNullNode("ifnull", inputs[0], inputs[1])
	case ast.Coalesce:
		if len(inputs) == 0 {
			return nil, fmt.Errorf("missing arguments for coalesce function")
		}
		return t.ep.AddCoalesceNode("coalesce", inputs)
	case ast.Cos, ast.Sin, ast.Acos:
		if len(inputs) != 1 {
			return nil, fmt.Errorf("incorrect arguments for function %v", f.FuncName.L)
		}

		return t.ep.AddTrigonometricFunction(astName2NodeName[f.FuncName.L], astName2NodeName[f.FuncName.L], inputs[0], inTensorPartyCodes)
	case ast.GeoDist:
		if len(inputs) != 4 && len(inputs) != 5 {
			return nil, fmt.Errorf("incorrect arguments for function %v, exepcting (longittude1, latitude1, longtitude2, latitude2)", f.FuncName.L)
		}

		return t.buildGeoDistanceFunction(inputs)
	}
	return nil, fmt.Errorf("buildScalarFunction doesn't support %s", f.FuncName.L)
}

func (t *translator) addConstantNode(value *types.Datum) (*graph.Tensor, error) {
	return t.ep.AddConstantNode("make_constant", value, t.enginesInfo.GetPartyInfo().GetParties())
}

func (t *translator) addFilterNode(filter *graph.Tensor, tensorToFilter map[int64]*graph.Tensor) (map[int64]*graph.Tensor, error) {
	// private and share tensors filter have different tensor status
	shareTensors := []*graph.Tensor{}
	var shareIds []int64
	// private tensors need record it's owner party
	privateTensorsMap := make(map[string][]*graph.Tensor)
	privateIdsMap := make(map[string][]int64)
	for _, tensorId := range sliceutil.SortMapKeyForDeterminism(tensorToFilter) {
		it := tensorToFilter[tensorId]
		switch it.Status() {
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
	resultIdToTensor := make(map[int64]*graph.Tensor)
	if len(shareTensors) > 0 {
		// convert filter to public here, so filter must be visible to all parties
		publicFilter, err := t.converter.convertTo(filter, &publicPlacement{partyCodes: t.enginesInfo.GetPartyInfo().GetParties()})
		if err != nil {
			return nil, fmt.Errorf("buildSelection: %v", err)
		}
		// handling share tensors here
		output, err := t.ep.AddFilterNode("apply_filter", shareTensors, publicFilter, t.enginesInfo.GetPartyInfo().GetParties())
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
			if !filter.CC.IsVisibleFor(p) {
				return nil, fmt.Errorf("failed to check ccl: filter (%+v) is not visible to %s", filter, p)
			}
			newFilter, err := t.converter.convertTo(filter, &privatePlacement{partyCode: p})
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

// For now, only support psi in
func (t *translator) addInNode(f *expression.ScalarFunction, left, right *graph.Tensor) (*graph.Tensor, error) {
	if left.DType != right.DType {
		return nil, fmt.Errorf("addInNode: left type %v should be same with right %v", left.DType, right.DType)
	}
	outCc, err := ccl.InferScalarFuncCCLUsingArgCCL(f, []*ccl.CCL{left.CC, right.CC})
	if err != nil {
		return nil, fmt.Errorf("addInNode: %v", err)
	}
	creator := algorithmCreator.getCreator(operator.OpNameIn)
	if creator == nil {
		return nil, fmt.Errorf("fail to get algorithm creator for op type %s", operator.OpNameIn)
	}
	algs, err := creator(map[string][]*ccl.CCL{graph.Left: []*ccl.CCL{left.CC},
		graph.Right: []*ccl.CCL{right.CC}}, map[string][]*ccl.CCL{graph.Out: []*ccl.CCL{outCc}}, t.enginesInfo.GetParties())
	if err != nil {
		return nil, fmt.Errorf("addInNode: %v", err)
	}
	for _, alg := range algs {
		pl := alg.outputPlacement[graph.Out][0]
		if pl.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE && pl.partyList()[0] != left.OwnerPartyCode {
			// result maybe need copy to issuer party, so the alg which out party is issuer party should be preferred
			alg.cost.communicationCost += 1
		}
	}
	// select algorithm with the lowest cost
	bestAlg, err := t.getBestAlg(operator.OpNameIn, map[string][]*graph.Tensor{graph.Left: {left}, graph.Right: {right}}, algs)
	if err != nil {
		return nil, err
	}
	// Convert Tensor Status if needed
	if left, err = t.converter.convertTo(left, bestAlg.inputPlacement[graph.Left][0]); err != nil {
		return nil, fmt.Errorf("addBinaryNode: %v", err)
	}
	if right, err = t.converter.convertTo(right, bestAlg.inputPlacement[graph.Right][0]); err != nil {
		return nil, fmt.Errorf("addBinaryNode: %v", err)
	}
	// TODO(xiaoyuan) add share in/local in later
	return t.addPSIInNode(left, right, outCc, bestAlg.outputPlacement[graph.Out][0].partyList()[0], t.CompileOpts.GetOptimizerHints().GetPsiAlgorithmType())
}

// AddPSIInNode adds psi-in node
func (t *translator) addPSIInNode(left *graph.Tensor, right *graph.Tensor, outCCL *ccl.CCL, revealParty string, psiAlg proto.PsiAlgorithmType) (*graph.Tensor, error) {
	nodeName := "psi_in"
	output := t.ep.AddTensor(nodeName + "_out")
	output.Option = proto.TensorOptions_REFERENCE
	output.DType = proto.PrimitiveDataType_BOOL
	output.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	output.OwnerPartyCode = revealParty
	output.CC = outCCL
	attr0 := &graph.Attribute{}
	attr0.SetInt64(int64(psiAlg))
	attr1 := &graph.Attribute{}
	attr1.SetStrings([]string{left.OwnerPartyCode, right.OwnerPartyCode})
	attr2 := &graph.Attribute{}
	attr2.SetStrings([]string{revealParty})
	attrs := map[string]*graph.Attribute{operator.PsiAlgorithmAttr: attr0, operator.InputPartyCodesAttr: attr1, operator.RevealToAttr: attr2}
	if _, err := t.ep.AddExecutionNode(nodeName, operator.OpNameIn, map[string][]*graph.Tensor{graph.Left: {left}, graph.Right: {right}},
		map[string][]*graph.Tensor{graph.Out: {output}}, attrs, []string{left.OwnerPartyCode, right.OwnerPartyCode}); err != nil {
		return nil, err
	}
	return output, nil
}

var binaryOpBoolOutputMap = map[string]bool{
	operator.OpNameLess:         true,
	operator.OpNameLessEqual:    true,
	operator.OpNameGreater:      true,
	operator.OpNameGreaterEqual: true,
	operator.OpNameEqual:        true,
	operator.OpNameNotEqual:     true,
	operator.OpNameLogicalAnd:   true,
	operator.OpNameLogicalOr:    true,
	operator.OpNameIn:           true,
}
var arithOpMap = map[string]bool{
	operator.OpNameAdd:    true,
	operator.OpNameMinus:  true,
	operator.OpNameMul:    true,
	operator.OpNameDiv:    true,
	operator.OpNameIntDiv: true,
	operator.OpNameMod:    true,
}

func inferBinaryOpOutputType(opType string, left, right *graph.Tensor) (proto.PrimitiveDataType, error) {
	if _, ok := binaryOpBoolOutputMap[opType]; ok {
		return proto.PrimitiveDataType_BOOL, nil
	}
	if left.DType == proto.PrimitiveDataType_DATETIME || left.DType == proto.PrimitiveDataType_TIMESTAMP {
		if opType == operator.OpNameIntDiv {
			// the result of datetime/int is no longer time but int (days)
			return proto.PrimitiveDataType_INT64, nil
		}
		// if left and right are both datetime then func must be datediff
		if left.DType == right.DType {
			return proto.PrimitiveDataType_INT64, nil
		}
		return left.DType, nil
	}
	if _, ok := arithOpMap[opType]; ok {
		if opType == operator.OpNameDiv {
			return proto.PrimitiveDataType_FLOAT64, nil
		}
		if opType == operator.OpNameIntDiv || opType == operator.OpNameMod {
			return proto.PrimitiveDataType_INT64, nil
		}

		// prompt result to double if any arguments is float data types (float/double/...)
		if graph.IsFloatOrDoubleType(left.DType) || graph.IsFloatOrDoubleType(right.DType) {
			return proto.PrimitiveDataType_FLOAT64, nil
		}

		// for op +/-/*
		if left.DType == right.DType {
			return left.DType, nil
		}

		return proto.PrimitiveDataType_INT64, nil
	}
	return proto.PrimitiveDataType_PrimitiveDataType_UNDEFINED, fmt.Errorf("cannot infer output type for opType=%s", opType)
}

func (t *translator) addBinaryNode(opName string, opType string, left *graph.Tensor, right *graph.Tensor) (*graph.Tensor, error) {
	if ok := slices.Contains(operator.BinaryOps, opType); !ok {
		return nil, fmt.Errorf("failed to check op type AddBinaryNode: invalid opType %v", opType)
	}
	if err := graph.CheckBinaryOpInputType(opType, left, right); err != nil {
		return nil, fmt.Errorf("addBinaryNode: %w", err)
	}

	outputCCL, err := ccl.InferBinaryOpOutputVisibility(opType, left.CC, right.CC)
	if err != nil {
		return nil, err
	}

	creator := algorithmCreator.getCreator(opType)
	if creator == nil {
		return nil, fmt.Errorf("fail to get algorithm creator for op type %s", opType)
	}
	algs, err := creator(map[string][]*ccl.CCL{graph.Left: []*ccl.CCL{left.CC},
		graph.Right: []*ccl.CCL{right.CC}}, map[string][]*ccl.CCL{graph.Out: []*ccl.CCL{outputCCL}}, t.enginesInfo.GetParties())
	if err != nil {
		return nil, fmt.Errorf("addBinaryNode: %v", err)
	}
	// select algorithm with the lowest cost
	bestAlg, err := t.getBestAlg(opType, map[string][]*graph.Tensor{graph.Left: {left}, graph.Right: {right}}, algs)
	if err != nil {
		return nil, err
	}
	// Convert Tensor Status if needed
	if left, err = t.converter.convertTo(left, bestAlg.inputPlacement[graph.Left][0]); err != nil {
		return nil, fmt.Errorf("addBinaryNode: %v", err)
	}
	if right, err = t.converter.convertTo(right, bestAlg.inputPlacement[graph.Right][0]); err != nil {
		return nil, fmt.Errorf("addBinaryNode: %v", err)
	}
	output := t.ep.AddTensor(opName + "_out")
	output.Option = proto.TensorOptions_REFERENCE
	output.SetStatus(bestAlg.outputPlacement[graph.Out][0].Status())
	if output.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		output.OwnerPartyCode = bestAlg.outputPlacement[graph.Out][0].partyList()[0]
	}
	outputType, err := inferBinaryOpOutputType(opType, left, right)
	if err != nil {
		return nil, err
	}
	output.DType = outputType
	output.CC = outputCCL
	if _, err := t.ep.AddExecutionNode(opName, opType, map[string][]*graph.Tensor{graph.Left: {left}, graph.Right: {right}},
		map[string][]*graph.Tensor{graph.Out: {output}}, map[string]*graph.Attribute{}, bestAlg.outputPlacement[graph.Out][0].partyList()); err != nil {
		return nil, err
	}
	return output, nil
}

// best algorithm has lowest total cost which include data conversion cost and operator cost
func (t *translator) getBestAlg(opType string, inputs map[string][]*graph.Tensor, algs []*materializedAlgorithm) (*materializedAlgorithm, error) {
	var possibleAlgorithms []*materializedAlgorithm
	op, err := operator.FindOpDef(opType)
	if err != nil {
		return nil, err
	}
	for _, alg := range algs {
		unsupportedAlgFlag := false
		for _, key := range sliceutil.SortMapKeyForDeterminism(inputs) {
			tensors := inputs[key]
			for i, tensor := range tensors {
				newPlacement := alg.inputPlacement[key][i]
				cost, err := t.converter.getStatusConversionCost(tensor, newPlacement)
				if err != nil {
					unsupportedAlgFlag = true
					break
				}
				if graph.CheckParamStatusConstraint(op, alg.inputPlacement, alg.outputPlacement) != nil {
					unsupportedAlgFlag = true
					break
				}
				alg.cost.addCost(cost)
			}
		}
		if !unsupportedAlgFlag {
			possibleAlgorithms = append(possibleAlgorithms, alg)
		}
	}
	if len(possibleAlgorithms) == 0 {
		return nil, fmt.Errorf("getBestAlg: failed to find a algorithm")
	}

	bestAlg := possibleAlgorithms[0]
	for _, alg := range possibleAlgorithms[1:] {
		if alg.cost.calculateTotalCost() < bestAlg.cost.calculateTotalCost() {
			bestAlg = alg
		}
	}
	return bestAlg, nil
}

func (t *translator) addGroupAggNode(name string, opType string, groupId, groupNum *graph.Tensor, in []*graph.Tensor, partyCode string) ([]*graph.Tensor, error) {
	placement := &privatePlacement{partyCode: partyCode}
	inP := []*graph.Tensor{}
	for i, it := range in {
		ot, err := t.converter.convertTo(it, placement)
		if err != nil {
			return nil, fmt.Errorf("addGroupAggNode: name %v, opType %v, convert in#%v err: %v", name, opType, i, err)
		}
		inP = append(inP, ot)
	}
	groupIdP, err := t.converter.convertTo(groupId, placement)
	if err != nil {
		return nil, fmt.Errorf("addGroupAggNode: name %v, opType %v, convert group id err: %v", name, opType, err)
	}
	groupNumP, err := t.converter.convertTo(groupNum, placement)
	if err != nil {
		return nil, fmt.Errorf("addGroupAggNode: name %v, opType %v, convert group num err: %v", name, opType, err)
	}

	outputs := []*graph.Tensor{}
	for _, it := range inP {
		output := t.ep.AddTensorAs(it)
		output.Name = output.Name + "_" + name
		if opType == operator.OpNameGroupAvg {
			output.DType = proto.PrimitiveDataType_FLOAT64
		} else if opType == operator.OpNameGroupSum {
			if graph.IsFloatOrDoubleType(it.DType) {
				output.DType = proto.PrimitiveDataType_FLOAT64
			} else {
				output.DType = proto.PrimitiveDataType_INT64
			}
		} else if opType == operator.OpNameGroupCount || opType == operator.OpNameGroupCountDistinct {
			output.DType = proto.PrimitiveDataType_INT64
		}
		outputs = append(outputs, output)
	}
	if _, err := t.ep.AddExecutionNode(name, opType,
		map[string][]*graph.Tensor{
			"GroupId":  {groupIdP},
			"GroupNum": {groupNumP},
			"In":       inP,
		}, map[string][]*graph.Tensor{"Out": outputs},
		map[string]*graph.Attribute{}, []string{partyCode}); err != nil {
		return nil, fmt.Errorf("addGroupAggNode: name %v, opType %v, err %v", name, opType, err)
	}
	return outputs, nil
}

func (t *translator) addUniqueNode(name string, input *graph.Tensor, partyCode string) (*graph.Tensor, error) {
	inputP, err := t.converter.convertTo(input, &privatePlacement{partyCode: partyCode})
	if err != nil {
		return nil, fmt.Errorf("addUniqueNode: %v", err)
	}

	output := t.ep.AddTensorAs(inputP)
	output.Name = "unique_" + output.Name
	output.OwnerPartyCode = partyCode
	if _, err := t.ep.AddExecutionNode(name, operator.OpNameUnique, map[string][]*graph.Tensor{"Key": {inputP}},
		map[string][]*graph.Tensor{"UniqueKey": {output}}, map[string]*graph.Attribute{}, []string{partyCode}); err != nil {
		return nil, err
	}
	return output, nil
}

// if inputs tensors include share data or private tensors coming from different party, make all tensor share
func (t *translator) addSortNode(name string, key, in []*graph.Tensor) ([]*graph.Tensor, error) {
	makeShareFlag := false
	privatePartyCodes := make(map[string]bool)
	for _, tensor := range append(in, key...) {
		switch tensor.Status() {
		case proto.TensorStatus_TENSORSTATUS_SECRET, proto.TensorStatus_TENSORSTATUS_PUBLIC:
			makeShareFlag = true
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			privatePartyCodes[tensor.OwnerPartyCode] = true
		}
		if makeShareFlag {
			break
		}
		if len(privatePartyCodes) > 1 {
			makeShareFlag = true
			break
		}
	}
	var keyA, inA []*graph.Tensor
	// convert inputs to share
	if makeShareFlag {
		for _, in := range key {
			out, err := t.converter.convertTo(
				in, &sharePlacement{partyCodes: t.enginesInfo.GetParties()})
			if err != nil {
				return nil, fmt.Errorf("addSortNode: %v", err)
			}
			keyA = append(keyA, out)
		}
		for _, in := range in {
			out, err := t.converter.convertTo(
				in, &sharePlacement{partyCodes: t.enginesInfo.GetParties()})
			if err != nil {
				return nil, fmt.Errorf("addSortNode: %v", err)
			}
			inA = append(inA, out)
		}
	} else {
		keyA = key
		inA = in
	}

	outA := []*graph.Tensor{}
	for _, in := range inA {
		outA = append(outA, t.ep.AddTensorAs(in))
	}
	var partyCodes []string
	if makeShareFlag {
		partyCodes = t.enginesInfo.GetParties()
	} else {
		for p := range privatePartyCodes {
			partyCodes = append(partyCodes, p)
			break
		}
	}
	if _, err := t.ep.AddExecutionNode(name, operator.OpNameSort,
		map[string][]*graph.Tensor{"Key": keyA, "In": inA}, map[string][]*graph.Tensor{"Out": outA},
		map[string]*graph.Attribute{}, partyCodes); err != nil {
		return nil, fmt.Errorf("addSortNode: %v", err)
	}

	return outA, nil
}

func (t *translator) addObliviousGroupMarkNode(name string, key []*graph.Tensor) (*graph.Tensor, error) {
	var keyA []*graph.Tensor
	// convert inputs to share
	for _, in := range key {
		out, err := t.converter.convertTo(
			in, &sharePlacement{partyCodes: t.enginesInfo.GetParties()})
		if err != nil {
			return nil, fmt.Errorf("addObliviousGroupMarkNode: %v", err)
		}
		keyA = append(keyA, out)
	}

	out := t.ep.AddTensor("group_mark")
	out.Option = proto.TensorOptions_REFERENCE
	out.SetStatus(proto.TensorStatus_TENSORSTATUS_SECRET)
	out.DType = proto.PrimitiveDataType_BOOL

	if _, err := t.ep.AddExecutionNode(name, operator.OpNameObliviousGroupMark,
		map[string][]*graph.Tensor{"Key": keyA}, map[string][]*graph.Tensor{"Group": {out}},
		map[string]*graph.Attribute{}, t.enginesInfo.GetParties()); err != nil {
		return nil, fmt.Errorf("addObliviousGroupMarkNode: %v", err)
	}

	return out, nil
}

func (t *translator) addObliviousGroupAggNode(funcName string, group *graph.Tensor, in *graph.Tensor) (*graph.Tensor, error) {
	opName, ok := operator.ObliviousGroupAggOp[funcName]
	if !ok {
		return nil, fmt.Errorf("addObliviousGroupAggNode: unsupported op %v", funcName)
	}

	// convert inputs to share
	inA, err := t.converter.convertTo(
		in, &sharePlacement{partyCodes: t.enginesInfo.GetParties()})
	if err != nil {
		return nil, fmt.Errorf("addObliviousGroupAggNode: %v", err)
	}

	outA := t.ep.AddTensorAs(inA)
	if funcName == ast.AggFuncAvg {
		outA.DType = proto.PrimitiveDataType_FLOAT64
	} else if funcName == ast.AggFuncCount {
		outA.DType = proto.PrimitiveDataType_INT64
	} else if funcName == ast.AggFuncSum {
		if inA.DType == proto.PrimitiveDataType_BOOL {
			outA.DType = proto.PrimitiveDataType_INT64
		} else if graph.IsFloatOrDoubleType(inA.DType) {
			outA.DType = proto.PrimitiveDataType_FLOAT64
		}
	}
	if _, err := t.ep.AddExecutionNode(funcName, opName,
		map[string][]*graph.Tensor{"Group": {group}, "In": {inA}}, map[string][]*graph.Tensor{"Out": {outA}},
		map[string]*graph.Attribute{}, t.enginesInfo.GetParties()); err != nil {
		return nil, fmt.Errorf("addObliviousGroupAggNode: %v", err)
	}

	return outA, nil
}

func (t *translator) addShuffleNode(name string, inTs []*graph.Tensor) ([]*graph.Tensor, error) {
	var inA []*graph.Tensor
	// convert inputs to share
	for _, in := range inTs {
		out, err := t.converter.convertTo(
			in, &sharePlacement{partyCodes: t.enginesInfo.GetParties()})
		if err != nil {
			return nil, fmt.Errorf("addSortNode: %v", err)
		}
		inA = append(inA, out)
	}
	outA := []*graph.Tensor{}
	for _, in := range inA {
		outA = append(outA, t.ep.AddTensorAs(in))
	}
	if _, err := t.ep.AddExecutionNode(name, operator.OpNameShuffle,
		map[string][]*graph.Tensor{"In": inA}, map[string][]*graph.Tensor{"Out": outA},
		map[string]*graph.Attribute{}, t.enginesInfo.GetParties()); err != nil {
		return nil, fmt.Errorf("addShuffleNode: %v", err)
	}
	return outA, nil
}

func (t *translator) addConcatNode(inputs []*graph.Tensor) (*graph.Tensor, error) {
	// infer output ccl
	newCC := inputs[0].CC.Clone()
	for _, t := range inputs[1:] {
		newCC.UpdateMoreRestrictedCCLFrom(t.CC)
	}
	for i, p := range t.enginesInfo.GetPartyInfo().GetParties() {
		if newCC.LevelFor(p) == ccl.Unknown {
			errStr := "failed to check union ccl: "
			for _, t := range inputs {
				errStr += fmt.Sprintf(" ccl of child %d is (%v)", i, t.CC)
			}
			return nil, fmt.Errorf(errStr)
		}
	}

	newIn := []*graph.Tensor{}
	for _, it := range inputs {
		// concat tensors coming from different party by default
		// make share before adding concat node
		ot, err := t.converter.convertTo(
			it, &sharePlacement{partyCodes: t.enginesInfo.GetParties()})
		if err != nil {
			return nil, fmt.Errorf("addConcatNode: %v", err)
		}
		newIn = append(newIn, ot)
	}
	out := t.ep.AddTensorAs(newIn[0])
	for _, t := range newIn {
		out.SecretStringOwners = append(out.SecretStringOwners, t.SecretStringOwners...)
		if t.DType != out.DType {
			return nil, fmt.Errorf("addConcatNode: not support concat different type, please cast before union")
		}
	}
	out.CC = newCC
	if _, err := t.ep.AddExecutionNode("concat", operator.OpNameConcat,
		map[string][]*graph.Tensor{"In": newIn}, map[string][]*graph.Tensor{graph.Out: {out}},
		map[string]*graph.Attribute{}, t.enginesInfo.GetParties()); err != nil {
		return nil, fmt.Errorf("addConcatNode: %v", err)
	}
	return out, nil
}

func (t *translator) addGroupNode(name string, inputs []*graph.Tensor, partyCode string) (*graph.Tensor, *graph.Tensor, error) {
	newInputs := []*graph.Tensor{}
	for _, it := range inputs {
		ot, err := t.converter.convertTo(it, &privatePlacement{partyCode: partyCode})
		if err != nil {
			return nil, nil, fmt.Errorf("addGroupNode: %v", err)
		}
		newInputs = append(newInputs, ot)
	}
	outputId := t.ep.AddColumn("group_id", proto.TensorStatus_TENSORSTATUS_PRIVATE,
		proto.TensorOptions_REFERENCE, proto.PrimitiveDataType_INT64)
	cc := inputs[0].CC.Clone()
	for _, i := range inputs[1:] {
		cc.UpdateMoreRestrictedCCLFrom(i.CC)
	}
	outputId.OwnerPartyCode = partyCode
	outputId.CC = cc
	outputNum := t.ep.AddTensorAs(outputId)
	outputNum.Name = "group_num"
	if _, err := t.ep.AddExecutionNode(name, operator.OpNameGroup, map[string][]*graph.Tensor{"Key": newInputs},
		map[string][]*graph.Tensor{"GroupId": {outputId}, "GroupNum": {outputNum}}, map[string]*graph.Attribute{}, []string{partyCode}); err != nil {
		return nil, nil, err
	}
	return outputId, outputNum, nil
}

func checkInputTypeNotString(inputs []*graph.Tensor) error {
	for _, input := range inputs {
		if input.DType == proto.PrimitiveDataType_STRING {
			return fmt.Errorf("input type %v is not supported", input.DType)
		}
	}
	return nil
}

func (t *translator) addCaseWhenNode(name string, conds, value []*graph.Tensor, valueElse *graph.Tensor) (*graph.Tensor, error) {
	if err := checkInputTypeNotString(conds); err != nil {
		return nil, fmt.Errorf("addCaseWhenNode: %v", err)
	}
	// check all value are of the same type
	for _, v := range value {
		if v.DType != valueElse.DType {
			return nil, fmt.Errorf("addCaseWhenNode: unmatched dtype in CASE WHEN else dtype(%v) != then dtype(%v)",
				graph.ShortStatus(proto.PrimitiveDataType_name[int32(valueElse.DType)]),
				graph.ShortStatus(proto.PrimitiveDataType_name[int32(v.DType)]),
			)
		}
	}
	out := t.ep.AddTensorAs(valueElse)
	var cond_ccl []*ccl.CCL
	for _, t := range conds {
		out.CC.UpdateMoreRestrictedCCLFrom(t.CC)
		cond_ccl = append(cond_ccl, t.CC)
	}
	var value_ccl []*ccl.CCL
	for _, t := range value {
		out.CC.UpdateMoreRestrictedCCLFrom(t.CC)
		value_ccl = append(value_ccl, t.CC)
	}

	creator := algorithmCreator.getCreator(operator.OpNameCaseWhen)
	if creator == nil {
		return nil, fmt.Errorf("fail to get algorithm creator for op type %s", operator.OpNameCaseWhen)
	}

	algs, err := creator(map[string][]*ccl.CCL{graph.Condition: cond_ccl,
		graph.Value: value_ccl, graph.ValueElse: {valueElse.CC}}, map[string][]*ccl.CCL{graph.Out: {out.CC}}, t.enginesInfo.GetParties())
	if err != nil {
		return nil, fmt.Errorf("addCaseWhenNode: %v", err)
	}

	inputTs := map[string][]*graph.Tensor{graph.Condition: conds, graph.Value: value, graph.ValueElse: {valueElse}}
	// select algorithm with the lowest cost
	bestAlg, err := t.getBestAlg(operator.OpNameCaseWhen, inputTs, algs)
	if err != nil {
		return nil, err
	}
	// Convert Tensor Status if needed
	convertedTs, err := t.converter.convertStatusForMap(inputTs, bestAlg.inputPlacement)
	if err != nil {
		return nil, err
	}
	out.SetStatus(bestAlg.outputPlacement[graph.Out][0].Status())
	if out.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		out.OwnerPartyCode = bestAlg.outputPlacement[graph.Out][0].partyList()[0]
	}

	if _, err := t.ep.AddExecutionNode(name, operator.OpNameCaseWhen, convertedTs, map[string][]*graph.Tensor{"Out": {out}}, map[string]*graph.Attribute{}, bestAlg.outputPlacement[graph.Out][0].partyList()); err != nil {
		return nil, fmt.Errorf("addIfNode: %v", err)
	}
	return out, nil
}

func (t *translator) addGroupHeSumNode(name string, groupId, groupNum, in *graph.Tensor, groupParty, inParty string) (*graph.Tensor, error) {
	partyAttr := &graph.Attribute{}
	partyAttr.SetStrings([]string{groupParty, inParty})

	placement := &privatePlacement{partyCode: groupParty}
	groupIdP, err := t.converter.convertTo(groupId, placement)
	if err != nil {
		return nil, fmt.Errorf("addGroupHeSumNode: name %v, convert group id err: %v", name, err)
	}
	groupNumP, err := t.converter.convertTo(groupNum, placement)
	if err != nil {
		return nil, fmt.Errorf("addGroupHeSumNode: name %v, convert group num err: %v", name, err)
	}
	placement = &privatePlacement{partyCode: inParty}
	inP, err := t.converter.convertTo(in, placement)
	if err != nil {
		return nil, fmt.Errorf("addGroupHeSumNode: name %v, convert agg err: %v", name, err)
	}

	output := t.ep.AddTensorAs(in)
	output.Name = output.Name + "_sum"
	if graph.IsFloatOrDoubleType(in.DType) {
		output.DType = proto.PrimitiveDataType_FLOAT64
	} else {
		output.DType = proto.PrimitiveDataType_INT64
	}
	if _, err := t.ep.AddExecutionNode(name, operator.OpNameGroupHeSum,
		map[string][]*graph.Tensor{
			"GroupId":  {groupIdP},
			"GroupNum": {groupNumP},
			"In":       {inP},
		}, map[string][]*graph.Tensor{"Out": {output}},
		map[string]*graph.Attribute{operator.InputPartyCodesAttr: partyAttr}, []string{groupParty, inParty}); err != nil {
		return nil, fmt.Errorf("addGroupHeSumNode: name %v, opType %v, err %v", name, operator.OpNameGroupHeSum, err)
	}
	return output, nil
}

func (t *translator) addIfNode(name string, cond, tTrue, tFalse *graph.Tensor) (*graph.Tensor, error) {
	if tTrue.DType != tFalse.DType {
		return nil, fmt.Errorf("failed to check data type for true (%s), false (%s)", proto.PrimitiveDataType_name[int32(tTrue.DType)], proto.PrimitiveDataType_name[int32(tFalse.DType)])
	}
	if err := checkInputTypeNotString([]*graph.Tensor{cond}); err != nil {
		return nil, fmt.Errorf("addIfNode: %v", err)
	}
	out := t.ep.AddTensorAs(tTrue)
	// inference
	// result cond -> part of tTrue, part of tFalse
	// result tTrue -> cond, part of tFalse
	out.CC.UpdateMoreRestrictedCCLFrom(tFalse.CC)
	out.CC.UpdateMoreRestrictedCCLFrom(cond.CC)
	creator := algorithmCreator.getCreator(operator.OpNameIf)
	if creator == nil {
		return nil, fmt.Errorf("fail to get algorithm creator for op type %s", operator.OpNameIf)
	}
	algs, err := creator(map[string][]*ccl.CCL{graph.Condition: {cond.CC},
		graph.ValueIfTrue: {tTrue.CC}, graph.ValueIfFalse: {tTrue.CC}}, map[string][]*ccl.CCL{graph.Out: {out.CC}}, t.enginesInfo.GetParties())
	if err != nil {
		return nil, fmt.Errorf("addIfNode: %v", err)
	}
	inputTs := map[string][]*graph.Tensor{graph.Condition: {cond}, graph.ValueIfTrue: {tTrue}, graph.ValueIfFalse: {tFalse}}
	// select algorithm with the lowest cost
	bestAlg, err := t.getBestAlg(operator.OpNameIf, inputTs, algs)
	if err != nil {
		return nil, err
	}
	// Convert Tensor Status if needed
	convertedTs, err := t.converter.convertStatusForMap(inputTs, bestAlg.inputPlacement)
	if err != nil {
		return nil, err
	}
	out.SetStatus(bestAlg.outputPlacement[graph.Out][0].Status())
	if out.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		out.OwnerPartyCode = bestAlg.outputPlacement[graph.Out][0].partyList()[0]
	}
	if _, err := t.ep.AddExecutionNode(name, operator.OpNameIf, convertedTs, map[string][]*graph.Tensor{"Out": {out}}, map[string]*graph.Attribute{}, bestAlg.outputPlacement[graph.Out][0].partyList()); err != nil {
		return nil, fmt.Errorf("addIfNode: %v", err)
	}
	return out, nil
}
