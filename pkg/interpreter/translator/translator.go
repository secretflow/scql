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
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"slices"

	"github.com/apache/arrow/go/v17/arrow"

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
	"github.com/secretflow/scql/pkg/util/stringutil"
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
	ast.Asin:       operator.OpNameASin,
	ast.Tan:        operator.OpNameTan,
	ast.Cot:        operator.OpNameCot,
	ast.Atan:       operator.OpNameATan,
	ast.Atan2:      operator.OpNameATan2,
	// tidb parser does not suport acot function
	// ast.Acot:       operator.OpNameACot,
	ast.Abs:     operator.OpNameAbs,
	ast.Ceil:    operator.OpNameCeil,
	ast.Floor:   operator.OpNameFloor,
	ast.Round:   operator.OpNameRound,
	ast.Degrees: operator.OpNameDegrees,
	ast.Radians: operator.OpNameRadians,
	ast.Ln:      operator.OpNameLn,
	ast.Log2:    operator.OpNameLog2,
	ast.Log10:   operator.OpNameLog10,
	ast.Sqrt:    operator.OpNameSqrt,
	ast.Exp:     operator.OpNameExp,
	ast.Pow:     operator.OpNamePow,
}

var floatOutputBinaryOp = map[string]bool{
	operator.OpNameDiv:   true,
	operator.OpNameATan2: true,
}

var skipTypeCheckAst = map[string]bool{
	ast.Pow: true,
}

var unaryOutputType = map[string]proto.PrimitiveDataType{
	ast.Ceil:    proto.PrimitiveDataType_INT64,
	ast.Floor:   proto.PrimitiveDataType_INT64,
	ast.Round:   proto.PrimitiveDataType_INT64,
	ast.Degrees: proto.PrimitiveDataType_FLOAT64,
	ast.Radians: proto.PrimitiveDataType_FLOAT64,
	ast.Ln:      proto.PrimitiveDataType_FLOAT64,
	ast.Log2:    proto.PrimitiveDataType_FLOAT64,
	ast.Log10:   proto.PrimitiveDataType_FLOAT64,
	ast.Sqrt:    proto.PrimitiveDataType_FLOAT64,
	ast.Exp:     proto.PrimitiveDataType_FLOAT64,
}

type translator struct {
	ep              *graph.GraphBuilder
	issuerPartyCode string
	enginesInfo     *graph.EnginesInfo
	sc              *proto.SecurityConfig
	CompileOpts     *proto.CompileOptions

	AffectedByGroupThreshold bool
	converter                *statusConverter
	createdAt                time.Time
}

func NewTranslator(
	enginesInfo *graph.EnginesInfo,
	sc *proto.SecurityConfig,
	issuerPartyCode string, compileOpts *proto.CompileOptions, createdAt time.Time) (
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
		createdAt:       createdAt,
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

func (t *translator) Translate(lp core.LogicalPlan) (*graph.Graph, error) {
	// preprocessing lp
	processor := LpPrePocessor{}
	if err := processor.process(lp); err != nil {
		return nil, err
	}
	builder, err := newLogicalNodeBuilder(t.issuerPartyCode, t.enginesInfo, convertOriginalCCL(t.sc), t.CompileOpts.GetSecurityCompromise().GetGroupByThreshold(), t.CompileOpts.GetOptimizerHints().GetPsiAlgorithmType())
	if err != nil {
		return nil, err
	}
	ln, err := builder.buildLogicalNode(lp)
	if err != nil {
		return nil, err
	}
	// Check if the result is visible to the issuer PartyCode and intoOpt PartyCode
	cclCheckPartyList := []string{t.issuerPartyCode}
	if lp.IntoOpt() != nil {
		// support select into case without partyCode, e.g: select xxx into outfile 'path/to/file'
		if len(lp.IntoOpt().PartyFiles) == 1 && lp.IntoOpt().PartyFiles[0].PartyCode == "" {
			lp.IntoOpt().PartyFiles[0].PartyCode = t.issuerPartyCode
		}
		for _, partyFile := range lp.IntoOpt().PartyFiles {
			if partyFile.PartyCode != t.issuerPartyCode {
				cclCheckPartyList = append(cclCheckPartyList, partyFile.PartyCode)
			}
		}
	}
	for _, partyCode := range cclCheckPartyList {
		for i, col := range ln.Schema().Columns {
			cc := ln.CCL()[col.UniqueID]
			if !cc.IsVisibleFor(partyCode) {
				return nil, status.New(
					proto.Code_CCL_CHECK_FAILED,
					fmt.Sprintf("ccl check failed: the %dth column %s in the result is not visibile (%s) to party %s", i+1, col.OrigName, cc.LevelFor(partyCode).String(), partyCode))
			}
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
		candidateParties = sliceutil.Exclude(candidateParties, t.issuerPartyCode)
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
	case *SortNode:
		return t.buildSort(x)
	case *MaxOneRowNode:
		return t.buildMaxOneRow(x)
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

	for _, partyFile := range intoOpt.PartyFiles {
		input, output, err := t.prepareResultNodeIoForParty(ln, partyFile.PartyCode)
		if err != nil {
			return fmt.Errorf("addDumpFileNode: prepare io failed: %v", err)
		}
		err = t.ep.AddDumpFileNodeForParty("dump_file", input, output, intoOpt, partyFile)
		if err != nil {
			return fmt.Errorf("AddDumpFileNode: %v", err)
		}
	}
	return nil
}

func (t *translator) prepareResultNodeIo(ln logicalNode) (input, output []*graph.Tensor, err error) {
	return t.prepareResultNodeIoForParty(ln, t.issuerPartyCode)
}

func (t *translator) prepareResultNodeIoForParty(ln logicalNode, partyCode string) (input, output []*graph.Tensor, err error) {
	for i, it := range ln.ResultTable() {
		// Reveal tensor to into party code
		it, err = t.converter.convertTo(it, &privatePlacement{partyCode: partyCode})
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
	partyCode := joinKeys[0].OwnerPartyCode
	if err = assertTensorsBelongTo(append(payloads, joinKeys...), partyCode); err != nil {
		return nil, nil, fmt.Errorf("addBucketNode: %s", err.Error())
	}
	bucketTsMap := make(map[int]*graph.Tensor)
	for _, it := range payloads {
		ot := t.ep.AddTensorAs(it)
		bucketTsMap[it.ID] = ot
		bucketPayloads = append(bucketPayloads, ot)
	}
	for _, it := range joinKeys {
		if _, ok := bucketTsMap[it.ID]; ok {
			continue
		}
		ot := t.ep.AddTensorAs(it)
		bucketTsMap[it.ID] = ot
	}
	bucketKeys = make([]*graph.Tensor, len(joinKeyIds))
	for i, id := range joinKeyIds {
		bucketKeys[i] = bucketTsMap[id]
	}
	partyAttr := graph.Attribute{}
	partyAttr.SetStrings(partyCodes)
	_, err = t.ep.AddExecutionNode("bucket", operator.OpNameBucket,
		map[string][]*graph.Tensor{"In": mergeTensors(joinKeys, payloads), "Key": joinKeys}, map[string][]*graph.Tensor{graph.Out: mergeTensors(bucketKeys, bucketPayloads)}, map[string]*graph.Attribute{operator.InputPartyCodesAttr: &partyAttr}, []string{partyCode})
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
		return t.buildScalarFunction(x, tensors, isApply)
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
		return t.buildScalarFunction(x, tensors, false)
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

func (t *translator) buildCompareNode(name string, inputs []*graph.Tensor) (*graph.Tensor, error) {
	nodeInputs := []*graph.Tensor{}

	allVisibleParty, err := t.findPartyWithAccessToAllTensors(inputs)
	if err != nil {
		return nil, err
	}

	var parties []string
	if allVisibleParty == "" {
		for _, input := range inputs {
			it, err := t.converter.convertTo(input, &sharePlacement{partyCodes: t.enginesInfo.GetParties()})
			if err != nil {
				return nil, err
			}

			nodeInputs = append(nodeInputs, it)
		}
		parties = t.enginesInfo.GetParties()
	} else {
		parties = []string{allVisibleParty}
		for _, input := range inputs {
			if input.OwnerPartyCode != allVisibleParty {
				it, err := t.converter.convertTo(input, &privatePlacement{partyCode: allVisibleParty})
				if err != nil {
					return nil, err
				}

				nodeInputs = append(nodeInputs, it)
			} else {
				nodeInputs = append(nodeInputs, input)
			}
		}
	}

	return t.ep.AddVariadicCompareNode(name, name, nodeInputs, parties)
}

func (t *translator) buildScalarFunction(f *expression.ScalarFunction, tensors map[int64]*graph.Tensor, isApply bool) (*graph.Tensor, error) {
	if f.FuncName.L == ast.Now || f.FuncName.L == ast.Curdate {
		unix_time := t.createdAt
		date := time.Date(unix_time.Year(), unix_time.Month(), unix_time.Day(), 0, 0, 0, 0, t.createdAt.Location())
		var timeDatum types.Datum
		if f.FuncName.L == ast.Now {
			timeDatum = types.NewTimeDatum(types.NewTime(types.NewMysqlTime(unix_time), mysql.TypeDate, types.DefaultFsp))
		} else if f.FuncName.L == ast.Curdate {
			timeDatum = types.NewTimeDatum(types.NewTime(types.NewMysqlTime(date), mysql.TypeDate, types.DefaultFsp))
		} else {
			return nil, fmt.Errorf("buildScalarFunction: unsupported function %s", f.FuncName.L)
		}
		return t.addConstantNode(&timeDatum)
	}

	args := f.GetArgs()
	if f.FuncName.L == ast.Substr || f.FuncName.L == ast.Substring {
		strInput, err := t.getTensorFromExpression(args[0], tensors)
		if err != nil {
			return nil, fmt.Errorf("buildScalarFunction:err input for %s", f.FuncName.L)
		}
		if strInput.DType != proto.PrimitiveDataType_STRING {
			// TODO: support other types
			return nil, fmt.Errorf("buildScalarFunction:err input type{%s} for %s", strInput.DType, f.FuncName.L)
		}
		opt := SliceOptions{
			Start: 0,
			Stop:  math.MaxInt64,
			Step:  1,
		}
		// substr(str, start) or substr(str, start, length)
		if len(args) >= 2 {
			// arrow index start from 0 while mysql substr start from 1
			start, ok := args[1].(*expression.Constant)
			if !ok {
				return nil, fmt.Errorf("buildScalarFunction: args[1] should be constant for %s", f.FuncName.L)
			}
			opt.Start = start.Value.GetInt64() - 1
		}
		// substr(str, start, length)
		if len(args) >= 3 {
			length, ok := args[2].(*expression.Constant)
			if !ok {
				return nil, fmt.Errorf("buildScalarFunction: args[2] should be constant for %s", f.FuncName.L)
			}
			opt.Stop = opt.Start + length.Value.GetInt64()
		}

		return t.ep.AddArrowFuncNode("arrow_substr", "utf8_slice_codeunits", opt, []*graph.Tensor{strInput}, strInput.DType)
	}

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
	if !isApply && f.FuncName.L != ast.StrToDate {
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
			return nil, fmt.Errorf("buildScalarFunction: err input for %s expected for %d got %d", f.FuncName.L, 1, len(inputs))
		}
		return t.ep.AddNotNode("not", inputs[0], inTensorPartyCodes)
	case ast.IsNull:
		return t.ep.AddIsNullNode("is_null", inputs[0])
	case ast.Lower:
		return t.ep.AddArrowFuncNode("arrow_lower", "utf8_lower", nil, inputs, inputs[0].DType)
	case ast.Upper:
		return t.ep.AddArrowFuncNode("arrow_upper", "utf8_upper", nil, inputs, inputs[0].DType)
	case ast.Trim:
		// TODO: support more trim options, only support trim whitespace now
		return t.ep.AddArrowFuncNode("arrow_trim", "utf8_trim", TrimOptions{Characters: " "}, inputs, inputs[0].DType)
	case ast.Concat:
		var separator types.Datum
		separator.SetString("") // only support nil separator now
		sepTensor, err := t.addConstantNode(&separator)
		if err != nil {
			return nil, fmt.Errorf("buildScalarFunction: build separator for concat failed: %v ", err)
		}
		// arrow function 'binary_join_element_wise' treats the last input as separator
		ins, err := t.addBroadcastToNodeOndemand(append(inputs, sepTensor))
		if err != nil {
			return nil, fmt.Errorf("buildScalarFunction: broadcast for concat failed: %v ", err)
		}
		var privateIns []*graph.Tensor
		for _, in := range ins {
			privateIn, err := t.converter.convertTo(in, &privatePlacement{partyCode: t.issuerPartyCode})
			if err != nil {
				return nil, fmt.Errorf("buildScalarFunction: convert inputs to issuer '%s' failed: %v", ins[0].OwnerPartyCode, err)
			}
			privateIns = append(privateIns, privateIn)
		}

		return t.ep.AddArrowFuncNode("arrow_concat", "binary_join_element_wise", nil, privateIns, proto.PrimitiveDataType_STRING)
	// binary function
	case ast.LT, ast.GT, ast.GE, ast.EQ, ast.LE, ast.NE, ast.LogicOr, ast.LogicAnd, ast.Plus, ast.Minus, ast.Mul, ast.Div, ast.IntDiv, ast.Mod, ast.AddDate, ast.SubDate, ast.DateDiff, ast.Pow:
		if len(inputs) != 2 {
			return nil, fmt.Errorf("buildScalarFunction:err input for %s expected for %d got %d", f.FuncName.L, 2, len(inputs))
		}

		output, err := t.addBinaryNode(astName2NodeName[f.FuncName.L], astName2NodeName[f.FuncName.L], inputs[0], inputs[1])
		if skipTypeCheckAst[f.FuncName.L] {
			output.SkipDTypeCheck = true
		}
		return output, err
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
		return t.buildCompareNode(astName2NodeName[f.FuncName.L], inputs)
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
	case ast.Cot:
		if len(inputs) != 1 {
			return nil, fmt.Errorf("incorrect arguments for function %v", f.FuncName.L)
		}

		one := types.NewIntDatum(int64(1))
		oneConst, err := t.addConstantNode(&one)
		if err != nil {
			return nil, fmt.Errorf("buildScalarFunction: err %s", err)
		}

		tanOp, err := t.ep.AddTrigonometricFunction(astName2NodeName[ast.Tan], astName2NodeName[ast.Tan], inputs[0], inTensorPartyCodes)
		if err != nil {
			return nil, fmt.Errorf("buildScalarFunction: err %s", err)
		}

		oneScalar, err := t.addBroadcastToNodeOndemand([]*graph.Tensor{oneConst, tanOp})
		if err != nil {
			return nil, fmt.Errorf("buildScalarFunction: err %s", err)
		}
		return t.addBinaryNode(astName2NodeName[ast.Div], astName2NodeName[ast.Div], oneScalar[0], oneScalar[1])
	case ast.Cos, ast.Sin, ast.Acos, ast.Asin, ast.Tan, ast.Atan:
		if len(inputs) != 1 {
			return nil, fmt.Errorf("incorrect arguments for function %v", f.FuncName.L)
		}

		return t.ep.AddTrigonometricFunction(astName2NodeName[f.FuncName.L], astName2NodeName[f.FuncName.L], inputs[0], inTensorPartyCodes)
	case ast.GeoDist:
		if len(inputs) != 4 && len(inputs) != 5 {
			return nil, fmt.Errorf("incorrect arguments for function %v, exepcting (longittude1, latitude1, longtitude2, latitude2)", f.FuncName.L)
		}

		return t.buildGeoDistanceFunction(inputs)
	case ast.Atan2:
		if len(inputs) != 2 {
			return nil, fmt.Errorf("incorrect arguments for function ATAN2, expecting 2 but get %v", len(inputs))
		}

		return t.addBinaryNode(astName2NodeName[f.FuncName.L], astName2NodeName[f.FuncName.L], inputs[0], inputs[1])
	case ast.Abs, ast.Ceil, ast.Floor, ast.Round, ast.Radians, ast.Degrees, ast.Ln, ast.Log10, ast.Log2, ast.Sqrt, ast.Exp:
		if len(inputs) != 1 {
			return nil, fmt.Errorf("buildScalarFunction: incorrect arguments for function %v, exepcting 1 but got %v", f.FuncName.L, len(inputs))
		}
		var outputType proto.PrimitiveDataType
		var exist bool
		if outputType, exist = unaryOutputType[f.FuncName.L]; !exist {
			outputType = inputs[0].DType
		}

		return t.ep.AddUnaryNumericNode(astName2NodeName[f.FuncName.L], astName2NodeName[f.FuncName.L], inputs[0], outputType, inTensorPartyCodes)
	case ast.StrToDate:
		if len(inputs) != 2 {
			return nil, fmt.Errorf("incorrect arguments for function StrToDate, expecting 2 but get %v", len(inputs))
		}

		left, right := inputs[0], inputs[1]
		dateStrExpr, formatStrExpr := f.GetArgs()[0], f.GetArgs()[1]
		dateStrConst, dateIsConst := dateStrExpr.(*expression.Constant)
		formatStrConst, formatIsConst := formatStrExpr.(*expression.Constant)

		if left.DType != proto.PrimitiveDataType_STRING {
			return nil, fmt.Errorf("buildScalarFunction: invalid left argument type for the STR_TO_DATE function, exepecting string but got %s", proto.PrimitiveDataType_name[int32(left.DType)])
		}
		if right.DType != proto.PrimitiveDataType_STRING || !formatIsConst {
			return nil, fmt.Errorf("buildScalarFunction: invalid right argument type for the STR_TO_DATE function, exepecting constant string but got %s", proto.PrimitiveDataType_name[int32(right.DType)])
		}

		formatStr := formatStrConst.Value.GetString()
		// add constant node for constant dateStr - str_to_date('2025-06-05', '%Y-%m-%d')
		if dateIsConst && formatIsConst {
			goLayout, err := stringutil.MySQLDateFormatToGoLayout(formatStr)
			if err != nil {
				return nil, fmt.Errorf("buildScalarFunction: STR_TO_DATE format string '%s' is invalid: %w", formatStr, err)
			}

			dateStr := dateStrConst.Value.GetString()
			parsedTime, err := time.Parse(goLayout, dateStr)
			if err != nil {
				return nil, fmt.Errorf("buildScalarFunction: STR_TO_DATE date string '%s' is invalid: %w", dateStr, err)
			}

			unixSec := parsedTime.Unix()
			var datum types.Datum
			datum.SetInt64(unixSec)

			return t.addConstantNode(&datum)
		}

		arrowFormatStr, err := stringutil.MySQLDateFormatToArrowFormat(formatStr)
		if err != nil {
			return nil, fmt.Errorf("buildScalarFunction: STR_TO_DATE format string '%s' is invalid: %w", formatStr, err)
		}

		opt := &StrptimeOptions{
			Format:      arrowFormatStr,
			Unit:        arrow.Second,
			ErrorIsNull: true,
		}

		return t.ep.AddArrowFuncNode("str_to_date", "strptime", opt, []*graph.Tensor{inputs[0]}, proto.PrimitiveDataType_DATETIME)
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
	for tensorId, it := range sliceutil.SortedMap(tensorToFilter) {
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
			return nil, fmt.Errorf("addFilterNode: %v", err)
		}
		// handling share tensors here
		output, err := t.ep.AddFilterNode("apply_filter", shareTensors, publicFilter, t.enginesInfo.GetPartyInfo().GetParties())
		if err != nil {
			return nil, fmt.Errorf("addFilterNode: %v", err)
		}
		for i, id := range shareIds {
			resultIdToTensor[id] = output[i]
		}
	}
	// handling private tensors here
	if len(privateTensorsMap) > 0 {
		for p, ts := range sliceutil.SortedMap(privateTensorsMap) {
			if !filter.CC.IsVisibleFor(p) {
				return nil, fmt.Errorf("failed to check ccl: filter (%+v) is not visible to %s", filter, p)
			}
			newFilter, err := t.converter.convertTo(filter, &privatePlacement{partyCode: p})
			if err != nil {
				return nil, fmt.Errorf("addFilterNode: %v", err)
			}
			output, err := t.ep.AddFilterNode("apply_filter", ts, newFilter, []string{p})
			if err != nil {
				return nil, fmt.Errorf("addFilterNode: %v", err)
			}
			for i, id := range privateIdsMap[p] {
				resultIdToTensor[id] = output[i]
			}
		}
	}
	return resultIdToTensor, nil
}

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
	algs, err := creator(map[string][]*ccl.CCL{graph.Left: {left.CC},
		graph.Right: {right.CC}}, map[string][]*ccl.CCL{graph.Out: {outCc}}, t.enginesInfo.GetParties())
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
	// TODO(xiaoyuan) add share in later
	return t.addBasicInNode(left, right, outCc, bestAlg.outputPlacement[graph.Out][0].partyList()[0], t.CompileOpts.GetOptimizerHints().GetPsiAlgorithmType())
}

func (t *translator) addBasicInNode(left *graph.Tensor, right *graph.Tensor, outCCL *ccl.CCL, revealParty string, psiAlg proto.PsiAlgorithmType) (*graph.Tensor, error) {
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
	if left.OwnerPartyCode == right.OwnerPartyCode {
		localInAttr := &graph.Attribute{}
		localInAttr.SetInt64(operator.LocalIn)
		attrs[operator.InTypeAttr] = localInAttr
	}
	parties := []string{left.OwnerPartyCode, right.OwnerPartyCode}
	parties = sliceutil.SliceDeDup(parties)
	if _, err := t.ep.AddExecutionNode(nodeName, operator.OpNameIn, map[string][]*graph.Tensor{graph.Left: {left}, graph.Right: {right}},
		map[string][]*graph.Tensor{graph.Out: {output}}, attrs, parties); err != nil {
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
	operator.OpNameATan2:  true,
	operator.OpNamePow:    true,
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
		if floatOutputBinaryOp[opType] {
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

var constTensorNeedCastOp = map[string]bool{
	operator.OpNameLess:         true,
	operator.OpNameLessEqual:    true,
	operator.OpNameGreater:      true,
	operator.OpNameGreaterEqual: true,
	operator.OpNameEqual:        true,
	operator.OpNameAdd:          true,
	operator.OpNameMinus:        true,
}

func (t *translator) addBinaryNode(opName string, opType string, left *graph.Tensor, right *graph.Tensor) (*graph.Tensor, error) {
	if ok := slices.Contains(operator.BinaryOps, opType); !ok {
		return nil, fmt.Errorf("failed to check op type AddBinaryNode: invalid opType %v", opType)
	}

	// only support string to time currently
	if _, ok := constTensorNeedCastOp[opType]; ok {
		var err error
		const ConstantDataName = "constant_data"

		castIfNeeded := func(constTensor, otherTensor *graph.Tensor, targetType proto.PrimitiveDataType) (*graph.Tensor, error) {
			if (constTensor.Name == ConstantDataName && constTensor.DType == proto.PrimitiveDataType_STRING) && (otherTensor.DType == proto.PrimitiveDataType_DATETIME || otherTensor.DType == proto.PrimitiveDataType_TIMESTAMP) {
				inTensorPartyCodes := t.extractPartyCodeFromTensor(constTensor)
				return t.ep.AddCastNode("cast", targetType, constTensor, inTensorPartyCodes)
			}
			return constTensor, nil
		}

		if left.Name == ConstantDataName {
			left, err = castIfNeeded(left, right, right.DType)
			if err != nil {
				return nil, err
			}
		} else if right.Name == ConstantDataName {
			right, err = castIfNeeded(right, left, left.DType)
			if err != nil {
				return nil, err
			}
		}
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
	algs, err := creator(map[string][]*ccl.CCL{graph.Left: {left.CC},
		graph.Right: {right.CC}}, map[string][]*ccl.CCL{graph.Out: {outputCCL}}, t.enginesInfo.GetParties())
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
		for key, tensors := range sliceutil.SortedMap(inputs) {
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

func (t *translator) addGroupAggNode(name string, opType string, groupId, groupNum *graph.Tensor, in []*graph.Tensor, partyCode string, attr map[string]*graph.Attribute) ([]*graph.Tensor, error) {
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
		attr, []string{partyCode}); err != nil {
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
func (t *translator) addSortNode(name string, key, in []*graph.Tensor, reverse bool) ([]*graph.Tensor, error) {
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

	sortAttr := make(map[string]*graph.Attribute)
	reverseAttr := &graph.Attribute{}
	reverseAttr.SetBool(reverse)
	sortAttr[operator.ReverseAttr] = reverseAttr
	if _, err := t.ep.AddExecutionNode(name, operator.OpNameSort,
		map[string][]*graph.Tensor{"Key": keyA, "In": inA}, map[string][]*graph.Tensor{"Out": outA},
		sortAttr, partyCodes); err != nil {
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

func (t *translator) addObliviousGroupAggNode(funcName string, group *graph.Tensor, in *graph.Tensor, attr map[string]*graph.Attribute) (*graph.Tensor, error) {
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
	if funcName == ast.AggFuncAvg || funcName == ast.WindowFuncPercentRank {
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
		attr, t.enginesInfo.GetParties()); err != nil {
		return nil, fmt.Errorf("addObliviousGroupAggNode: %v", err)
	}

	return outA, nil
}

func (t *translator) addShuffleNode(name string, inTs []*graph.Tensor) ([]*graph.Tensor, error) {
	var inA []*graph.Tensor
	// convert inputs to share
	for _, in := range inTs {
		if in.Status() == proto.TensorStatus_TENSORSTATUS_PUBLIC {
			inA = append(inA, in)
			continue
		}
		out, err := t.converter.convertTo(
			in, &sharePlacement{partyCodes: t.enginesInfo.GetParties()})
		if err != nil {
			return nil, fmt.Errorf("addShuffleNode: %v", err)
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
			return nil, errors.New(errStr)
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

func (t *translator) addWindowNode(name string,
	key []*graph.Tensor,
	partitionId *graph.Tensor,
	partitionNum *graph.Tensor,
	output *graph.Tensor,
	attr *graph.Attribute,
	party string) error {
	if _, err := t.ep.AddExecutionNode(name, name,
		map[string][]*graph.Tensor{"Key": key, "PartitionId": {partitionId}, "PartitionNum": {partitionNum}},
		map[string][]*graph.Tensor{"Out": {output}},
		map[string]*graph.Attribute{operator.ReverseAttr: attr},
		[]string{party}); err != nil {
		return err
	}

	return nil
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

	output := t.ep.AddTensorAs(inP)
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

func (t *translator) buildCrossJoin(left logicalNode, right logicalNode) (map[int64]*graph.Tensor, error) {
	rval := map[int64]*graph.Tensor{}
	leftOutput := []*graph.Tensor{}
	for i, tensor := range left.ResultTable() {
		tt := t.ep.AddTensorAs(tensor)
		leftOutput = append(leftOutput, tt)
		rval[left.Schema().Columns[i].UniqueID] = tt
	}
	leftPartyCode := left.ResultTable()[0].OwnerPartyCode

	rightOutput := []*graph.Tensor{}
	for i, tensor := range right.ResultTable() {
		tt := t.ep.AddTensorAs(tensor)
		rightOutput = append(rightOutput, tt)
		rval[right.Schema().Columns[i].UniqueID] = tt
	}
	rightPartyCode := right.ResultTable()[0].OwnerPartyCode
	attr := &graph.Attribute{}
	attr.SetStrings([]string{leftPartyCode, rightPartyCode})
	_, err := t.ep.AddReplicateNode(operator.OpNameReplicate, left.ResultTable(), right.ResultTable(),
		leftOutput, rightOutput, map[string]*graph.Attribute{operator.InputPartyCodesAttr: attr}, sliceutil.SliceDeDup([]string{leftPartyCode, rightPartyCode}))
	if err != nil {
		return nil, fmt.Errorf("buildCrossJoin: %v", err)
	}

	return rval, nil
}
