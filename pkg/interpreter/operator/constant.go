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

import "github.com/secretflow/scql/pkg/parser/ast"

const (
	OpNameConstant      string = "Constant"
	OpNameMakePrivate   string = "MakePrivate"
	OpNameMakeShare     string = "MakeShare"
	OpNameMakePublic    string = "MakePublic"
	OpNameFilterByIndex string = "FilterByIndex"
	OpNameBucket        string = "Bucket"
	OpNameJoin          string = "Join"
	OpNameRunSQL        string = "RunSQL"
	OpNamePublish       string = "Publish"
	OpNameDumpFile      string = "DumpFile"
	OpNameInsertTable   string = "InsertTable"
	OpNameCopy          string = "Copy"
	OpNameFilter        string = "Filter"
	OpNameGreatest      string = "Greatest"
	OpNameLeast         string = "Least"
	OpNameIn            string = "In"
	OpNameReplicate     string = "Replicate"
	OpNameBroadcastTo   string = "BroadcastTo"
	OpNameCast          string = "Cast"
	OpNameLimit         string = "Limit"
	OpNameIsNull        string = "IsNull"
	// binary ops
	OpNameLess         string = "Less"
	OpNameLessEqual    string = "LessEqual"
	OpNameGreater      string = "Greater"
	OpNameGreaterEqual string = "GreaterEqual"
	OpNameEqual        string = "Equal"
	OpNameNotEqual     string = "NotEqual"
	OpNameLogicalAnd   string = "LogicalAnd"
	OpNameLogicalOr    string = "LogicalOr"
	// arithmetic ops
	OpNameAdd    string = "Add"
	OpNameMinus  string = "Minus"
	OpNameMul    string = "Mul"
	OpNameDiv    string = "Div"
	OpNameIntDiv string = "IntDiv"
	OpNameMod    string = "Mod"
	OpNameNot    string = "Not"
	OpNameSin    string = "Sin"
	OpNameCos    string = "Cos"
	OpNameACos   string = "ACos"
	// agg
	OpNameReduceSum string = "ReduceSum"
	OpNameReduceMax string = "ReduceMax"
	OpNameReduceMin string = "ReduceMin"
	// OpNameReduceMedian string = "ReduceMedian"
	OpNameReduceAvg   string = "ReduceAvg"
	OpNameReduceCount string = "ReduceCount"

	// private group by
	OpNameGroup              string = "Group"
	OpNameGroupSum           string = "GroupSum"
	OpNameGroupCount         string = "GroupCount"
	OpNameGroupCountDistinct string = "GroupCountDistinct"
	OpNameGroupAvg           string = "GroupAvg"
	OpNameGroupMin           string = "GroupMin"
	OpNameGroupMax           string = "GroupMax"
	OpNameGroupFirstOf       string = "GroupFirstOf"
	OpNameGroupHeSum         string = "GroupHeSum"

	OpNameUnique              string = "Unique"
	OpNameShape               string = "Shape"
	OpNameSort                string = "Sort"
	OpNameObliviousGroupMark  string = "ObliviousGroupMark"
	OpNameObliviousGroupCount string = "ObliviousGroupCount"
	OpNameObliviousGroupSum   string = "ObliviousGroupSum"
	OpNameObliviousGroupMax   string = "ObliviousGroupMax"
	OpNameObliviousGroupMin   string = "ObliviousGroupMin"
	OpNameObliviousGroupAvg   string = "ObliviousGroupAvg"
	OpNameShuffle             string = "Shuffle"
	// union all
	OpNameConcat string = "Concat"
	// condition ops
	OpNameCaseWhen string = "CaseWhen"
	OpNameIf       string = "If"
	OpNameIfNull   string = "IfNull"
	OpNameCoalesce string = "Coalesce"

	OpNameRowNumber string = "RowNumber"
)

const (
	// RevealToAttr, used by MakePrivateOp, PSI_In
	RevealToAttr = `reveal_to`
	// InputPartyCodesAttr, used by PSI_In/Join/Replicate/Copy/HeSum
	InputPartyCodesAttr = `input_party_codes`
	// used by Copy
	OutputPartyCodesAttr = `output_party_codes`
	// used by Limit
	LimitCountAttr  = "count"
	LimitOffsetAttr = "offset"
	// used by WriteTable
	TableNameAttr   = `table_name`
	ColumnNamesAttr = `column_names`
	// AlgorithmAttr
	PsiAlgorithmAttr     = `psi_algorithm`
	InTypeAttr           = `in_type`
	JoinTypeAttr         = `join_type`
	SqlAttr              = `sql`
	TableRefsAttr        = `table_refs`
	ScalarAttr           = `scalar`
	ToStatusAttr         = `to_status`
	FilePathAttr         = `file_path`
	FieldDeliminatorAttr = `field_deliminator`
	QuotingStyleAttr     = `quoting_style`
	LineTerminatorAttr   = `line_terminator`
	AxisAttr             = `axis`
	ReverseAttr          = `reverse`
)

var ReduceAggOp = map[string]string{
	ast.AggFuncSum: OpNameReduceSum,
	ast.AggFuncMax: OpNameReduceMax,
	ast.AggFuncMin: OpNameReduceMin,
	// ast.AggFuncMedian: OpNameReduceMedian,
	ast.AggFuncAvg:   OpNameReduceAvg,
	ast.AggFuncCount: OpNameReduceCount,
}

var ObliviousGroupAggOp = map[string]string{
	ast.AggFuncSum:   OpNameObliviousGroupSum,
	ast.AggFuncMax:   OpNameObliviousGroupMax,
	ast.AggFuncMin:   OpNameObliviousGroupMin,
	ast.AggFuncAvg:   OpNameObliviousGroupAvg,
	ast.AggFuncCount: OpNameObliviousGroupCount,
}

var GroupAggOp = map[string]string{
	ast.AggFuncSum:      OpNameGroupSum,
	ast.AggFuncMax:      OpNameGroupMax,
	ast.AggFuncMin:      OpNameGroupMin,
	ast.AggFuncAvg:      OpNameGroupAvg,
	ast.AggFuncCount:    OpNameGroupCount,
	ast.AggFuncFirstRow: OpNameGroupFirstOf,
}
