// Copyright 2019 PingCAP, Inc.
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

package plancodec

import "strconv"

const (
	// TypeSel is the type of Selection.
	TypeSel = "Selection"
	// TypeSet is the type of Set.
	TypeSet = "Set"
	// TypeProj is the type of Projection.
	TypeProj = "Projection"
	// TypeAgg is the type of Aggregation.
	TypeAgg = "Aggregation"
	// TypeHashAgg is the type of HashAgg.
	TypeHashAgg = "HashAgg"
	// TypeShow is the type of show.
	TypeShow = "Show"
	// TypeJoin is the type of Join.
	TypeJoin = "Join"
	// TypeUnion is the type of Union.
	TypeUnion = "Union"
	// TypeTableScan is the type of TableScan.
	TypeTableScan = "TableScan"
	// TypeDual is the type of TableDual.
	TypeDual = "TableDual"
	// TypeUnionScan is the type of UnionScan.
	TypeUnionScan = "UnionScan"
	// TypeSort is the type of Sort.
	TypeSort = "Sort"
	// TypeTopN is the type of TopN.
	TypeTopN = "TopN"
	// TypeLimit is the type of Limit.
	TypeLimit = "Limit"
	// TypeHashLeftJoin is the type of left hash join.
	TypeHashLeftJoin = "HashLeftJoin"
	// TypeHashRightJoin is the type of right hash join.
	TypeHashRightJoin = "HashRightJoin"
	// TypeMergeJoin is the type of merge join.
	TypeMergeJoin = "MergeJoin"
	// TypeIndexJoin is the type of index look up join.
	TypeIndexJoin = "IndexJoin"
	// TypeIndexMergeJoin is the type of index look up merge join.
	TypeIndexMergeJoin = "IndexMergeJoin"
	// TypeIndexHashJoin is the type of index nested loop hash join.
	TypeIndexHashJoin = "IndexHashJoin"
	// TypeApply is the type of Apply.
	TypeApply = "Apply"
	// TypeMaxOneRow is the type of MaxOneRow.
	TypeMaxOneRow = "MaxOneRow"
	// TypeExists is the type of Exists.
	TypeExists = "Exists"
	// TypeTableReader is the type of TableReader.
	TypeTableReader = "TableReader"
	// TypeWindow is the type of Window.
	TypeWindow = "Window"
)

// plan id.
const (
	typeSelID int = iota + 1
	typeSetID
	typeProjID
	typeAggID
	typeStreamAggID
	typeHashAggID
	typeShowID
	typeJoinID
	typeUnionID
	typeTableScanID
	typeUnionScanID
	typeIdxScanID
	typeSortID
	typeTopNID
	typeLimitID
	typeHashLeftJoinID
	typeHashRightJoinID
	typeMergeJoinID
	typeIndexJoinID
	typeIndexMergeJoinID
	typeIndexHashJoinID
	typeApplyID
	typeMaxOneRowID
	typeExistsID
	typeTableReaderID
	typeIndexMergeID
	typePointGet
	typeShowDDLJobs
	typeBatchPointGet
)

// TypeStringToPhysicalID converts the plan type string to plan id.
func TypeStringToPhysicalID(tp string) int {
	switch tp {
	case TypeSel:
		return typeSelID
	case TypeSet:
		return typeSetID
	case TypeProj:
		return typeProjID
	case TypeAgg:
		return typeAggID
	case TypeHashAgg:
		return typeHashAggID
	case TypeShow:
		return typeShowID
	case TypeJoin:
		return typeJoinID
	case TypeUnion:
		return typeUnionID
	case TypeTableScan:
		return typeTableScanID
	case TypeUnionScan:
		return typeUnionScanID
	case TypeSort:
		return typeSortID
	case TypeTopN:
		return typeTopNID
	case TypeLimit:
		return typeLimitID
	case TypeHashLeftJoin:
		return typeHashLeftJoinID
	case TypeHashRightJoin:
		return typeHashRightJoinID
	case TypeMergeJoin:
		return typeMergeJoinID
	case TypeIndexJoin:
		return typeIndexJoinID
	case TypeIndexMergeJoin:
		return typeIndexMergeJoinID
	case TypeIndexHashJoin:
		return typeIndexHashJoinID
	case TypeApply:
		return typeApplyID
	case TypeMaxOneRow:
		return typeMaxOneRowID
	case TypeExists:
		return typeExistsID
	case TypeTableReader:
		return typeTableReaderID
	}
	// Should never reach here.
	return 0
}

// PhysicalIDToTypeString converts the plan id to plan type string.
func PhysicalIDToTypeString(id int) string {
	switch id {
	case typeSelID:
		return TypeSel
	case typeSetID:
		return TypeSet
	case typeProjID:
		return TypeProj
	case typeAggID:
		return TypeAgg
	case typeHashAggID:
		return TypeHashAgg
	case typeShowID:
		return TypeShow
	case typeJoinID:
		return TypeJoin
	case typeUnionID:
		return TypeUnion
	case typeTableScanID:
		return TypeTableScan
	case typeUnionScanID:
		return TypeUnionScan
	case typeSortID:
		return TypeSort
	case typeTopNID:
		return TypeTopN
	case typeLimitID:
		return TypeLimit
	case typeHashLeftJoinID:
		return TypeHashLeftJoin
	case typeHashRightJoinID:
		return TypeHashRightJoin
	case typeMergeJoinID:
		return TypeMergeJoin
	case typeIndexJoinID:
		return TypeIndexJoin
	case typeIndexMergeJoinID:
		return TypeIndexMergeJoin
	case typeIndexHashJoinID:
		return TypeIndexHashJoin
	case typeApplyID:
		return TypeApply
	case typeMaxOneRowID:
		return TypeMaxOneRow
	case typeExistsID:
		return TypeExists
	case typeTableReaderID:
		return TypeTableReader
	}

	// Should never reach here.
	return "UnknownPlanID" + strconv.Itoa(id)
}
