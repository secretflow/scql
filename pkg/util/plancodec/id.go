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
