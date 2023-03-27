// Copyright 2017 PingCAP, Inc.
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

package expression

import "github.com/secretflow/scql/pkg/parser/ast"

// DisableFoldFunctions stores functions which prevent child scope functions from being constant folded.
// Typically, these functions shall also exist in unFoldableFunctions, to stop from being folded when they themselves
// are in child scope of an outer function, and the outer function is recursively folding its children.
var DisableFoldFunctions = map[string]struct{}{
	ast.Benchmark: {},
}

// DeferredFunctions stores non-deterministic functions, which can be deferred only when the plan cache is enabled.
var DeferredFunctions = map[string]struct{}{
	ast.Now:              {},
	ast.CurrentTimestamp: {},
	ast.UTCTime:          {},
	ast.Curtime:          {},
	ast.CurrentTime:      {},
	ast.UTCTimestamp:     {},
	ast.UnixTimestamp:    {},
	ast.Sysdate:          {},
	ast.Curdate:          {},
	ast.CurrentDate:      {},
	ast.UTCDate:          {},
	ast.Rand:             {},
	ast.UUID:             {},
}

// unFoldableFunctions stores functions which can not be folded duration constant folding stage.
var unFoldableFunctions = map[string]struct{}{
	ast.Sysdate:   {},
	ast.FoundRows: {},
	ast.Rand:      {},
	ast.UUID:      {},
	ast.Sleep:     {},
	ast.RowFunc:   {},
	ast.Values:    {},
	ast.SetVar:    {},
	ast.GetVar:    {},
	ast.GetParam:  {},
	ast.Benchmark: {},
	ast.DayName:   {},
	// ast.NextVal:   {},
	// ast.LastVal:   {},
	// ast.SetVal:    {},
}
