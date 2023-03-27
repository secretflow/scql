// Copyright 2018 PingCAP, Inc.
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

package aggregation

import (
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/codec"
	"github.com/secretflow/scql/pkg/util/mvmap"
)

// distinctChecker stores existing keys and checks if given data is distinct.
type distinctChecker struct {
	existingKeys *mvmap.MVMap
	key          []byte
	vals         [][]byte
	sc           *stmtctx.StatementContext
}

// createDistinctChecker creates a new distinct checker.
func createDistinctChecker(sc *stmtctx.StatementContext) *distinctChecker {
	return &distinctChecker{
		existingKeys: mvmap.NewMVMap(),
		sc:           sc,
	}
}

// Check checks if values is distinct.
func (d *distinctChecker) Check(values []types.Datum) (bool, error) {
	d.key = d.key[:0]
	var err error
	d.key, err = codec.EncodeValue(d.sc, d.key, values...)
	if err != nil {
		return false, err
	}
	d.vals = d.existingKeys.Get(d.key, d.vals[:0])
	if len(d.vals) > 0 {
		return false, nil
	}
	d.existingKeys.Put(d.key, []byte{})
	return true, nil
}
