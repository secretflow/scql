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

package ccl

import (
	"fmt"
	"sort"
	"strings"

	"github.com/secretflow/scql/pkg/expression"
)

func ExtractPartyCodes(in []*CCL) []string {
	var allPartyList []string
	partiesMap := make(map[string]bool, 0)
	for _, cc := range in {
		for _, p := range cc.Parties() {
			partiesMap[p] = true
		}
	}
	for p := range partiesMap {
		allPartyList = append(allPartyList, p)
	}
	// sort for determinism
	sort.Strings(allPartyList)

	return allPartyList
}

func LookUpVis(left, right CCLLevel) CCLLevel {
	if left == Encrypt || right == Encrypt {
		return Encrypt
	}
	if left == Plain {
		return right
	}
	if right == Plain {
		return left
	}
	if left == right {
		return left
	}
	return Unknown
}

func (cc *CCL) UpdateMoreRestrictedCCLFrom(other *CCL) {
	partyCodes := ExtractPartyCodes([]*CCL{cc, other})
	for _, p := range partyCodes {
		cc.SetLevelForParty(p, LookUpVis(cc.LevelFor(p), other.LevelFor(p)))
	}
}

func inferCompareFuncCCL(left, right CCLLevel) CCLLevel {
	if (left == Compare || left == Plain) && (right == Compare || right == Plain) {
		return Plain
	}
	return LookUpVis(left, right)
}

func (left *CCL) InferCompareFuncCCL(right *CCL) {
	partyCodes := ExtractPartyCodes([]*CCL{left, right})
	for _, p := range partyCodes {
		left.SetLevelForParty(p, inferCompareFuncCCL(left.LevelFor(p), right.LevelFor(p)))
	}
}

// add tests when more operator added here
func InferExpressionCCL(expr expression.Expression, ccl map[int64]*CCL) (*CCL, error) {
	// get all parties
	var allCCLs []*CCL
	for _, cc := range ccl {
		allCCLs = append(allCCLs, cc)
	}
	parties := ExtractPartyCodes(allCCLs)
	switch x := expr.(type) {
	case *expression.Constant:
		if len(parties) == 0 {
			return nil, fmt.Errorf("inferExpressionCCL: empty intput party codes")
		}
		return CreateAllPlainCCL(parties), nil
	case *expression.Column:
		cc, ok := ccl[x.UniqueID]
		if !ok {
			return nil, fmt.Errorf("inferExpressionCCL: unable to find col(%v)", x.UniqueID)
		}
		return cc.Clone(), nil
	case *expression.ScalarFunction:
		if len(x.GetArgs()) == 0 {
			if len(parties) == 0 {
				return nil, fmt.Errorf("inferExpressionCCL: empty intput party codes")
			}
			return CreateAllPlainCCL(parties), nil
		}
		var argCCL []*CCL
		for _, arg := range x.GetArgs() {
			cc, err := InferExpressionCCL(arg, ccl)
			if err != nil {
				return nil, err
			}
			argCCL = append(argCCL, cc)
		}
		return InferScalarFuncCCLUsingArgCCL(x, argCCL)
	default:
		return nil, fmt.Errorf("inferExpressionCCL: unsupported expression %s with type %T", x, x)
	}
}

func InferScalarFuncCCLUsingArgCCL(sf *expression.ScalarFunction, argCCL []*CCL) (*CCL, error) {
	isCompare, err := IsCompareOp(sf)
	if err != nil {
		return nil, fmt.Errorf("inferExpressionCCL: %v", err)
	}
	resultCC := argCCL[0].Clone()
	for _, argCCL := range argCCL[1:] {
		if isCompare {
			resultCC.InferCompareFuncCCL(argCCL)
			continue
		}
		resultCC.UpdateMoreRestrictedCCLFrom(argCCL)
	}
	return resultCC, nil
}

func InferBinaryOpOutputVisibility(opType string, left *CCL, right *CCL) (*CCL, error) {
	cc := left.Clone()
	if isCompareOpMap[opType] {
		cc.InferCompareFuncCCL(right)
	} else {
		cc.UpdateMoreRestrictedCCLFrom(right)
	}

	return cc, nil
}

func IsCompareOp(expr expression.Expression) (bool, error) {
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return false, fmt.Errorf("assert failed, expected: *ScalarFunction, actual: %T", expr)
	}
	isCompare := isCompareAstFuncMap[sf.FuncName.L]
	return isCompare, nil
}

func IsRankWindowFunc(name string) bool {
	if _, ok := isRankWindowFuncMap[strings.ToLower(name)]; ok {
		return true
	}

	return false
}
