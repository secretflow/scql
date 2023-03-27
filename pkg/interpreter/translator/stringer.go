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
	"strings"
)

func ToString(node logicalNode) string {
	strs, _ := toString(node, []string{}, []int{})
	return strings.Join(strs, "->")
}

func toString(in logicalNode, strs []string, idxs []int) ([]string, []int) {
	if len(in.Children()) > 1 {
		idxs = append(idxs, len(strs))
	}
	for _, c := range in.Children() {
		strs, idxs = toString(c, strs, idxs)
	}

	var str string
	switch x := in.(type) {
	case *JoinNode:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "Join{" + strings.Join(children, "->") + "}"
		str += fmt.Sprintf("%s", x.DataSourceParty())
	case *ApplyNode:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "Apply{" + strings.Join(children, "->") + "}"
		str += fmt.Sprintf("%s", x.DataSourceParty())
	default:
		str = fmt.Sprintf("%s%s", in.Type(), in.DataSourceParty())
	}
	strs = append(strs, str)
	return strs, idxs
}
