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

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

type CCL struct {
	partyLevels map[string]CCLLevel
}

func NewCCL() *CCL {
	return &CCL{
		partyLevels: make(map[string]CCLLevel),
	}
}

func (cc *CCL) Clone() *CCL {
	newpartyLevels := make(map[string]CCLLevel, len(cc.partyLevels))
	for p, level := range cc.partyLevels {
		newpartyLevels[p] = level
	}
	return &CCL{
		partyLevels: newpartyLevels,
	}
}

func (cc *CCL) LevelFor(party string) CCLLevel {
	vis, ok := cc.partyLevels[party]
	if !ok {
		// no default ccl level, just return unknown
		return Unknown
	}
	return vis
}

func (cc *CCL) SetLevelForParty(party string, level CCLLevel) {
	cc.partyLevels[party] = level
}

func (cc *CCL) IsVisibleFor(party string) bool {
	vis, ok := cc.partyLevels[party]
	if ok && vis == Plain {
		return true
	}
	return false
}

func (cc *CCL) GetVisibleParties() []string {
	var visibleParties []string
	for _, party := range cc.Parties() {
		if cc.IsVisibleFor(party) {
			visibleParties = append(visibleParties, party)
		}
	}
	return visibleParties
}

func (cc *CCL) IsVisibleForParties(parties []string) bool {
	for _, party := range parties {
		vis, ok := cc.partyLevels[party]
		if !ok || vis != Plain {
			return false
		}
	}
	return true
}

func (cc *CCL) String() string {
	levelString := ""
	for _, p := range sliceutil.SortMapKeyForDeterminism(cc.partyLevels) {
		levelString += fmt.Sprintf("%s->%s,", p, cc.partyLevels[p].String())
	}
	return fmt.Sprintf("{ccl level: %v}", levelString)
}

func (cc *CCL) Parties() []string {
	var parties []string
	for p := range cc.partyLevels {
		parties = append(parties, p)
	}
	return parties
}

func CreateAllPlainCCL(parties []string) *CCL {
	newCc := NewCCL()
	for _, p := range parties {
		newCc.partyLevels[p] = Plain
	}
	return newCc
}

func (level CCLLevel) String() string {
	return proto.SecurityConfig_ColumnControl_Visibility_name[int32(level)]
}
