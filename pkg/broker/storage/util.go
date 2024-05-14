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

package storage

import (
	"strings"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func ColumnPrivs2ColumnControls(cps []ColumnPriv) []*pb.SecurityConfig_ColumnControl {
	ccs := make([]*pb.SecurityConfig_ColumnControl, len(cps))
	for i, cp := range cps {
		ccs[i] = &pb.SecurityConfig_ColumnControl{
			PartyCode:    cp.DestParty,
			Visibility:   pb.SecurityConfig_ColumnControl_Visibility(pb.SecurityConfig_ColumnControl_Visibility_value[strings.ToUpper(cp.Priv)]),
			DatabaseName: cp.ProjectID,
			TableName:    cp.TableName,
			ColumnName:   cp.ColumnName,
		}
	}
	return ccs
}
