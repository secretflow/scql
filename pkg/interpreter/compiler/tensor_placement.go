// Copyright 2025 Ant Group Co., Ltd.
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

package compiler

import (
	"github.com/secretflow/scql/pkg/interpreter/graph"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

type tensorPlacement interface {
	Status() proto.TensorStatus
	String() string
}

type privatePlacement struct {
	partyCode string
}

func (p *privatePlacement) Status() proto.TensorStatus {
	return proto.TensorStatus_TENSORSTATUS_PRIVATE
}

func (p *privatePlacement) String() string {
	return "private-" + p.partyCode
}

type publicPlacement struct {
}

func (p *publicPlacement) Status() proto.TensorStatus {
	return proto.TensorStatus_TENSORSTATUS_PUBLIC
}

func (p *publicPlacement) String() string {
	return "public"
}

type secretPlacement struct {
}

func (p *secretPlacement) Status() proto.TensorStatus {
	return proto.TensorStatus_TENSORSTATUS_SECRET
}

func (p *secretPlacement) String() string {
	return "secret"
}

func IsPrivate(p tensorPlacement) bool {
	return p.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE
}

func IsPublic(p tensorPlacement) bool {
	return p.Status() == proto.TensorStatus_TENSORSTATUS_PUBLIC
}

func IsSecret(p tensorPlacement) bool {
	return p.Status() == proto.TensorStatus_TENSORSTATUS_SECRET
}

func extractTensorPlacement(tensor *graph.Tensor) tensorPlacement {
	switch tensor.Status() {
	case proto.TensorStatus_TENSORSTATUS_PRIVATE:
		if tensor.OwnerPartyCode == "" {
			return nil
		}
		return &privatePlacement{partyCode: tensor.OwnerPartyCode}
	case proto.TensorStatus_TENSORSTATUS_PUBLIC:
		return &publicPlacement{}
	case proto.TensorStatus_TENSORSTATUS_SECRET:
		return &secretPlacement{}
	default:
		return nil
	}
}
