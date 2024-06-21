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

package graph

import (
	"github.com/secretflow/scql/pkg/proto-gen/scql"
)

type TensorStatus int32

const (
	UnknownStatus TensorStatus = TensorStatus(scql.TensorStatus_TENSORSTATUS_UNKNOWN)
	PrivateStatus TensorStatus = TensorStatus(scql.TensorStatus_TENSORSTATUS_PRIVATE)
	SecretStatus  TensorStatus = TensorStatus(scql.TensorStatus_TENSORSTATUS_SECRET)
	CipherStatus  TensorStatus = TensorStatus(scql.TensorStatus_TENSORSTATUS_CIPHER)
	PublicStatus  TensorStatus = TensorStatus(scql.TensorStatus_TENSORSTATUS_PUBLIC)
)

// input tensors key
const (
	Left         string = "Left"
	Right        string = "Right"
	Condition    string = "Condition"
	ValueIfTrue  string = "ValueIfTrue"
	ValueIfFalse string = "ValueIfFalse"
	Value        string = "Value"
	ValueElse    string = "ValueElse"
	Out          string = "Out"
)

const (
	InnerJoin      = 0
	LeftOuterJoin  = 1
	RightOuterJoin = 2
)

const (
	PsiIn  = 0
	EcdhIn = 1
	OprfIn = 2
)
