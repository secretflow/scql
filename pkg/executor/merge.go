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

package executor

import (
	"fmt"

	"github.com/secretflow/scql/pkg/constant"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func findTensorListByName(tensors []*proto.Tensor, name string) []*proto.Tensor {
	var result []*proto.Tensor
	for _, t := range tensors {
		if t.Name == name {
			result = append(result, t)
		}
	}
	return result
}

func mergeStringTensorsToFirstTensor(tensors []*proto.Tensor) error {
	if len(tensors) == 0 {
		return fmt.Errorf("mergeStringTensors: invaild tensors number %v", len(tensors))
	}

	var stringsList [][]string
	for _, t := range tensors {
		if t.ElemType != proto.PrimitiveDataType_STRING {
			return fmt.Errorf("mergeStringTensors: invaild tensor element type %v", t.ElemType)
		}
		stringsList = append(stringsList, t.GetSs().GetSs())
	}

	// check shapes
	for i := 0; i < len(stringsList)-1; i++ {
		if len(stringsList[i]) != len(stringsList[i+1]) {
			return fmt.Errorf("mergeStringTensors: tensor shape doesn't match %v != %v", len(stringsList[i]), len(stringsList[i+1]))
		}
	}

	// set valid result to the first tensor
	for _, ss := range stringsList {
		for i, s := range ss {
			if s != constant.StringElementPlaceHolder {
				tensors[0].GetSs().GetSs()[i] = s
			}
		}
	}

	return nil
}
