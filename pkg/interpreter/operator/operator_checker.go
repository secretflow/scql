// Copyright 2026 Ant Group Co., Ltd.
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

package operator

import (
	"fmt"
	"slices"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

type statusConstraint interface {
	Status() pb.TensorStatus
}

// CheckParamStatusConstraint check parameter status constraint strictly
func CheckParamStatusConstraint[T statusConstraint](op *OperatorDef, inputs map[string][]T, outputs map[string][]T) error {
	opDef := op.GetOperatorDefProto()
	if len(opDef.InputParams) != len(inputs) {
		return fmt.Errorf("CheckParamStatusConstraint: op %v len(opDef.InputParams):%v != len(inputs):%v",
			opDef.Name, len(opDef.InputParams), len(inputs))
	}
	if len(opDef.OutputParams) != len(outputs) {
		return fmt.Errorf("CheckParamStatusConstraint: op %v len(opDef.OutputParams):%v != len(outputs):%v",
			opDef.Name, len(opDef.OutputParams), len(outputs))
	}

	constraintNameToStatus := map[string]pb.TensorStatus{}
	if err := checkParamStatusConstraintInternal(constraintNameToStatus, inputs, opDef.InputParams, opDef.ParamStatusConstraints); err != nil {
		return fmt.Errorf("opName %s %v", opDef.Name, err)
	}
	if err := checkParamStatusConstraintInternal(constraintNameToStatus, outputs, opDef.OutputParams, opDef.ParamStatusConstraints); err != nil {
		return fmt.Errorf("opName %s %v", opDef.Name, err)
	}
	return nil
}

func checkParamStatusConstraintInternal[T statusConstraint](constraintNameToStatus map[string]pb.TensorStatus,
	args map[string][]T, params []*pb.FormalParameter,
	paramStatusConstraint map[string]*pb.TensorStatusList) error {
	for _, param := range params {
		arguments, ok := args[param.ParamName]
		if !ok {
			return fmt.Errorf("can't find param:%v in arguments", param.ParamName)
		}

		if len(arguments) == 0 && param.Option == pb.FormalParameterOptions_FORMALPARAMETEROPTIONS_OPTIONAL {
			continue
		}

		if len(arguments) == 0 {
			return fmt.Errorf("param:%v must contains at least one argument", param.ParamName)
		}

		expectedStatus, ok := constraintNameToStatus[param.ParameterStatusConstraintName]
		if !ok {
			statusConstraint, ok := paramStatusConstraint[param.ParameterStatusConstraintName]
			if !ok {
				return fmt.Errorf("CheckParamStatusConstraint: can't find constraint for param:%v, constraintName:%v",
					param.ParamName, param.ParameterStatusConstraintName)
			}

			if !slices.Contains(statusConstraint.Status, args[param.ParamName][0].Status()) {
				return fmt.Errorf("CheckParamStatusConstraint: invalid status for param[%v] actual:%v, expected:%v", param.ParamName, args[param.ParamName][0].Status(), statusConstraint)
			}
			constraintNameToStatus[param.ParameterStatusConstraintName] = args[param.ParamName][0].Status()
		} else {
			if args[param.ParamName][0].Status() != expectedStatus {
				return fmt.Errorf("param status mismatch, actual:%v, expected:%v", args[param.ParamName][0].Status(), expectedStatus)
			}
		}
	}
	return nil
}
