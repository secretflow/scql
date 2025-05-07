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

package graph

import (
	"fmt"
	"strconv"

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/stringutil"
)

var (
	_ optimizeGraphRule = &optConstantCast{}
)

type optimizeGraphRule interface {
	optimize(*Graph) error
}

type GraphOptimizer struct {
	rules []optimizeGraphRule
}

func NewGraphOptimizer() *GraphOptimizer {
	rules := []optimizeGraphRule{&optConstantCast{}}
	return &GraphOptimizer{rules: rules}
}

func (g *GraphOptimizer) Optimize(graph *Graph) error {
	for _, rule := range g.rules {
		if err := rule.optimize(graph); err != nil {
			return err
		}
	}
	return nil
}

type optConstantCast struct {
}

func (rule optConstantCast) optimize(graph *Graph) error {
	for _, pipeline := range graph.Pipelines {
		for node := range pipeline.Nodes {
			if node.OpType != "Constant" {
				continue
			}

			// find broadcast node
			var broadCastNode *ExecutionNode
			for edge := range node.Edges {
				if edge.To.OpType == "BroadcastTo" {
					broadCastNode = edge.To
				}
			}
			if broadCastNode == nil {
				continue
			}

			// find cast node
			var castNode *ExecutionNode
			for edge := range broadCastNode.Edges {
				if edge.To.OpType == "Cast" {
					castNode = edge.To
				}
			}
			if castNode == nil {
				continue
			}

			// check whether cast is valid
			originType := node.Outputs["Out"][0].DType
			castType := castNode.Outputs["Out"][0].DType
			if !isValidCast(originType, castType) {
				return fmt.Errorf("GraphOptimizer: invalid cast from %v to %v", originType, castType)
			}

			// cast value
			scalarAttr := node.Attributes["scalar"]
			if scalarAttr == nil {
				return fmt.Errorf("GraphOptimizer: constant node doesn't have scalar attribute")
			}
			err := castValue(scalarAttr, originType, castType)
			if err != nil {
				return fmt.Errorf("GraphOptimizer: failed to cast value: %v", err)
			}

			// change tensor type
			if castType == proto.PrimitiveDataType_DATETIME || castType == proto.PrimitiveDataType_TIMESTAMP {
				node.Outputs["Out"][0].DType = proto.PrimitiveDataType_INT64
				broadCastNode.Outputs["Out"][0].DType = proto.PrimitiveDataType_INT64
			} else {
				node.Outputs["Out"][0].DType = castType
				broadCastNode.Outputs["Out"][0].DType = castType
			}

			// rearrange edges
			for edge := range broadCastNode.Edges {
				delete(broadCastNode.Edges, edge)
			}
			castNodeOutTs := castNode.Outputs["Out"][0]
			for edge := range castNode.Edges {
				edge.From = broadCastNode
				edge.Value = broadCastNode.Outputs["Out"][0]
				broadCastNode.Edges[edge] = true

				for _, input := range edge.To.Inputs {
					for i := range input {
						if input[i].ID == castNodeOutTs.ID {
							input[i] = edge.Value
						}
					}
				}
			}

			// remove castNode
			delete(pipeline.Nodes, castNode)
			graph.NodeCnt--
		}
	}
	return nil
}

func isValidCast(originType, castType proto.PrimitiveDataType) bool {
	switch originType {
	case proto.PrimitiveDataType_STRING:
		return castType == proto.PrimitiveDataType_INT64 || castType == proto.PrimitiveDataType_FLOAT64 || castType == proto.PrimitiveDataType_DATETIME || castType == proto.PrimitiveDataType_TIMESTAMP
	case proto.PrimitiveDataType_INT32, proto.PrimitiveDataType_INT64:
		return castType == proto.PrimitiveDataType_FLOAT32 || castType == proto.PrimitiveDataType_FLOAT64 || castType == proto.PrimitiveDataType_STRING
	case proto.PrimitiveDataType_FLOAT32, proto.PrimitiveDataType_FLOAT64:
		return castType == proto.PrimitiveDataType_INT32 || castType == proto.PrimitiveDataType_INT64 || castType == proto.PrimitiveDataType_STRING
	case proto.PrimitiveDataType_BOOL:
		return castType == proto.PrimitiveDataType_INT32 || castType == proto.PrimitiveDataType_STRING
	case proto.PrimitiveDataType_DATETIME, proto.PrimitiveDataType_TIMESTAMP:
		return castType == proto.PrimitiveDataType_STRING || castType == proto.PrimitiveDataType_INT64
	default:
		return originType == castType
	}
}

func castValue(scalarAttr *Attribute, originType, castType proto.PrimitiveDataType) error {
	originalValue := scalarAttr.GetAttrValue()
	if originalValue == nil {
		return fmt.Errorf("constant node doesn't have value")
	}

	switch originType {
	case proto.PrimitiveDataType_STRING:
		strVal, ok := originalValue.(string)
		if !ok {
			return fmt.Errorf("expected string value")
		}
		if castType == proto.PrimitiveDataType_INT64 {
			castValue, err := strconv.ParseInt(strVal, 10, 64)
			if err != nil {
				return err
			}
			scalarAttr.SetInt64(castValue)
			return nil
		} else if castType == proto.PrimitiveDataType_FLOAT64 {
			castValue, err := strconv.ParseFloat(strVal, 64)
			if err != nil {
				return err
			}
			scalarAttr.SetDouble(castValue)
			return nil
		} else if castType == proto.PrimitiveDataType_DATETIME || castType == proto.PrimitiveDataType_TIMESTAMP { // return int64 value
			if stringutil.IsDateString(strVal) {
				tsMilli, err := stringutil.StringToUnixMilli(strVal)
				if err != nil {
					return fmt.Errorf("failed to parse date/time constant %q: %v", strVal, err)
				}
				scalarAttr.SetInt64(tsMilli)
				return nil
			}
			return fmt.Errorf("date/time constant format should be 'YYYY-MM-DD hh:mm:ss'")
		}
	case proto.PrimitiveDataType_INT32, proto.PrimitiveDataType_INT64:
		intVal, ok := originalValue.(int64)
		if !ok {
			return fmt.Errorf("expected int64 value")
		}
		if castType == proto.PrimitiveDataType_FLOAT64 {
			scalarAttr.SetDouble(float64(intVal))
			return nil
		} else if castType == proto.PrimitiveDataType_STRING {
			scalarAttr.SetString(strconv.FormatInt(intVal, 10))
			return nil
		}
	case proto.PrimitiveDataType_FLOAT32, proto.PrimitiveDataType_FLOAT64:
		floatVal, ok := originalValue.(float64)
		if !ok {
			return fmt.Errorf("expected float64 value")
		}
		if castType == proto.PrimitiveDataType_INT64 {
			scalarAttr.SetInt64(int64(floatVal))
			return nil
		} else if castType == proto.PrimitiveDataType_STRING {
			scalarAttr.SetString(fmt.Sprintf("%f", floatVal))
			return nil
		}
	case proto.PrimitiveDataType_BOOL:
		boolVal, ok := originalValue.(bool)
		if !ok {
			return fmt.Errorf("expected bool value")
		}
		if castType == proto.PrimitiveDataType_INT32 {
			if boolVal {
				scalarAttr.SetInt(1)
				return nil
			} else {
				scalarAttr.SetInt(0)
				return nil
			}
		} else if castType == proto.PrimitiveDataType_STRING {
			scalarAttr.SetString(strconv.FormatBool(boolVal))
			return nil
		}
	case proto.PrimitiveDataType_DATETIME, proto.PrimitiveDataType_TIMESTAMP:
		strVal, ok := originalValue.(string)
		if !ok {
			return fmt.Errorf("expected datetime string value")
		}
		if castType == proto.PrimitiveDataType_STRING {
			scalarAttr.SetString(strVal)
			return nil
		} else if castType == proto.PrimitiveDataType_INT64 {
			if stringutil.IsDateString(strVal) {
				tsMilli, err := stringutil.StringToUnixMilli(strVal)
				if err != nil {
					return fmt.Errorf("failed to parse date/time constant %q: %v", strVal, err)
				}
				scalarAttr.SetInt64(tsMilli)
				return nil
			}
			return fmt.Errorf("date/time constant format should be 'YYYY-MM-DD hh:mm:ss'")
		}
	}
	return fmt.Errorf("invalid cast from %v to %v", originType, castType)
}
