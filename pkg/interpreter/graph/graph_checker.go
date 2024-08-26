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
	"fmt"

	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
)

var (
	_ checkGraphRule = &checkTensorCCL{}
)

type checkGraphRule interface {
	checkGraph(*Graph) error
}

type GraphChecker struct {
	rules []checkGraphRule
}

func NewGraphChecker() *GraphChecker {
	rules := []checkGraphRule{&checkTensorCCL{}}
	return &GraphChecker{rules: rules}
}

func (c *GraphChecker) Check(graph *Graph) error {
	for _, rule := range c.rules {
		if err := rule.checkGraph(graph); err != nil {
			return err
		}
	}
	return nil
}

type checkTensorCCL struct {
}

func (c checkTensorCCL) checkGraph(graph *Graph) error {
	for _, pipeline := range graph.Pipelines {
		for node := range pipeline.Nodes {
			for _, ts := range node.Outputs {
				for _, t := range ts {
					if t.Status() == scql.TensorStatus_TENSORSTATUS_PRIVATE && t.CC.LevelFor(t.OwnerPartyCode) != ccl.Plain {
						return fmt.Errorf("failed to check tensor ccl for tensor(%+v), ccl is not plaintext for it's owner", t)
					}
				}
			}
		}
	}
	return nil
}
