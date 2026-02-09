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

package compiler

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

// ExecutionGraphPass builds execution graph from operator graph
type ExecutionGraphPass struct{}

// NewExecutionGraphPass creates a new execution graph pass
func NewExecutionGraphPass() *ExecutionGraphPass {
	return &ExecutionGraphPass{}
}

// Name returns the pass name
func (p *ExecutionGraphPass) Name() string {
	return "ExecutionGraphPass"
}

// Run builds execution graph
func (p *ExecutionGraphPass) Run(c *CompileContext) error {
	// Build execution graph from operator graph
	executionGraphBuilder := NewExecutionGraphBuilder(
		c.TensorMetaManager,
		c.SecurityRelaxationManager,
		c.VisibilityTable,
		c.EnginesInfo,
		c.Request.GetCompileOpts(),
	)
	executionGraph, err := executionGraphBuilder.Build(c.OperatorGraph)
	if err != nil {
		return fmt.Errorf("failed to build execution graph: %v", err)
	}
	logrus.Debugf("execution graph: %s", executionGraph.DumpGraphviz())

	// Store execution graph in context
	c.ExecutionGraph = executionGraph

	return nil
}
