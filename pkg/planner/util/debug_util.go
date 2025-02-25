// Copyright 2025 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package util

import (
	"fmt"

	"github.com/secretflow/scql/pkg/planner/core"
)

// Helper function to print the details of a LogicalPlan
func printPlanDetails(plan core.LogicalPlan, level int) {
	indent := "    "
	for i := 0; i < level; i++ {
		indent += "    "
	}

	fmt.Printf("%sPlan ID: %d\n", indent, plan.ID())
	fmt.Printf("%sPlan Type: %s\n", indent, plan.TP())
	fmt.Println(indent + "Children:")
	for _, child := range plan.Children() {
		printPlanDetails(child, level+1)
	}
}

// Debug function to print the logical plan
func PrintLogicalPlan(plan core.LogicalPlan) {
	fmt.Println("Logical Plan:")
	printPlanDetails(plan, 0)
}
