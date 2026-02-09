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

package core

import (
	"context"
)

type selectionMerger struct{}

func (s *selectionMerger) name() string {
	return "merge_selection"
}

func (s *selectionMerger) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	if err := s.mergeSelection(lp); err != nil {
		return nil, err
	}
	return lp, nil
}

// mergeSelection merges lp and it's father when they both are selection
func (s *selectionMerger) mergeSelection(lp LogicalPlan) error {
	if selection, ok := lp.(*LogicalSelection); ok && len(selection.Children()) == 1 {
		if childSelection, ok := lp.Children()[0].(*LogicalSelection); ok {
			selection.Conditions = append(selection.Conditions, childSelection.Conditions...)
			selection.Children()[0] = childSelection.Children()[0]
		}
	}
	for _, childLp := range lp.Children() {
		if err := s.mergeSelection(childLp); err != nil {
			return err
		}
	}
	return nil
}
