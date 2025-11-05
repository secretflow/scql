// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"math"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/parser/ast"
)

type joinReorderGreedySolver struct {
	*baseSingleGroupJoinOrderSolver
	eqEdges []*expression.ScalarFunction
}

// solve reorders the join nodes in the group based on a greedy algorithm.
//
// For each node having a join equal condition with the current join tree in
// the group, calculate the cumulative join cost of that node and the join
// tree, choose the node with the smallest cumulative cost to join with the
// current join tree.
//
// cumulative join cost = CumCount(lhs) + CumCount(rhs) + RowCount(join)
//
//	For base node, its CumCount equals to the sum of the count of its subtree.
//	See baseNodeCumCost for more details.
//
// TODO: this formula can be changed to real physical cost in future.
//
// For the nodes and join trees which don't have a join equal condition to
// connect them, we make a bushy join tree to do the cartesian joins finally.
func (s *joinReorderGreedySolver) solve(joinNodePlans []LogicalPlan) (LogicalPlan, error) {
	for _, node := range joinNodePlans {
		s.curJoinGroup = append(s.curJoinGroup, &jrNode{
			p:       node,
			cumCost: s.baseNodeCumCost(node),
		})
	}
	var optimizedCurJoinGroup []*jrNode
	for {
		if len(s.curJoinGroup) == 0 {
			break
		}
		newNode := s.popIfFirstNodeCantBeMerged()
		if newNode != nil {
			optimizedCurJoinGroup = append(optimizedCurJoinGroup, newNode)
		}
	}
	s.curJoinGroup = optimizedCurJoinGroup

	var cartesianGroup []LogicalPlan
	for len(s.curJoinGroup) > 0 {
		newNode, err := s.constructConnectedJoinTree()
		if err != nil {
			return nil, err
		}
		cartesianGroup = append(cartesianGroup, newNode.p)
	}

	return s.makeBushyJoin(cartesianGroup), nil
}

func (s *joinReorderGreedySolver) popIfFirstNodeCantBeMerged() *jrNode {
	curJoinTree := s.curJoinGroup[0]
	// only merging plans with same party code
	for i, node := range s.curJoinGroup[1:] {
		// skip join node which can't merge with curJoinTree
		if node.p.PartyCode() == "" || (curJoinTree.p.PartyCode() != node.p.PartyCode()) {
			continue
		}
		newJoin, remainOthers := s.checkConnectionAndMakeJoin(curJoinTree.p, node.p)
		if newJoin == nil {
			continue
		}
		curCost := s.calcJoinCumCost(newJoin, curJoinTree, node)
		mergedIdx := i + 1
		// remove merged node
		s.curJoinGroup = append(s.curJoinGroup[:mergedIdx], s.curJoinGroup[mergedIdx+1:]...)
		s.otherConds = remainOthers
		curJoinTree = &jrNode{
			p:       newJoin,
			cumCost: curCost,
		}
		s.curJoinGroup[0] = curJoinTree
		return nil
	}
	// If we could find more join node, meaning that the sub connected graph have been totally explored.
	s.curJoinGroup = s.curJoinGroup[1:]
	return curJoinTree
}

func (s *joinReorderGreedySolver) constructConnectedJoinTree() (*jrNode, error) {
	curJoinTree := s.curJoinGroup[0]
	s.curJoinGroup = s.curJoinGroup[1:]

	for {
		bestCost := math.MaxFloat64
		bestIdx := -1
		var finalRemainOthers []expression.Expression
		var bestJoin LogicalPlan
		for i, node := range s.curJoinGroup {
			newJoin, remainOthers := s.checkConnectionAndMakeJoin(curJoinTree.p, node.p)
			if newJoin == nil {
				continue
			}
			// NOTE: didn't work for scql
			// _, err := newJoin.recursiveDeriveStats()
			// if err != nil {
			// 	return nil, err
			// }
			curCost := s.calcJoinCumCost(newJoin, curJoinTree, node)
			if bestCost > curCost {
				bestCost = curCost
				bestJoin = newJoin
				bestIdx = i
				finalRemainOthers = remainOthers
			}
		}
		// If we could find more join node, meaning that the sub connected graph have been totally explored.
		if bestJoin == nil {
			break
		}
		curJoinTree = &jrNode{
			p:       bestJoin,
			cumCost: bestCost,
		}
		s.curJoinGroup = append(s.curJoinGroup[:bestIdx], s.curJoinGroup[bestIdx+1:]...)
		s.otherConds = finalRemainOthers
	}
	return curJoinTree, nil
}

func (s *joinReorderGreedySolver) checkConnectionAndMakeJoin(leftNode, rightNode LogicalPlan) (LogicalPlan, []expression.Expression) {
	var usedEdges []*expression.ScalarFunction
	remainOtherConds := make([]expression.Expression, len(s.otherConds))
	copy(remainOtherConds, s.otherConds)
	for _, edge := range s.eqEdges {
		lCol := edge.GetArgs()[0].(*expression.Column)
		rCol := edge.GetArgs()[1].(*expression.Column)
		if leftNode.Schema().Contains(lCol) && rightNode.Schema().Contains(rCol) {
			usedEdges = append(usedEdges, edge)
		} else if rightNode.Schema().Contains(lCol) && leftNode.Schema().Contains(rCol) {
			newSf := expression.NewFunctionInternal(s.ctx, ast.EQ, edge.GetType(), rCol, lCol).(*expression.ScalarFunction)
			usedEdges = append(usedEdges, newSf)
		}
	}
	if len(usedEdges) == 0 {
		return nil, nil
	}
	var otherConds []expression.Expression
	mergedSchema := expression.MergeSchema(leftNode.Schema(), rightNode.Schema())
	remainOtherConds, otherConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		return expression.ExprFromSchema(expr, mergedSchema)
	})
	return s.newJoinWithEdges(leftNode, rightNode, usedEdges, otherConds), remainOtherConds
}
