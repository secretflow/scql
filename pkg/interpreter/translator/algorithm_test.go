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

package translator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/graph"
)

func createBinaryInputOutput() []map[string][]*ccl.CCL {
	result := make([]map[string][]*ccl.CCL, 0)
	in := map[string][]*ccl.CCL{
		graph.Left:  []*ccl.CCL{ccl.NewCCL()},
		graph.Right: []*ccl.CCL{ccl.NewCCL()},
	}
	out := map[string][]*ccl.CCL{
		graph.Out: []*ccl.CCL{ccl.NewCCL()},
	}
	result = append(result, in)
	result = append(result, out)
	return result
}

func TestCreateBinaryAlg(t *testing.T) {
	r := require.New(t)
	{
		inputs := createBinaryInputOutput()
		in := inputs[0]
		in[graph.Left][0].SetLevelForParty("party1", ccl.Encrypt)
		in[graph.Left][0].SetLevelForParty("party2", ccl.Encrypt)
		in[graph.Right][0].SetLevelForParty("party1", ccl.Encrypt)
		in[graph.Right][0].SetLevelForParty("party2", ccl.Encrypt)
		out := inputs[1]
		out[graph.Out][0].SetLevelForParty("party1", ccl.Encrypt)
		out[graph.Out][0].SetLevelForParty("party2", ccl.Encrypt)
		algs, err := createBinaryAlg(in, out, []string{"party1", "party2"})
		r.Nil(err)
		expectedAlg := &materializedAlgorithm{
			cost: newAlgCost(1, 3),
			inputPlacement: map[string][]placement{
				graph.Left: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
				graph.Right: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
			},
			outputPlacement: map[string][]placement{
				graph.Out: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
			},
		}
		r.Equal(1, len(algs))
		r.Equal(expectedAlg, algs[0])
	}
	{
		inputs := createBinaryInputOutput()
		in := inputs[0]
		in[graph.Left][0].SetLevelForParty("party1", ccl.Plain)
		in[graph.Left][0].SetLevelForParty("party2", ccl.Encrypt)
		in[graph.Right][0].SetLevelForParty("party1", ccl.Plain)
		in[graph.Right][0].SetLevelForParty("party2", ccl.Encrypt)
		out := inputs[1]
		out[graph.Out][0].SetLevelForParty("party1", ccl.Plain)
		out[graph.Out][0].SetLevelForParty("party2", ccl.Encrypt)
		algs, err := createBinaryAlg(in, out, []string{"party1", "party2"})
		r.Nil(err)
		expectedAlg1 := &materializedAlgorithm{
			cost: newAlgCost(1, 3),
			inputPlacement: map[string][]placement{
				graph.Left: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
				graph.Right: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
			},
			outputPlacement: map[string][]placement{
				graph.Out: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
			},
		}
		expectedAlg2 := &materializedAlgorithm{
			cost: newAlgCost(0, 1),
			inputPlacement: map[string][]placement{
				graph.Left: []placement{
					&privatePlacement{partyCode: "party1"},
				},
				graph.Right: []placement{
					&privatePlacement{partyCode: "party1"},
				},
			},
			outputPlacement: map[string][]placement{
				graph.Out: []placement{
					&privatePlacement{partyCode: "party1"},
				},
			},
		}
		r.Equal(2, len(algs))
		r.Equal(expectedAlg1, algs[0])
		r.Equal(expectedAlg2, algs[1])
	}
	{
		inputs := createBinaryInputOutput()
		in := inputs[0]
		in[graph.Left][0].SetLevelForParty("party1", ccl.Plain)
		in[graph.Left][0].SetLevelForParty("party2", ccl.Plain)
		in[graph.Right][0].SetLevelForParty("party1", ccl.Plain)
		in[graph.Right][0].SetLevelForParty("party2", ccl.Encrypt)
		out := inputs[1]
		out[graph.Out][0].SetLevelForParty("party1", ccl.Plain)
		out[graph.Out][0].SetLevelForParty("party2", ccl.Encrypt)
		algs, err := createBinaryAlg(in, out, []string{"party1", "party2"})
		r.Nil(err)
		expectedAlg1 := &materializedAlgorithm{
			cost: newAlgCost(1, 3),
			inputPlacement: map[string][]placement{
				graph.Left: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
				graph.Right: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
			},
			outputPlacement: map[string][]placement{
				graph.Out: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
			},
		}
		expectedAlg2 := &materializedAlgorithm{
			cost: newAlgCost(0, 1),
			inputPlacement: map[string][]placement{
				graph.Left: []placement{
					&privatePlacement{partyCode: "party1"},
				},
				graph.Right: []placement{
					&privatePlacement{partyCode: "party1"},
				},
			},
			outputPlacement: map[string][]placement{
				graph.Out: []placement{
					&privatePlacement{partyCode: "party1"},
				},
			},
		}
		expectedAlg3 := &materializedAlgorithm{
			cost: newAlgCost(1, 3),
			inputPlacement: map[string][]placement{
				graph.Left: []placement{
					&publicPlacement{partyCodes: []string{"party1", "party2"}},
				},
				graph.Right: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
			},
			outputPlacement: map[string][]placement{
				graph.Out: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
			},
		}
		expectedAlg4 := &materializedAlgorithm{
			cost: newAlgCost(0, 2),
			inputPlacement: map[string][]placement{
				graph.Left: []placement{
					&publicPlacement{partyCodes: []string{"party1", "party2"}},
				},
				graph.Right: []placement{
					&privatePlacement{partyCode: "party1"},
				},
			},
			outputPlacement: map[string][]placement{
				graph.Out: []placement{
					&privatePlacement{partyCode: "party1"},
				},
			},
		}
		r.Equal(4, len(algs))
		r.Equal(expectedAlg1, algs[0])
		r.Equal(expectedAlg2, algs[1])
		r.Equal(expectedAlg3, algs[2])
		r.Equal(expectedAlg4, algs[3])
	}
}

func TestCreateBinaryAlgNoComm(t *testing.T) {
	r := require.New(t)
	{
		inputs := createBinaryInputOutput()
		in := inputs[0]
		in[graph.Left][0].SetLevelForParty("party1", ccl.Encrypt)
		in[graph.Left][0].SetLevelForParty("party2", ccl.Encrypt)
		in[graph.Right][0].SetLevelForParty("party1", ccl.Encrypt)
		in[graph.Right][0].SetLevelForParty("party2", ccl.Encrypt)
		out := inputs[1]
		out[graph.Out][0].SetLevelForParty("party1", ccl.Encrypt)
		out[graph.Out][0].SetLevelForParty("party2", ccl.Encrypt)
		algs, err := createBinaryAlgNoComm(in, out, []string{"party1", "party2"})
		r.Nil(err)
		expectedAlg := &materializedAlgorithm{
			cost: newAlgCost(0, 3),
			inputPlacement: map[string][]placement{
				graph.Left: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
				graph.Right: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
			},
			outputPlacement: map[string][]placement{
				graph.Out: []placement{
					&sharePlacement{partyCodes: []string{"party1", "party2"}},
				},
			},
		}
		r.Equal(1, len(algs))
		r.Equal(expectedAlg, algs[0])
	}
}
