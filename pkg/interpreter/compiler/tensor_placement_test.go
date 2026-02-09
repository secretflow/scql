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

package compiler

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/secretflow/scql/pkg/interpreter/graph"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestPrivatePlacement(t *testing.T) {
	party := "alice"
	placement := &privatePlacement{partyCode: party}

	t.Run("Status returns private", func(t *testing.T) {
		assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, placement.Status())
	})

	t.Run("String returns correct format", func(t *testing.T) {
		assert.Equal(t, "private-alice", placement.String())
	})

	t.Run("IsPrivate returns true", func(t *testing.T) {
		assert.True(t, IsPrivate(placement))
	})

	t.Run("IsPublic returns false", func(t *testing.T) {
		assert.False(t, IsPublic(placement))
	})

	t.Run("IsSecret returns false", func(t *testing.T) {
		assert.False(t, IsSecret(placement))
	})
}

func TestPublicPlacement(t *testing.T) {
	placement := &publicPlacement{}

	t.Run("Status returns public", func(t *testing.T) {
		assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PUBLIC, placement.Status())
	})

	t.Run("String returns public", func(t *testing.T) {
		assert.Equal(t, "public", placement.String())
	})

	t.Run("IsPrivate returns false", func(t *testing.T) {
		assert.False(t, IsPrivate(placement))
	})

	t.Run("IsPublic returns true", func(t *testing.T) {
		assert.True(t, IsPublic(placement))
	})

	t.Run("IsSecret returns false", func(t *testing.T) {
		assert.False(t, IsSecret(placement))
	})
}

func TestSecretPlacement(t *testing.T) {
	placement := &secretPlacement{}

	t.Run("Status returns secret", func(t *testing.T) {
		assert.Equal(t, proto.TensorStatus_TENSORSTATUS_SECRET, placement.Status())
	})

	t.Run("String returns secret", func(t *testing.T) {
		assert.Equal(t, "secret", placement.String())
	})

	t.Run("IsPrivate returns false", func(t *testing.T) {
		assert.False(t, IsPrivate(placement))
	})

	t.Run("IsPublic returns false", func(t *testing.T) {
		assert.False(t, IsPublic(placement))
	})

	t.Run("IsSecret returns true", func(t *testing.T) {
		assert.True(t, IsSecret(placement))
	})
}

func TestExtractTensorPlacement(t *testing.T) {
	testCases := []struct {
		name           string
		status         proto.TensorStatus
		ownerPartyCode string
		expectedType   string
	}{
		{
			name:           "private tensor with owner",
			status:         proto.TensorStatus_TENSORSTATUS_PRIVATE,
			ownerPartyCode: "alice",
			expectedType:   "*compiler.privatePlacement",
		},
		{
			name:           "private tensor without owner",
			status:         proto.TensorStatus_TENSORSTATUS_PRIVATE,
			ownerPartyCode: "",
			expectedType:   "<nil>",
		},
		{
			name:           "public tensor",
			status:         proto.TensorStatus_TENSORSTATUS_PUBLIC,
			ownerPartyCode: "",
			expectedType:   "*compiler.publicPlacement",
		},
		{
			name:           "secret tensor",
			status:         proto.TensorStatus_TENSORSTATUS_SECRET,
			ownerPartyCode: "",
			expectedType:   "*compiler.secretPlacement",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tensor := &graph.Tensor{
				OwnerPartyCode: tc.ownerPartyCode,
			}
			// Mock the Status method
			tensor.SetStatus(tc.status)

			result := extractTensorPlacement(tensor)
			assert.Equal(t, tc.expectedType, fmt.Sprintf("%T", result))
		})
	}
}
