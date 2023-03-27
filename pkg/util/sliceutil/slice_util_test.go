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

package sliceutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSliceDeDup(t *testing.T) {
	r := require.New(t)
	r.Equal([]string{"a", "b"}, SliceDeDup([]string{"a", "b", "a", "b", "b"}))
	r.Equal([]int{1, 2, 3}, SliceDeDup([]int{1, 2, 1, 2, 2, 1, 3}))
	r.Equal([]float64{0.1, 0.2}, SliceDeDup([]float64{0.1, 0.2, 0.1}))
}
