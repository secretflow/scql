// Copyright 2024 Ant Group Co., Ltd.
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
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/compute"
)

var _ compute.FunctionOptions = &SliceOptions{}
var _ compute.FunctionOptions = &TrimOptions{}
var _ compute.FunctionOptions = &StrptimeOptions{}

type SliceOptions struct {
	Start int64 `compute:"start"`
	Stop  int64 `compute:"stop"`
	Step  int64 `compute:"step"`
}

func (SliceOptions) TypeName() string { return "SliceOptions" }

type TrimOptions struct {
	Characters string `compute:"characters"`
}

func (TrimOptions) TypeName() string { return "TrimOptions" }

type StrptimeOptions struct {
	Format      string         `compute:"format"`
	Unit        arrow.TimeUnit `compute:"unit"`
	ErrorIsNull bool           `compute:"error_is_null"`
}

func (StrptimeOptions) TypeName() string { return "StrptimeOptions" }
