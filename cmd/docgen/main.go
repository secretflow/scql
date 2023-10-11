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

package main

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"text/template"

	"github.com/secretflow/scql/pkg/interpreter/operator"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

type OpDocFiller struct {
	Version  int
	AllOpDef []*proto.OperatorDef
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func AttributeToString(attribute *proto.AttributeValue) string {
	t := attribute.GetT()
	switch t.ElemType {
	case proto.PrimitiveDataType_BOOL:
		if len(t.BoolData) == 1 {
			return fmt.Sprintf("%v", t.GetBoolData()[0])
		}
		return fmt.Sprintf("%v", t.GetBoolData())
	case proto.PrimitiveDataType_STRING:
		if len(t.StringData) == 1 {
			return t.GetStringData()[0]
		}
		return fmt.Sprintf("%v", t.GetStringData())
	case proto.PrimitiveDataType_FLOAT32:
		if len(t.FloatData) == 1 {
			return fmt.Sprintf("%v", t.GetFloatData()[0])
		}
		return fmt.Sprintf("%v", t.GetFloatData())
	case proto.PrimitiveDataType_FLOAT64:
		if len(t.DoubleData) == 1 {
			return fmt.Sprintf("%v", t.GetDoubleData()[0])
		}
		return fmt.Sprintf("%v", t.GetDoubleData())
	case proto.PrimitiveDataType_INT8, proto.PrimitiveDataType_INT16, proto.PrimitiveDataType_INT32:
		if len(t.Int32Data) == 1 {
			return fmt.Sprintf("%v", t.GetInt32Data()[0])
		}
		return fmt.Sprintf("%v", t.GetInt32Data())
	case proto.PrimitiveDataType_INT64:
		if len(t.Int64Data) == 1 {
			return fmt.Sprintf("%v", t.GetInt64Data()[0])
		}
		return fmt.Sprintf("%v", t.GetInt64Data())
	default:
		return "error: unsupported attribute type"
	}
}

func OptionToString(opt proto.FormalParameterOptions) string {
	switch opt {
	case proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE:
		return "single"
	case proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC:
		return "variadic"
	case proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_OPTIONAL:
		return "optional"
	default:
		return "undefined"
	}
}

func StatusToString(opt proto.TensorStatus) string {
	switch opt {
	case proto.TensorStatus_TENSORSTATUS_UNKNOWN:
		return "unknown"
	case proto.TensorStatus_TENSORSTATUS_PRIVATE:
		return "private"
	case proto.TensorStatus_TENSORSTATUS_SECRET:
		return "secret"
	case proto.TensorStatus_TENSORSTATUS_CIPHER:
		return "cipher"
	case proto.TensorStatus_TENSORSTATUS_PUBLIC:
		return "public"
	default:
		return "undefined"
	}
}

func StatusListToString(in *proto.TensorStatusList) string {
	var result []string
	for _, e := range in.Status {
		result = append(result, StatusToString(e))
	}
	return strings.Join(result, ",")
}

func main() {
	ops, version := operator.GetAllOpDef()
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].GetName() < ops[j].GetName()
	})
	filler := OpDocFiller{
		AllOpDef: ops,
		Version:  version,
	}
	fileName := "cmd/docgen/scql_operators.md.tmpl"
	tmpl, err := template.New(path.Base(fileName)).Funcs(
		template.FuncMap{
			"attributeToString":  AttributeToString,
			"optionToString":     OptionToString,
			"statusToString":     StatusToString,
			"statusListToString": StatusListToString,
		}).ParseFiles(fileName)
	check(err)

	f, err := os.Create("docs/reference/operators.md")
	check(err)
	err = tmpl.Execute(f, filler)
	check(err)
}
