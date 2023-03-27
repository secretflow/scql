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
	switch x := attribute.GetT().GetValue().(type) {
	case *proto.Tensor_Bs:
		if len(x.Bs.Bs) == 1 {
			return fmt.Sprintf("%v", x.Bs.Bs[0])
		}
		return fmt.Sprintf("%v", x.Bs.Bs)
	case *proto.Tensor_Ss:
		if len(x.Ss.Ss) == 1 {
			return x.Ss.Ss[0]
		}
		return fmt.Sprintf("%v", x.Ss.Ss)
	case *proto.Tensor_Fs:
		if len(x.Fs.Fs) == 1 {
			return fmt.Sprintf("%v", x.Fs.Fs[0])
		}
		return fmt.Sprintf("%v", x.Fs.Fs)
	case *proto.Tensor_Is:
		if len(x.Is.Is) == 1 {
			return fmt.Sprintf("%v", x.Is.Is[0])
		}
		return fmt.Sprintf("%v", x.Is.Is)
	case *proto.Tensor_I64S:
		if len(x.I64S.I64S) == 1 {
			return fmt.Sprintf("%v", x.I64S.I64S[0])
		}
		return fmt.Sprintf("%v", x.I64S.I64S)
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

	f, err := os.Create("docs/reference/scql_operators.md")
	check(err)
	err = tmpl.Execute(f, filler)
	check(err)
}
