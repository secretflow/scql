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

package constant

const (
	SecondsOneDay = 24 * 3600
)

const (
	StringElementPlaceHolder = `__null__`
)

const (
	DefaultGroupByThreshold uint64 = 4
)

const (
	ReasonCallbackFrontendFail = "CallbackFrontendFail"
	ReasonSessionNotFound      = "SessionNotFound"
	ReasonSessionAbnormalQuit  = "SessionAbnormalQuit"
	ReasonSessionNormalQuit    = "SessionNormalQuit"
	ReasonInvalidRequest       = "InvalidRequest"
	ReasonInvalidRequestFormat = "InvalidRequestFormat"
	ReasonInvalidResponse      = "InvalidResponse"
	ReasonCallEngineFail       = "CallEngineFail"

	ActionNameSCDBQueryJobDone     = "SCDBQueryJobDone"
	ActionNameSCDBCallbackFrontend = "SCDBCallbackFrontend"
)

// NOTE: the type values are defined by DataType Enum in "api/v1/column.proto"
var StringTypeAlias = map[string]bool{"string": true, "str": true}

var IntegerTypeAlias = map[string]bool{"int32": true, "int64": true, "integer": true, "int": true}
var FloatTypeAlias = map[string]bool{"float": true, "float32": true}
var DoubleTypeAlias = map[string]bool{"double": true, "float64": true}
var DateTimeTypeAlias = map[string]bool{"datetime": true}
var TimeStampTypeAlias = map[string]bool{"timestamp": true}

func merge(ms ...map[string]bool) map[string]bool {
	res := map[string]bool{}
	for _, m := range ms {
		for k, v := range m {
			res[k] = v
		}
	}
	return res
}

var SupportTypes = merge(StringTypeAlias, IntegerTypeAlias, FloatTypeAlias, DoubleTypeAlias, DateTimeTypeAlias, TimeStampTypeAlias)

const (
	GMsm3Hash  = "GMSM3"
	Sha256Hash = "SHA256"
)
