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

package status

import (
	"errors"
	"fmt"

	"github.com/secretflow/scql/pkg/proto-gen/scql"
)

var _ error = &Status{}

type Status struct {
	code scql.Code
	err  error
}

func (s *Status) Error() string {
	return fmt.Sprintf("Error: code=%v, msg=\"%v\"", int32(s.code), s.err)
}

func (s *Status) Unwrap() error {
	return s.err
}

func (s *Status) Code() scql.Code {
	return s.code
}

func (s *Status) Message() string {
	return s.err.Error()
}

func (s *Status) ToProto() *scql.Status {
	return &scql.Status{
		Code:    int32(s.code),
		Message: s.Message(),
	}
}

func NewStatusFromProto(status *scql.Status) *Status {
	if status == nil {
		return nil
	}
	code := status.GetCode()
	msg := status.GetMessage()
	if _, ok := scql.Code_name[code]; !ok {
		return New(scql.Code_INTERNAL, msg)
	}
	return New(scql.Code(code), msg)
}

func New(code scql.Code, msg string) *Status {
	return &Status{code: code, err: errors.New(msg)}
}

func Wrap(code scql.Code, err error) *Status {
	return &Status{code: code, err: err}
}
