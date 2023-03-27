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
	"testing"

	"github.com/stretchr/testify/require"

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func makeStatusError(code proto.Code) error {
	return New(code, "some bad happened")
}

func makeError(msg string) error {
	return errors.New(msg)
}

func TestErrorAsStatus(t *testing.T) {
	r := require.New(t)

	{
		err := makeStatusError(proto.Code_NOT_FOUND)

		var status *Status
		r.True(errors.As(err, &status), "err must be status error")
		r.Equal(status.code, proto.Code_NOT_FOUND)
	}

	{
		err := makeError("xxx")

		var status *Status
		r.False(errors.As(err, &status), "err is not status error")
	}

	{
		// test case with error chains
		err1 := fmt.Errorf("op xxx is not implemented")

		err2 := Wrap(proto.Code_NOT_SUPPORTED, err1)

		// Wrapping errors with %w
		// Reference: https://go.dev/blog/go1.13-errors
		err3 := fmt.Errorf("some bad happend: %w", err2)

		var status *Status
		r.True(errors.As(err3, &status), "err3 must be status error")
		r.Equal(status.code, proto.Code_NOT_SUPPORTED)
	}
}
