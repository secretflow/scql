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

package parallel

import "errors"

func ParallelRun[resultT any, t any](inputs []t, fn func(input t) (resultT, error)) ([]resultT, error) {
	type errStruct struct {
		result resultT
		err    error
	}
	retCh := make(chan errStruct, len(inputs))
	for _, input := range inputs {
		go func(input t) {
			res, resError := fn(input)
			retCh <- errStruct{res, resError}
		}(input)
	}
	var err error
	var results []resultT
	for range inputs {
		retData := <-retCh
		results = append(results, retData.result)
		err = errors.Join(err, retData.err)
	}
	return results, err
}
