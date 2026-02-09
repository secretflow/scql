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

package url

import (
	"strings"
)

// JoinHostPath join host and path by concat them directly with only extra proccessing flows:
// 1. Add missig slash '/' between host and path
// 2. Remove duplidate slash '/' between host and path
func JoinHostPath(host, path string) string {
	// note: path.Join("http://localhost/", "/public/query") will produce "http:/localhost/public/query", prefix 'http:/' but not 'http://'
	if strings.HasPrefix(path, "/") {
		return strings.TrimSuffix(host, "/") + path
	}
	return strings.TrimSuffix(host, "/") + "/" + path
}
