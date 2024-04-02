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

// Due to circular references, please avoid moving this file to testutil.
package testdata

import "os"

func CreateTestPemFiles(path string) error {
	fileNames := []string{"private_key_alice.pem", "private_key_bob.pem", "private_key_carol.pem"}
	fileContents := []string{
		`-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEICh+ZViILyFPq658OPYq6iKlSj802q1LfrmrV2i1GWcn
-----END PRIVATE KEY-----
`,
		`-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIOlHfgiMd9iEQtlJVdWGSBLyGnqIVc+sU3MAcfpJcP4S
-----END PRIVATE KEY-----
`,
		`-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFvceWQFFw2LNbqVrJs1nSZji0HQZABKzTfrOABTBn5F
-----END PRIVATE KEY-----
`,
	}
	for i, fileName := range fileNames {
		filePath := path + "/" + fileName
		file, err := os.Create(filePath)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = file.WriteString(fileContents[i])
		if err != nil {
			return err
		}
	}
	filePath := path + "/" + "party_info_test.json"
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(`
	{
		"participants": [
		  {
			"party_code": "alice",
			"endpoint": "http://localhost:8082",
			"pubkey": "MCowBQYDK2VwAyEAqhfJVWZX32aVh00fUqfrbrGkwboi8ZpTpybLQ4rbxoA="
		  },
		  {
			"party_code": "bob",
			"endpoint": "http://localhost:8084",
			"pubkey": "MCowBQYDK2VwAyEAN3w+v2uks/QEaVZiprZ8oRChMkBOZJSAl6V/5LvOnt4="
		  },
		  {
			"party_code": "carol",
			"endpoint": "http://localhost:8086",
			"pubkey": "MCowBQYDK2VwAyEANhiAXTvL4x2jYUiAbQRo9XuOTrFFnAX4Q+YlEAgULs8="
		  }
		]
	  }`)
	if err != nil {
		return err
	}
	return nil
}
