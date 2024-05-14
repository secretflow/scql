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
package brokerutil

import (
	"fmt"
	"os"
)

const (
	AlicePemFilKey   = "alice_pem_file"
	BobPemFileKey    = "bob_pem_file"
	CarolPemFileKey  = "carol_pem_file"
	PartyInfoFileKey = "party_info_file"
)

func CreateTestPemFiles(portsMap map[string]int, tempDir string) (filesMap map[string]string, err error) {
	filesMap = make(map[string]string)
	fileContents := map[string]string{
		AlicePemFilKey: `-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEICh+ZViILyFPq658OPYq6iKlSj802q1LfrmrV2i1GWcn
-----END PRIVATE KEY-----
`,
		BobPemFileKey: `-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIOlHfgiMd9iEQtlJVdWGSBLyGnqIVc+sU3MAcfpJcP4S
-----END PRIVATE KEY-----
`,
		CarolPemFileKey: `-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFvceWQFFw2LNbqVrJs1nSZji0HQZABKzTfrOABTBn5F
-----END PRIVATE KEY-----
`,
	}
	var toRemoveFilePath []string
	defaultPorts := map[string]int{
		"alice": 8082,
		"bob":   8084,
		"carol": 8086,
	}
	// use default portsMap if no portsMap are specified
	for portName, port := range portsMap {
		defaultPorts[portName] = port
	}
	for key, value := range fileContents {
		var file *os.File
		file, err = os.CreateTemp(tempDir, "*.pem")
		if err != nil {
			return
		}
		toRemoveFilePath = append(toRemoveFilePath, file.Name())
		defer file.Close()
		_, err = file.WriteString(value)
		if err != nil {
			return
		}
		filesMap[key] = file.Name()
	}
	var file *os.File
	file, err = os.CreateTemp(tempDir, "*.json")
	if err != nil {
		return
	}
	toRemoveFilePath = append(toRemoveFilePath, file.Name())
	defer file.Close()
	filesMap[PartyInfoFileKey] = file.Name()
	_, err = file.WriteString(fmt.Sprintf(`
	{
		"participants": [
		  {
			"party_code": "alice",
			"endpoint": "http://localhost:%d",
			"pubkey": "MCowBQYDK2VwAyEAqhfJVWZX32aVh00fUqfrbrGkwboi8ZpTpybLQ4rbxoA="
		  },
		  {
			"party_code": "bob",
			"endpoint": "http://localhost:%d",
			"pubkey": "MCowBQYDK2VwAyEAN3w+v2uks/QEaVZiprZ8oRChMkBOZJSAl6V/5LvOnt4="
		  },
		  {
			"party_code": "carol",
			"endpoint": "http://localhost:%d",
			"pubkey": "MCowBQYDK2VwAyEANhiAXTvL4x2jYUiAbQRo9XuOTrFFnAX4Q+YlEAgULs8="
		  }
		]
	  }`, defaultPorts["alice"], defaultPorts["bob"], defaultPorts["carol"]))
	return
}
