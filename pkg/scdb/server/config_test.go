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

package server

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	r := require.New(t)

	// given
	yamlContent := `
scdb_host: http://example.com
port: 8080
query_result_callback_timeout_ms: 200
session_expire_ms: 2
check_session_expire_interval_ms: 3
password_check: true
log_level: debug
storage:
  type: sqlite
  conn_str: ":memory:"
  max_idle_conns: 10
  max_open_conns: 100
  conn_max_idle_time: 2m
  conn_max_lifetime: 5m
grm:
  host: http://example.com
  timeout_ms: 5
  grm_mode: stdgrm
`
	tmpFile, err := os.CreateTemp("", "*.yaml")
	r.NoError(err)

	// cleanup temp file
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(yamlContent)
	r.NoError(err)

	tmpFile.Close()

	// when
	cfg, err := NewConfig(tmpFile.Name())

	r.NoError(err)

	// then
	expectedCfg := &Config{
		SCDBHost:                     "http://example.com",
		Port:                         "8080",
		QueryResultCbTimeoutMs:       200,
		SessionExpireMs:              2,
		CheckSessionExpireIntervalMs: 3,
		LogLevel:                     "debug",
		PasswordCheck:                true,
		Storage: StorageConf{
			Type:            "sqlite",
			ConnStr:         ":memory:",
			MaxIdleConns:    10,
			MaxOpenConns:    100,
			ConnMaxIdleTime: time.Duration(2) * time.Minute,
			ConnMaxLifetime: time.Duration(5) * time.Minute,
		},
		GRM: GRMConf{
			Host:      "http://example.com",
			TimeoutMs: 5,
			GrmMode:   "stdgrm",
		},
	}
	r.Equal(expectedCfg, cfg)
}

func TestNewConfigWithEnv(t *testing.T) {
	r := require.New(t)

	// given
	yamlContent := `
scdb_host: http://example.com
port: 8080
query_result_callback_timeout_ms: 200
session_expire_ms: 2
check_session_expire_interval_ms: 3
storage:
  type: sqlite
  conn_str: ":memory:"
  max_idle_conns: 10
  max_open_conns: 100
  conn_max_idle_time: 2m
  conn_max_lifetime: 5m
grm:
  host: http://example.com
  timeout_ms: 5
  grm_mode: toygrm
  toy_grm_conf: testdata/toy_grm.json
`
	tmpFile, err := os.CreateTemp("", "*.yaml")
	r.NoError(err)

	// cleanup temp file
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(yamlContent)
	r.NoError(err)

	tmpFile.Close()
	os.Setenv("SCDB_CONN_STR", "db-str-from-env")
	// when
	cfg, err := NewConfig(tmpFile.Name())

	r.NoError(err)

	// then
	expectedCfg := &Config{
		SCDBHost:                     "http://example.com",
		Port:                         "8080",
		QueryResultCbTimeoutMs:       200,
		SessionExpireMs:              2,
		CheckSessionExpireIntervalMs: 3,
		PasswordCheck:                false,
		Storage: StorageConf{
			Type:            "sqlite",
			ConnStr:         "db-str-from-env",
			MaxIdleConns:    10,
			MaxOpenConns:    100,
			ConnMaxIdleTime: time.Duration(2) * time.Minute,
			ConnMaxLifetime: time.Duration(5) * time.Minute,
		},
		GRM: GRMConf{
			Host:       "http://example.com",
			TimeoutMs:  5,
			GrmMode:    "toygrm",
			ToyGrmConf: "testdata/toy_grm.json",
		},
	}
	r.Equal(expectedCfg, cfg)
}
