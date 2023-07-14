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

	"github.com/secretflow/scql/pkg/audit"
)

func TestNewConfig(t *testing.T) {
	r := require.New(t)

	// given
	yamlContent := `
scdb_host: http://example.com
port: 8080
protocol: http
query_result_callback_timeout: 1m
session_expire_time: 1h
session_expire_check_time: 20s
password_check: true
log_level: debug
audit:
  audit_log_file: another/audit.log
  audit_detail_file: another/plan.log
storage:
  type: sqlite
  conn_str: ":memory:"
  max_idle_conns: 10
  max_open_conns: 100
  conn_max_idle_time: 2m
  conn_max_lifetime: 5m
grm:
  grm_mode: stdgrm
  host: http://example.com
  timeout: 5s
engine:
  timeout: 120s
  protocol: http
  content_type: application/json
  spu:
    protocol: SEMI2K
    field: FM64
    sigmoid_mode: SIGMOID_REAL
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
		SCDBHost:             "http://example.com",
		Port:                 8080,
		Protocol:             "http",
		QueryResultCbTimeout: 1 * time.Minute,
		SessionExpireTime:    1 * time.Hour,
		SessionCheckInterval: 20 * time.Second,
		LogLevel:             "debug",
		AuditConfig: audit.AuditConf{
			EnableAuditLog:          true,
			AuditLogFile:            "another/audit.log",
			AuditDetailFile:         "another/plan.log",
			AuditMaxSizeInMegaBytes: DefaultAuditMaxSizeInMegaBytes,
			AuditMaxBackupsCount:    DefaultAuditMaxBackupsCount,
			AuditMaxAgeInDays:       DefaultAuditMaxAgeInDays,
			AuditMaxCompress:        DefaultAuditMaxCompress,
		},
		PasswordCheck: true,
		Storage: StorageConf{
			Type:            "sqlite",
			ConnStr:         ":memory:",
			MaxIdleConns:    10,
			MaxOpenConns:    100,
			ConnMaxIdleTime: time.Duration(2) * time.Minute,
			ConnMaxLifetime: time.Duration(5) * time.Minute,
		},
		GRM: GRMConf{
			GrmMode: "stdgrm",
			Host:    "http://example.com",
			Timeout: 5 * time.Second,
		},
		Engine: EngineConfig{
			ClientTimeout: 120 * time.Second,
			Protocol:      "http",
			ContentType:   "application/json",
			SpuRuntimeCfg: &RuntimeCfg{
				Protocol:    "SEMI2K",
				Field:       "FM64",
				SigmoidMode: "SIGMOID_REAL",
			},
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
protocol: http
query_result_callback_timeout: 1m
session_expire_time: 1h
session_expire_check_time: 20s
log_level: info
storage:
  type: sqlite
  conn_str: ":memory:"
  max_idle_conns: 10
  max_open_conns: 100
  conn_max_idle_time: 2m
  conn_max_lifetime: 5m
grm:
  grm_mode: toygrm
  toy_grm_conf: testdata/toy_grm.json
engine:
  timeout: 120s
  protocol: http
  content_type: application/json
  spu:
    protocol: SEMI2K
    field: FM64
    sigmoid_mode: SIGMOID_REAL
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
		SCDBHost:             "http://example.com",
		Port:                 8080,
		Protocol:             "http",
		QueryResultCbTimeout: 1 * time.Minute,
		SessionExpireTime:    1 * time.Hour,
		SessionCheckInterval: 20 * time.Second,
		PasswordCheck:        false,
		LogLevel:             "info",
		AuditConfig: audit.AuditConf{
			EnableAuditLog:          true,
			AuditLogFile:            DefaultAuditLogFile,
			AuditDetailFile:         DefaultAudiDetailFile,
			AuditMaxSizeInMegaBytes: DefaultAuditMaxSizeInMegaBytes,
			AuditMaxBackupsCount:    DefaultAuditMaxBackupsCount,
			AuditMaxAgeInDays:       DefaultAuditMaxAgeInDays,
			AuditMaxCompress:        DefaultAuditMaxCompress,
		},
		Storage: StorageConf{
			Type:            "sqlite",
			ConnStr:         "db-str-from-env",
			MaxIdleConns:    10,
			MaxOpenConns:    100,
			ConnMaxIdleTime: 2 * time.Minute,
			ConnMaxLifetime: 5 * time.Minute,
		},
		GRM: GRMConf{
			GrmMode:    "toygrm",
			ToyGrmConf: "testdata/toy_grm.json",
		},
		Engine: EngineConfig{
			ClientTimeout: 120 * time.Second,
			Protocol:      "http",
			ContentType:   "application/json",
			SpuRuntimeCfg: &RuntimeCfg{
				Protocol:    "SEMI2K",
				Field:       "FM64",
				SigmoidMode: "SIGMOID_REAL",
			},
		},
	}
	r.Equal(expectedCfg, cfg)
}
