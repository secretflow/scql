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

package config

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/audit"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
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
  audit_detail_file: another/detail.log
storage:
  type: sqlite
  conn_str: ":memory:"
  max_idle_conns: 10
  max_open_conns: 100
  conn_max_idle_time: 2m
  conn_max_lifetime: 5m
engine:
  timeout: 120s
  protocol: http
  content_type: application/json
  spu: |
    {
        "protocol": "SEMI2K",
        "field": "FM64",
        "ttp_beaver_config": {"server_host": "127.0.0.1"}
    }
party_auth:
  method: pubkey
  enable_timestamp_check: true
  validity_period: 3m
security_compromise:
  group_by_threshold: 3
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
		EnableAuditLogger:    true,
		AuditConfig: audit.AuditConf{
			AuditLogFile:            "another/audit.log",
			AuditDetailFile:         "another/detail.log",
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
		Engine: EngineConfig{
			ClientMode:    DefaultEngineClientMode,
			ClientTimeout: 120 * time.Second,
			Protocol:      "http",
			ContentType:   "application/json",
			SpuRuntimeCfg: `{
				"protocol": "SEMI2K",
				"field": "FM64",
				"ttp_beaver_config": {"server_host": "127.0.0.1"}
}
`,
		},
		PartyAuth: PartyAuthConf{
			Method:               "pubkey",
			EnableTimestampCheck: true,
			ValidityPeriod:       time.Minute * 3,
		},
		SecurityCompromise: SecurityCompromiseConf{GroupByThreshold: 3},
	}
	expectedCfg.Engine.SpuRuntimeCfg = strings.ReplaceAll(expectedCfg.Engine.SpuRuntimeCfg, "\t", " ")
	r.Equal(expectedCfg, cfg)
	spuConf, err := NewSpuRuntimeCfg(cfg.Engine.SpuRuntimeCfg)
	r.NoError(err)
	r.Equal(spu.ProtocolKind_SEMI2K, spuConf.Protocol)
	r.Equal(spu.FieldType_FM64, spuConf.Field)
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
engine:
  timeout: 120s
  protocol: http
  content_type: application/json
  spu: |
    {
        "protocol": "SEMI2K",
        "field": "FM64"
    }
party_auth:
  method: token
  enable_timestamp_check: true
  validity_period: 3m
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
		EnableAuditLogger:    true,
		AuditConfig: audit.AuditConf{
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
		Engine: EngineConfig{
			ClientMode:    DefaultEngineClientMode,
			ClientTimeout: 120 * time.Second,
			Protocol:      "http",
			ContentType:   "application/json",
			SpuRuntimeCfg: `{
				"protocol": "SEMI2K",
				"field": "FM64"
}
`,
		},
		PartyAuth: PartyAuthConf{
			Method:               "token",
			EnableTimestampCheck: true,
			ValidityPeriod:       time.Minute * 3,
		},
	}
	expectedCfg.Engine.SpuRuntimeCfg = strings.ReplaceAll(expectedCfg.Engine.SpuRuntimeCfg, "\t", " ")
	r.Equal(expectedCfg, cfg)
}
