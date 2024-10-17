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
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"os"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/secretflow/scql/pkg/audit"
	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/parser/auth"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
	"github.com/secretflow/scql/pkg/util/message"
)

const (
	DefaultQueryResultCbTimeout = 200 * time.Millisecond // 200ms
	DefaultSessionExpireTime    = 2 * 24 * time.Hour     // 2 days
	DefaultSessionCheckInterval = 1 * time.Hour          // 1 hours
	DefaultClientTimeout        = 120 * time.Second      // 120s
	DefaultProtocol             = "https"
	DefaultLogLevel             = "info"
	DefaultEngineClientMode     = "HTTP"
)

const (
	DefaultEnableAudit             = true
	DefaultAuditLogFile            = "audit/audit.log"
	DefaultAudiDetailFile          = "audit/detail.log"
	DefaultAuditMaxSizeInMegaBytes = 500
	DefaultAuditMaxBackupsCount    = 10
	DefaultAuditMaxAgeInDays       = 180
	DefaultAuditMaxCompress        = false
)

type EngineConfig struct {
	ClientMode    string        `yaml:"mode"`
	ClientTimeout time.Duration `yaml:"timeout"`
	Protocol      string        `yaml:"protocol"`
	ContentType   string        `yaml:"content_type"`
	SpuRuntimeCfg string        `yaml:"spu"`
	TLSCfg        TlsConf       `yaml:"tls"`
}

type TlsConf struct {
	Mode       string `yaml:"mode"`
	CACertFile string `yaml:"cacert_file"`
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
}

type SecurityCompromiseConf struct {
	GroupByThreshold uint64 `yaml:"group_by_threshold"`
	RevealGroupMark  bool   `yaml:"reveal_group_mark"`
}

// Config contains bootstrap configuration for SCDB
type Config struct {
	// SCDBHost is used as callback url for engine worked in async mode
	SCDBHost             string                 `yaml:"scdb_host"`
	Port                 int                    `yaml:"port"`
	Protocol             string                 `yaml:"protocol"`
	QueryResultCbTimeout time.Duration          `yaml:"query_result_callback_timeout"`
	SessionExpireTime    time.Duration          `yaml:"session_expire_time"`
	SessionCheckInterval time.Duration          `yaml:"session_expire_check_time"`
	AuthEncType          string                 `yaml:"auth_enc_type"`
	PasswordCheck        bool                   `yaml:"password_check"`
	LogLevel             string                 `yaml:"log_level"`
	EnableAuditLogger    bool                   `yaml:"enable_audit_logger"`
	AuditConfig          audit.AuditConf        `yaml:"audit"`
	TlsConfig            TlsConf                `yaml:"tls"`
	Storage              StorageConf            `yaml:"storage"`
	Engine               EngineConfig           `yaml:"engine"`
	SecurityCompromise   SecurityCompromiseConf `yaml:"security_compromise"`
	PartyAuth            PartyAuthConf          `yaml:"party_auth"`
}

const (
	StorageTypeSQLite = "sqlite"
	StorageTypeMySQL  = "mysql"
)

type StorageConf struct {
	Type            string        `yaml:"type"`
	ConnStr         string        `yaml:"conn_str"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

const (
	PartyAuthMethodNone   = "none"
	PartyAuthMethodToken  = "token"
	PartyAuthMethodPubKey = "pubkey"
)

type PartyAuthConf struct {
	Method               string        `yaml:"method"`
	EnableTimestampCheck bool          `yaml:"enable_timestamp_check"`
	ValidityPeriod       time.Duration `yaml:"validity_period"`
}

// NewConfig constructs Config from YAML file
func NewConfig(configPath string) (*Config, error) {
	content, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %v", configPath, err)
	}
	config := NewDefaultConfig()
	if err = yaml.Unmarshal(content, &config); err != nil {
		return nil, err
	}
	if err := CheckConfigValues(config); err != nil {
		return nil, err
	}
	// get conn str from env
	// if need more env parameters, please use viper
	if conStr := os.Getenv("SCDB_CONN_STR"); conStr != "" {
		config.Storage.ConnStr = conStr
	}
	if config.AuthEncType == constant.GMsm3Hash {
		auth.EncHashType = constant.GMsm3Hash
	}
	return config, nil
}

func CheckConfigValues(config *Config) error {
	if config.Protocol == DefaultProtocol && (config.TlsConfig.CertFile == "" || config.TlsConfig.KeyFile == "") {
		return fmt.Errorf("SCDB work in https, cert_file or key_file couldn't be empty")
	}
	if config.Engine.SpuRuntimeCfg == "" {
		return fmt.Errorf("SpuRuntimeCfg is empty")
	}
	return nil
}

func NewDefaultConfig() *Config {
	var config Config
	config.LogLevel = DefaultLogLevel
	config.EnableAuditLogger = DefaultEnableAudit
	config.AuditConfig = audit.AuditConf{
		AuditLogFile:            DefaultAuditLogFile,
		AuditDetailFile:         DefaultAudiDetailFile,
		AuditMaxSizeInMegaBytes: DefaultAuditMaxSizeInMegaBytes,
		AuditMaxBackupsCount:    DefaultAuditMaxBackupsCount,
		AuditMaxAgeInDays:       DefaultAuditMaxAgeInDays,
		AuditMaxCompress:        DefaultAuditMaxCompress,
	}
	config.QueryResultCbTimeout = DefaultQueryResultCbTimeout
	config.SessionExpireTime = DefaultSessionExpireTime
	config.SessionCheckInterval = DefaultSessionCheckInterval
	config.Protocol = DefaultProtocol
	config.Storage = StorageConf{
		MaxIdleConns:    1,
		MaxOpenConns:    1,
		ConnMaxIdleTime: -1,
		ConnMaxLifetime: -1,
	}
	config.Engine = EngineConfig{
		ClientMode:    DefaultEngineClientMode,
		ClientTimeout: DefaultClientTimeout,
		Protocol:      DefaultProtocol,
	}
	config.PartyAuth = PartyAuthConf{
		Method:               PartyAuthMethodPubKey,
		EnableTimestampCheck: true,
		ValidityPeriod:       30 * time.Second, // 30s
	}
	return &config
}

func NewSpuRuntimeCfg(confStr string) (*spu.RuntimeConfig, error) {
	if confStr == "" {
		return nil, fmt.Errorf("nil spu runtime config is unsupported")
	}
	runtimeConf := spu.RuntimeConfig{}
	err := message.ProtoUnmarshal([]byte(confStr), &runtimeConf)
	if err != nil {
		return nil, err
	}
	if runtimeConf.PublicRandomSeed == 0 {
		val, err := rand.Int(rand.Reader, big.NewInt(int64(math.MaxInt64)))
		if err != nil {
			return nil, err
		}
		runtimeConf.PublicRandomSeed = val.Uint64()
	}
	return &runtimeConf, nil
}
