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
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/secretflow/scql/pkg/interpreter/translator"
)

const (
	DefaultSessionExpireTime            = 24 * time.Hour    // 24 hours
	DefaultSessionCheckInterval         = 1 * time.Minute   // 1 minute
	DefaultEngineClientTimeout          = 120 * time.Second // 120s
	DefaultInterTimeout                 = 5 * time.Second   // 5s
	DefaultBrokerIntraServerHost        = "127.0.0.1"       // default use localhost for safety
	DefaultLogLevel                     = "info"
	DefaultEngineProtocol               = "http"
	DefaultExchangeJobInfoRetryTimes    = 3
	DefaultExchangeJobInfoRetryInterval = 200 * time.Millisecond
)

type Config struct {
	IntraServer  ServerConfig  `yaml:"intra_server"`
	InterServer  ServerConfig  `yaml:"inter_server"`
	InterTimeout time.Duration `yaml:"inter_timeout"`
	// used in engine callback
	IntraHost string `yaml:"intra_host"`
	LogLevel  string `yaml:"log_level"`
	// self party code
	PartyCode          string                            `yaml:"party_code"`
	PrivatePemPath     string                            `yaml:"private_pem_path"`
	PartyInfoFile      string                            `yaml:"party_info_file"`
	Engine             EngineConfig                      `yaml:"engine"`
	Storage            StorageConf                       `yaml:"storage"`
	SecurityCompromise translator.SecurityCompromiseConf `yaml:"security_compromise"`
	// cache
	SessionExpireTime    time.Duration `yaml:"session_expire_time"`
	SessionCheckInterval time.Duration `yaml:"session_expire_check_time"`
	// exchange job info
	ExchangeJobInfoRetryTimes    int           `yaml:"exchange_job_info_retry_times"`
	ExchangeJobInfoRetryInterval time.Duration `yaml:"exchange_job_info_retry_interval"`
}

type ServerConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Protocol string `yaml:"protocol"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type StorageConf struct {
	Type            string        `yaml:"type"`
	ConnStr         string        `yaml:"conn_str"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

type EngineConfig struct {
	ClientTimeout time.Duration `yaml:"timeout"`
	Protocol      string        `yaml:"protocol"`
	ContentType   string        `yaml:"content_type"`
	Uris          []EngineUri   `yaml:"uris"`
}

type EngineUri struct {
	ForPeer string `yaml:"for_peer"`
	ForSelf string `yaml:"for_self"`
}

const (
	StorageTypeSQLite = "sqlite"
	StorageTypeMySQL  = "mysql"
)

// NewConfig construsts Config from YAML file
func NewConfig(configPath string) (*Config, error) {
	content, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %+v", configPath, err)
	}
	config := newDefaultConfig()
	if err := yaml.Unmarshal(content, config); err != nil {
		return nil, fmt.Errorf("failed to parse configuration: %+v", err)
	}
	// get conn str from env
	// if need more env parameters, please use viper
	if conStr := os.Getenv("SCDB_CONN_STR"); conStr != "" {
		config.Storage.ConnStr = conStr
	}

	// checks for config
	if len(config.Engine.Uris) == 0 {
		return nil, fmt.Errorf("NewConfig: no engine uris")
	}
	return config, nil
}

func newDefaultConfig() *Config {
	var config Config
	config.IntraServer.Host = DefaultBrokerIntraServerHost
	config.InterTimeout = DefaultInterTimeout
	config.LogLevel = DefaultLogLevel

	config.SessionExpireTime = DefaultSessionExpireTime
	config.SessionCheckInterval = DefaultSessionCheckInterval
	config.ExchangeJobInfoRetryTimes = DefaultExchangeJobInfoRetryTimes
	config.ExchangeJobInfoRetryInterval = DefaultExchangeJobInfoRetryInterval
	config.Storage = StorageConf{
		MaxIdleConns:    1,
		MaxOpenConns:    1,
		ConnMaxIdleTime: -1,
		ConnMaxLifetime: -1,
	}
	config.Engine = EngineConfig{
		ClientTimeout: DefaultEngineClientTimeout,
		Protocol:      DefaultEngineProtocol,
	}
	config.SecurityCompromise = translator.SecurityCompromiseConf{RevealGroupMark: false}
	return &config
}
