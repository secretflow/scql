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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultSessionExpireTime            = 24 * time.Hour    // 24 hours
	DefaultSessionCheckInterval         = 30 * time.Minute  // 30 minute
	DefaultEngineClientTimeout          = 120 * time.Second // 120s
	DefaultInterTimeout                 = 5 * time.Second   // 5s
	DefaultBrokerIntraServerHost        = "127.0.0.1"       // default use localhost for safety
	DefaultLogLevel                     = "info"
	DefaultEngineProtocol               = "http"
	DefaultExchangeJobInfoRetryTimes    = 3
	DefaultExchangeJobInfoRetryInterval = 200 * time.Millisecond
	DefaultEngineClientMode             = "GRPC"
)

type SecurityCompromiseConf struct {
	GroupByThreshold uint64 `yaml:"group_by_threshold"`
	RevealGroupMark  bool   `yaml:"reveal_group_mark"`
}

type Config struct {
	IntraServer  ServerConfig  `yaml:"intra_server"`
	InterServer  ServerConfig  `yaml:"inter_server"`
	InterTimeout time.Duration `yaml:"inter_timeout"`
	// used in engine callback
	IntraHost string `yaml:"intra_host"`
	LogLevel  string `yaml:"log_level"`
	// self party code
	PartyCode      string `yaml:"party_code"`
	PrivateKeyPath string `yaml:"private_key_path"`
	InterHost      string `yaml:"inter_host"`
	// base64 encoded PEM-encoded private key
	PrivateKeyData     string                 `yaml:"private_key_data"`
	PartyInfoFile      string                 `yaml:"party_info_file"`
	Engine             EngineConfig           `yaml:"engine"`
	Storage            StorageConf            `yaml:"storage"`
	SecurityCompromise SecurityCompromiseConf `yaml:"security_compromise"`
	Discovery          DiscoveryConf          `yaml:"discovery"`
	// cache
	SessionExpireTime    time.Duration `yaml:"session_expire_time"`
	SessionCheckInterval time.Duration `yaml:"session_expire_check_time"`
	PersistSession       bool          `yaml:"persist_session"`
	// streaming
	Batched bool `yaml:"batched"`
	// exchange job info
	ExchangeJobInfoRetryTimes    int           `yaml:"exchange_job_info_retry_times"`
	ExchangeJobInfoRetryInterval time.Duration `yaml:"exchange_job_info_retry_interval"`
}

type DiscoveryConf struct {
	// supported discovery types: "file", "kuscia"
	Type   string          `yaml:"type"`
	File   string          `yaml:"file"`
	Consul *ConsulApiConf  `yaml:"consul"`
	Kuscia *KusciaDiscConf `yaml:"kuscia"`
}

type TLSConf struct {
	Mode       string `yaml:"mode"`
	CertPath   string `yaml:"cert"`
	KeyPath    string `yaml:"key"`
	CACertPath string `yaml:"cacert"`
}

type ConsulApiConf struct {
	Address    string   `yaml:"address"`
	Token      string   `yaml:"token"`
	TLS        *TLSConf `yaml:"tls"`
	Datacenter string   `yaml:"datacenter"`
}

type KusciaDiscConf struct {
	KusciaApiConf `yaml:",inline"`
}

type KusciaApiConf struct {
	Endpoint string `yaml:"endpoint"`
	// supported tls mode: "NOTLS", "TLS", "MTLS"
	// - Token is needed if tls_mode == TLS or tls_mode == MTLS
	TLSMode string `yaml:"tls_mode"`
	Cert    string `yaml:"cert"`
	Key     string `yaml:"key"`
	CaCert  string `yaml:"cacert"`
	Token   string `yaml:"token"`
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
	ClientMode    string        `yaml:"mode"`
	ClientTimeout time.Duration `yaml:"timeout"`
	Protocol      string        `yaml:"protocol"`
	TLSCfg        TLSConf       `yaml:"tls_cfg"`
	ContentType   string        `yaml:"content_type"`
	// supported schedulers: "kuscia" or "naive"
	Scheduler string `yaml:"scheduler"`
	// Uris valid when scheduler is empty or "naive"
	Uris []EngineUri `yaml:"uris"`
	// KusciaSchedulerOption valid only when scheduler is "kuscia"
	KusciaSchedulerOption *KusciaSchedulerConf `yaml:"kuscia_scheduler"`
}

type KusciaSchedulerConf struct {
	KusciaApiConf `yaml:",inline"`

	MaxPollTimes int           `yaml:"max_poll_times"`
	PollInterval time.Duration `yaml:"poll_interval"`
	MaxWaitTime  time.Duration `yaml:"max_wait_time"`

	// default value is false, set true for debugging purpose
	KeepJobAliveForDebug bool `yaml:"keep_job_alive_for_debug"`
}

type EngineUri struct {
	ForPeer string `yaml:"for_peer"`
	ForSelf string `yaml:"for_self"`
}

const (
	StorageTypeSQLite   = "sqlite"
	StorageTypeMySQL    = "mysql"
	StorageTypePostgres = "postgres"
)

const (
	NoTLS     string = "notls"
	TLS       string = "tls"
	MutualTLS string = "mtls"
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
	if strings.ToLower(config.Engine.Scheduler) != "kuscia" && len(config.Engine.Uris) == 0 {
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
	config.PersistSession = true
	config.ExchangeJobInfoRetryTimes = DefaultExchangeJobInfoRetryTimes
	config.ExchangeJobInfoRetryInterval = DefaultExchangeJobInfoRetryInterval
	config.Storage = StorageConf{
		MaxIdleConns:    1,
		MaxOpenConns:    1,
		ConnMaxIdleTime: -1,
		ConnMaxLifetime: -1,
	}
	config.Engine = EngineConfig{
		ClientMode:    DefaultEngineClientMode,
		ClientTimeout: DefaultEngineClientTimeout,
		Protocol:      DefaultEngineProtocol,
		KusciaSchedulerOption: &KusciaSchedulerConf{
			MaxPollTimes: 20,
			PollInterval: time.Second,
			MaxWaitTime:  time.Minute,
		},
	}
	return &config
}

func LoadTLSConfig(mode, cacertPath, certPath, keyPath string) (*tls.Config, error) {
	var tlsConfig *tls.Config
	cacert, err := os.ReadFile(cacertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cacert file")
	}
	cacertPool := x509.NewCertPool()
	if !cacertPool.AppendCertsFromPEM(cacert) {
		return nil, fmt.Errorf("failed to append CA certificates")
	}

	lowerMode := strings.ToLower(mode)
	switch lowerMode {
	case TLS:
		tlsConfig = &tls.Config{
			RootCAs: cacertPool,
		}
	case MutualTLS:
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load X509 key pair: %w", err)
		}

		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      cacertPool,
		}
	}
	return tlsConfig, nil
}
