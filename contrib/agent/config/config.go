// Copyright 2024 Ant Group Co., Ltd.
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
)

const (
	DefaultWaitTimeout      = 60 * time.Second
	DefaultWaitQueryTimeout = 1 * time.Hour
	DefaultScqlImage        = "scql-image"
)

type Config struct {
	TaskConfig    string         `yaml:"task_config"`
	ClusterDefine string         `yaml:"cluster_define"`
	Kuscia        *KusciaApiConf `yaml:"kuscia"`
	// default timeout for waiting for kuscia schedule/project invite/table creation/...
	WaitTimeout time.Duration `yaml:"wait_timeout"`
	// timeout for query task to be finished
	WaitQueryTimeout time.Duration `yaml:"wait_query_timeout"`
	ScqlImage        string        `yaml:"scql_image"`
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

func NewConfig(configPath string) (*Config, error) {
	content, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %+v", configPath, err)
	}
	config := &Config{
		WaitTimeout:      DefaultWaitTimeout,
		WaitQueryTimeout: DefaultWaitQueryTimeout,
		ScqlImage:        DefaultScqlImage,
	}
	if err := yaml.Unmarshal(content, config); err != nil {
		return nil, fmt.Errorf("failed to parse configuration: %+v", err)
	}

	return config, nil
}
