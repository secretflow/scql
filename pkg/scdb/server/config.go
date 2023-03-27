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
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"os"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/secretflow/scql/pkg/proto-gen/spu"
)

const (
	DefaultQueryResultCbTimeoutMs       = 200
	DefaultSessionExpireMs              = 2 * 24 * 60 * 60 * 1000 // two days
	DefaultCheckSessionExpireIntervalMs = 60 * 60 * 1000          // check every hour
	SecondsToMs                         = 1000
)

type EngineConfig struct {
	ClientTimeout time.Duration `yaml:"timeout"`
	Protocol      string        `yaml:"protocol"`
	ContentType   string        `yaml:"content_type"`
	SpuRuntimeCfg *RuntimeCfg   `yaml:"spu"`
}

type RuntimeCfg struct {
	Protocol               string `yaml:"protocol"`
	Field                  string `yaml:"field"`
	FxpFractionBits        int64  `yaml:"fxp_fraction_bits"`
	EnableActionTrace      bool   `yaml:"enable_action_trace"`
	EnableTypeChecker      bool   `yaml:"enable_type_checker"`
	EnablePphloTrace       bool   `yaml:"enable_pphlo_trace"`
	EnableProcessorDump    bool   `yaml:"enable_processor_dump"`
	ProcessorDumpDir       string `yaml:"processor_dump_dir"`
	EnablePphloProfile     bool   `yaml:"enable_pphlo_profile"`
	EnableHalProfile       bool   `yaml:"enable_hal_profile"`
	RevealSecretCondition  bool   `yaml:"reveal_secret_condition"`
	RevealSecretIndicies   bool   `yaml:"reveal_secret_indicies"`
	PublicRandomSeed       uint64 `yaml:"public_random_seed"`
	FxpDivGoldschmidtIters int64  `yaml:"fxp_div_goldschmidt_iters"`
	FxpExpMode             string `yaml:"fxp_exp_mode"`
	FxpExpIters            int64  `yaml:"fxp_exp_iters"`
	FxpLogMode             string `yaml:"fxp_log_mode"`
	FxpLogIters            int64  `yaml:"fxp_log_iters"`
	FxpLogOrders           int64  `yaml:"fxp_log_orders"`
	SigmoidMode            string `yaml:"sigmoid_mode"`
	BeaverType             string `yaml:"beaver_type"`
	TtpBeaverHost          string `yaml:"ttp_beaver_host"`
}

type TlsConf struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// Config contains bootstrap configuration for SCDB
type Config struct {
	// SCDBHost is used as callback url for engine worked in async mode
	SCDBHost                     string       `yaml:"scdb_host"`
	Protocol                     string       `yaml:"protocol"`
	QueryResultCbTimeoutMs       int64        `yaml:"query_result_callback_timeout_ms"`
	SessionExpireMs              int64        `yaml:"session_expire_ms"`
	CheckSessionExpireIntervalMs int64        `yaml:"check_session_expire_interval_ms"`
	Port                         string       `yaml:"port"`
	PasswordCheck                bool         `yaml:"password_check"`
	LogLevel                     string       `yaml:"log_level"`
	TlsConfig                    TlsConf      `yaml:"tls"`
	Storage                      StorageConf  `yaml:"storage"`
	GRM                          GRMConf      `yaml:"grm"`
	Engine                       EngineConfig `yaml:"engine"`
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

type GrmType int

const (
	ToyGrmMode GrmType = iota + 1
	StdGrmMode
)

var GrmModeType = map[string]GrmType{
	"toygrm": ToyGrmMode,
	"stdgrm": StdGrmMode,
}

type GRMConf struct {
	// if GRM work in toy mod, scdb will construct grm service from json file located in ToyGrmConf
	GrmMode    string `yaml:"grm_mode"`
	Host       string `yaml:"host"`
	TimeoutMs  int64  `yaml:"timeout_ms"`
	ToyGrmConf string `yaml:"toy_grm_conf"`
}

// NewConfig constructs Config from YAML file
func NewConfig(configPath string) (*Config, error) {
	content, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %v", configPath, err)
	}
	var config Config
	if err = yaml.Unmarshal(content, &config); err != nil {
		return nil, err
	}
	// get conn str from env
	// if need more env parameters, please use viper
	if conStr := os.Getenv("SCDB_CONN_STR"); conStr != "" {
		config.Storage.ConnStr = conStr
	}
	return &config, nil
}

func NewSpuRuntimeCfg(config *RuntimeCfg) (*spu.RuntimeConfig, error) {
	if config == nil {
		return nil, nil
	}
	RuntimeConfig := &spu.RuntimeConfig{
		FxpFractionBits:        config.FxpFractionBits,
		EnableActionTrace:      config.EnableActionTrace,
		EnableTypeChecker:      config.EnableTypeChecker,
		EnablePphloTrace:       config.EnablePphloTrace,
		EnableProcessorDump:    config.EnableProcessorDump,
		ProcessorDumpDir:       config.ProcessorDumpDir,
		EnablePphloProfile:     config.EnablePphloProfile,
		EnableHalProfile:       config.EnableHalProfile,
		RevealSecretCondition:  config.RevealSecretCondition,
		RevealSecretIndicies:   config.RevealSecretIndicies,
		FxpDivGoldschmidtIters: config.FxpDivGoldschmidtIters,
		FxpExpIters:            config.FxpExpIters,
		FxpLogIters:            config.FxpLogIters,
		FxpLogOrders:           config.FxpLogOrders,
		TtpBeaverConfig:        &spu.TTPBeaverConfig{ServerHost: config.TtpBeaverHost},
	}
	val, err := rand.Int(rand.Reader, big.NewInt(int64(math.MaxInt64)))
	if err != nil {
		return nil, err
	}
	RuntimeConfig.PublicRandomSeed = val.Uint64()
	if value, exist := spu.ProtocolKind_value[config.Protocol]; exist {
		RuntimeConfig.Protocol = spu.ProtocolKind(value)
	} else if config.Protocol != "" {
		return nil, fmt.Errorf("unknown protocol kind: %s", config.Protocol)
	}
	if value, exist := spu.FieldType_value[config.Field]; exist {
		RuntimeConfig.Field = spu.FieldType(value)
	} else if config.Field != "" {
		return nil, fmt.Errorf("unknown field: %s", config.Field)
	}

	if value, exist := spu.RuntimeConfig_ExpMode_value[config.FxpExpMode]; exist {
		RuntimeConfig.FxpExpMode = spu.RuntimeConfig_ExpMode(value)
	} else if config.FxpExpMode != "" {
		return nil, fmt.Errorf("unknown fxp exp mode: %s", config.FxpExpMode)
	}

	if value, exist := spu.RuntimeConfig_LogMode_value[config.FxpLogMode]; exist {
		RuntimeConfig.FxpLogMode = spu.RuntimeConfig_LogMode(value)
	} else if config.FxpLogMode != "" {
		return nil, fmt.Errorf("unknown fxp log mode: %s", config.FxpLogMode)
	}

	if value, exist := spu.RuntimeConfig_SigmoidMode_value[config.SigmoidMode]; exist {
		RuntimeConfig.SigmoidMode = spu.RuntimeConfig_SigmoidMode(value)
	} else if config.SigmoidMode != "" {
		return nil, fmt.Errorf("unknown sigmoid mode: %s", config.SigmoidMode)
	}
	if value, exist := spu.RuntimeConfig_BeaverType_value[config.BeaverType]; exist {
		RuntimeConfig.BeaverType = spu.RuntimeConfig_BeaverType(value)
	} else if config.BeaverType != "" {
		return nil, fmt.Errorf("unknown beaver type: %s", config.BeaverType)
	}

	return RuntimeConfig, nil
}
