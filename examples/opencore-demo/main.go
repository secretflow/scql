// Copyright 2026 Ant Group Co., Ltd.
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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/secretflow/scql/pkg/config"
	"github.com/secretflow/scql/pkg/executor"
	"github.com/secretflow/scql/pkg/interpreter/compiler"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	v1 "github.com/secretflow/scql/pkg/proto-gen/scql/v1alpha1"
	"github.com/secretflow/scql/pkg/util/tableview"
)

type Config struct {
	SQL                 string                     `json:"sql"`
	Catalog             *pb.Catalog                `json:"catalog"`               // Catalog metadata
	SecurityConf        *v1.CompilerSecurityConfig `json:"security_conf"`         // Security configuration
	EngineEndpoints     map[string]string          `json:"engine_endpoints"`      // Map of party code to engine endpoint (for client connection)
	EngineLinkEndpoints map[string]string          `json:"engine_link_endpoints"` // Map of party code to link endpoint (for engine-to-engine communication)
	EngineClientType    string                     `json:"engine_client_type"`    // "HTTP" or "GRPC", defaults to "HTTP"
	EngineTimeout       int                        `json:"engine_timeout"`        // timeout in seconds
	TLSCACert           string                     `json:"tls_ca_cert"`           // Path to CA certificate (for GRPC)
	Issuer              string                     `json:"issuer"`                // party code
	CompileOpts         *v1.CompileOptions         `json:"compile_opts"`          // Compile options
}

func main() {
	var configFile string
	var showHelp bool
	var sql string

	flag.StringVar(&configFile, "config", "", "Path to config JSON file")
	flag.BoolVar(&showHelp, "help", false, "Show help message")
	flag.StringVar(&sql, "sql", "", "SQL query to execute (overrides config file SQL if provided)")
	flag.Parse()

	if showHelp || configFile == "" {
		printHelp()
		os.Exit(0)
	}

	// Load configuration
	config, err := loadConfig(configFile)
	if err != nil {
		logrus.Fatalf("Failed to load config: %v", err)
	}

	if sql != "" {
		config.SQL = sql
	}

	// Execute the workflow
	if err := execute(config); err != nil {
		logrus.Fatalf("Execution failed: %v", err)
	}

	logrus.Info("Execution completed successfully")
}

func execute(config *Config) error {
	ctx := context.Background()

	// Step 1: Compile SQL to execution plan
	logrus.Info("Step 1: Compiling SQL to execution plan...")

	compiledPlan, err := compileSQL(ctx, config)
	if err != nil {
		return fmt.Errorf("compilation failed: %w", err)
	}

	logrus.Infof("Compilation successful. SubGraphs: %d", len(compiledPlan.SubGraphs))

	// Step 2: Execute the plan on engine
	logrus.Info("Step 2: Executing plan on engine...")

	result, err := executePlan(ctx, config, compiledPlan)
	if err != nil {
		return fmt.Errorf("execution failed: %w", err)
	}

	// Step 3: Display results
	logrus.Info("Step 3: Query results:")
	displayResults(result)

	return nil
}

func compileSQL(ctx context.Context, config *Config) (*pb.CompiledPlan, error) {
	// Validate required fields
	if config.Catalog == nil {
		return nil, fmt.Errorf("catalog is required")
	}
	if config.SecurityConf == nil {
		return nil, fmt.Errorf("security_conf is required")
	}

	// Use default compile options if not provided
	compileOpts := config.CompileOpts
	if compileOpts == nil {
		compileOpts = &v1.CompileOptions{}
	}

	// Build compile request
	req := &v1.CompileSQLRequest{
		Query:          config.SQL,
		Issuer:         &pb.PartyId{Code: config.Issuer},
		Catalog:        config.Catalog,
		CompileOpts:    compileOpts,
		IssueTime:      timestamppb.Now(),
		SecurityConfig: config.SecurityConf,
		AdditionalInfo: &v1.AdditionalInfoSpec{
			NeedOperatorGraph: false,
		},
	}

	// Compile
	compiledPlan, err := compiler.Compile(ctx, req)
	if err != nil {
		return nil, err
	}

	return compiledPlan, nil
}

func executePlan(ctx context.Context, conf *Config, compiledPlan *pb.CompiledPlan) (*pb.QueryResult, error) {
	if len(compiledPlan.SubGraphs) == 0 {
		return nil, fmt.Errorf("no subgraphs in compiled plan")
	}

	// Create a unique session ID for this execution
	sessionID := fmt.Sprintf("opencore-demo-%d", time.Now().Unix())

	// Build JobStartParams with all parties
	jobParams := &pb.JobStartParams{
		JobId:         sessionID,
		PartyCode:     conf.Issuer,                 // The party initiating the query
		SpuRuntimeCfg: compiledPlan.SpuRuntimeConf, // SPU configuration from compiled plan
	}

	// Copy party info from compiled plan
	if compiledPlan.Parties != nil {
		for i, party := range compiledPlan.Parties {
			host := ""
			// First try to get from engine_link_endpoints map (for engine-to-engine communication)
			if conf.EngineLinkEndpoints != nil {
				if endpoint, ok := conf.EngineLinkEndpoints[party.Code]; ok {
					host = endpoint
				}
			}
			if host == "" {
				return nil, fmt.Errorf("no engine link endpoint configured for party %s", party.Code)
			}
			jobParams.Parties = append(jobParams.Parties, &pb.JobStartParams_Party{
				Code: party.Code,
				Name: "", // Name is optional
				Host: host,
				Rank: int32(i),
			})
		}
	}

	// Setup engine client
	timeout := time.Duration(conf.EngineTimeout) * time.Second
	if timeout == 0 {
		timeout = 300 * time.Second // default 5 minutes
	}

	// Determine client type
	clientType := conf.EngineClientType
	if clientType == "" {
		clientType = executor.EngineClientTypeHTTP // default to HTTP
	}

	// Setup TLS config only for GRPC client type
	var tlsConfig *config.TLSConf
	if clientType == executor.EngineClientTypeGRPC && conf.TLSCACert != "" {
		tlsConfig = &config.TLSConf{
			Mode:       "tls",
			CACertPath: conf.TLSCACert,
		}
	}

	engineClient := executor.NewEngineClient(
		clientType,
		timeout,
		tlsConfig,
		"application/json",
		"", // protocol is not used anymore
	)

	// Send execution request to all parties
	type executionResult struct {
		partyCode string
		response  *pb.RunExecutionPlanResponse
		err       error
	}

	results := make(chan executionResult, len(compiledPlan.SubGraphs))

	for partyCode, subgraph := range compiledPlan.SubGraphs {
		go func(party string, graph *pb.SubGraph) {
			// Determine the engine endpoint for this party
			engineEndpoint := ""
			if conf.EngineEndpoints != nil {
				if endpoint, ok := conf.EngineEndpoints[party]; ok {
					engineEndpoint = endpoint
				}
			}
			if engineEndpoint == "" {
				results <- executionResult{
					partyCode: party,
					err:       fmt.Errorf("no engine endpoint configured for party %s", party),
				}
				return
			}

			// Create execution request for this party with its own party code
			partyJobParams := &pb.JobStartParams{
				JobId:         jobParams.JobId,
				PartyCode:     party, // Set this engine's party code
				Parties:       jobParams.Parties,
				SpuRuntimeCfg: jobParams.SpuRuntimeCfg, // Copy SPU config
				TimeZone:      jobParams.TimeZone,
				LinkCfg:       jobParams.LinkCfg,
				PsiCfg:        jobParams.PsiCfg,
				LogCfg:        jobParams.LogCfg,
			}

			execReq := &pb.RunExecutionPlanRequest{
				JobParams: partyJobParams,
				Graph:     graph,
				Async:     false, // Synchronous execution
			}

			// Call engine
			logrus.Infof("Calling engine for party %s at %s...", party, engineEndpoint)
			response, err := engineClient.RunExecutionPlan(
				engineEndpoint,
				"", // no credential
				execReq,
			)

			results <- executionResult{
				partyCode: party,
				response:  response,
				err:       err,
			}
		}(partyCode, subgraph)
	}

	// Wait for all results
	var issuerResult *pb.RunExecutionPlanResponse
	var firstError error

	for i := 0; i < len(compiledPlan.SubGraphs); i++ {
		result := <-results
		if result.err != nil {
			logrus.Errorf("Party %s execution failed: %v", result.partyCode, result.err)
			if firstError == nil {
				firstError = result.err
			}
			continue
		}

		// Check response status
		if result.response.Status.Code != int32(pb.Code_OK) {
			err := fmt.Errorf("party %s engine returned error: %s", result.partyCode, result.response.Status.Message)
			logrus.Error(err)
			if firstError == nil {
				firstError = err
			}
			continue
		}

		logrus.Infof("Party %s execution succeeded", result.partyCode)

		// Store the issuer's result (which contains the query output)
		if result.partyCode == conf.Issuer {
			issuerResult = result.response
		}
	}

	if firstError != nil {
		return nil, fmt.Errorf("execution failed: %w", firstError)
	}

	if issuerResult == nil {
		return nil, fmt.Errorf("no result from issuer party %s", conf.Issuer)
	}

	// Build query result from issuer's response
	queryResult := &pb.QueryResult{
		AffectedRows: issuerResult.NumRowsAffected,
		OutColumns:   issuerResult.OutColumns,
	}

	return queryResult, nil
}

func displayResults(result *pb.QueryResult) {
	// Print warnings if any
	for _, warn := range result.GetWarnings() {
		fmt.Printf("Warning: %v\n", warn.Reason)
	}

	// Print table result if there are output columns
	if len(result.GetOutColumns()) > 0 {
		fmt.Fprintf(os.Stdout, "[fetch]\n")
		// Create table writer
		table := tablewriter.NewWriter(os.Stdout)
		table.SetAutoWrapText(false)
		table.SetAutoFormatHeaders(false)

		// Convert result to table
		if err := tableview.ConvertToTable(result.GetOutColumns(), table); err != nil {
			logrus.Fatalf("[fetch] convertToTable with err: %v\n", err)
		}

		// Print summary and render table
		fmt.Printf("%v rows in set: (%vs)\n", table.NumLines(), result.GetCostTimeS())
		table.Render()
		return
	} else if result.GetAffectedRows() > 0 {
		// Print affected rows for non-SELECT queries
		fmt.Printf("[fetch] %v rows affected\n", result.GetAffectedRows())
		return
	}
}

func loadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	unmarshaler := protojson.UnmarshalOptions{
		DiscardUnknown: true,
		AllowPartial:   true,
	}

	// Unmarshal into a temporary map to extract protobuf fields
	var tmpMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &tmpMap); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	// Parse simple fields
	if val, ok := tmpMap["sql"]; ok {
		if err := json.Unmarshal(val, &config.SQL); err != nil {
			return nil, fmt.Errorf("failed to parse sql: %w", err)
		}
	}
	if val, ok := tmpMap["issuer"]; ok {
		if err := json.Unmarshal(val, &config.Issuer); err != nil {
			return nil, fmt.Errorf("failed to parse issuer: %w", err)
		}
	}
	if val, ok := tmpMap["engine_client_type"]; ok {
		if err := json.Unmarshal(val, &config.EngineClientType); err != nil {
			return nil, fmt.Errorf("failed to parse engine_client_type: %w", err)
		}
	}
	if val, ok := tmpMap["engine_timeout"]; ok {
		if err := json.Unmarshal(val, &config.EngineTimeout); err != nil {
			return nil, fmt.Errorf("failed to parse engine_timeout: %w", err)
		}
	}
	if val, ok := tmpMap["tls_ca_cert"]; ok {
		if err := json.Unmarshal(val, &config.TLSCACert); err != nil {
			return nil, fmt.Errorf("failed to parse tls_ca_cert: %w", err)
		}
	}
	if val, ok := tmpMap["engine_endpoints"]; ok {
		if err := json.Unmarshal(val, &config.EngineEndpoints); err != nil {
			return nil, fmt.Errorf("failed to parse engine_endpoints: %w", err)
		}
	}
	if val, ok := tmpMap["engine_link_endpoints"]; ok {
		if err := json.Unmarshal(val, &config.EngineLinkEndpoints); err != nil {
			return nil, fmt.Errorf("failed to parse engine_link_endpoints: %w", err)
		}
	}

	// Parse protobuf fields using protojson
	if val, ok := tmpMap["catalog"]; ok {
		config.Catalog = &pb.Catalog{}
		if err := unmarshaler.Unmarshal(val, config.Catalog); err != nil {
			return nil, fmt.Errorf("failed to parse catalog: %w", err)
		}
	}

	if val, ok := tmpMap["security_conf"]; ok {
		config.SecurityConf = &v1.CompilerSecurityConfig{}
		if err := unmarshaler.Unmarshal(val, config.SecurityConf); err != nil {
			return nil, fmt.Errorf("failed to parse security_conf: %w", err)
		}
	}

	if val, ok := tmpMap["compile_opts"]; ok {
		config.CompileOpts = &v1.CompileOptions{}
		if err := unmarshaler.Unmarshal(val, config.CompileOpts); err != nil {
			return nil, fmt.Errorf("failed to parse compile_opts: %w", err)
		}
	}

	return &config, nil
}

func printHelp() {
	fmt.Println(`opencore-demo - SCQL Compiler and Executor Tool

Usage:
  ./opencore-demo --config <config.json> --sql "SELECT * FROM my_table"(optional)

Description:
  This tool compiles SCQL queries and executes them on SCQL engines in a multi-party setup.

  The workflow:
  1. Compile SQL query to execution graph
  2. Distribute and execute the graph across multiple SCQL engines
  3. Display query results

Config File Format (JSON):
  {
    "sql": "SELECT * FROM my_table",
    "catalog": {
      "tables": [{
        "table_name": "my_table",
        "columns": [
          {"name": "id", "type": "string", "ordinal_position": 1},
          {"name": "value", "type": "int", "ordinal_position": 2}
        ],
        "ref_table": "alice.my_table",
        "db_type": "mysql",
        "owner": {"code": "alice"}
      }]
    },
    "security_conf": {
      "reverse_inference_conf": {
        "enable_reverse_inference": false
      }
    },
    "engine_endpoints": {
      "alice": "localhost:8003",
      "bob": "localhost:8005"
    },
    "engine_link_endpoints": {
      "alice": "localhost:8004",
      "bob": "localhost:8006"
    },
    "engine_client_type": "GRPC",
    "engine_timeout": 300,
    "tls_ca_cert": "/path/to/ca.crt",
    "issuer": "alice",
    "compile_opts": {
      "spu_conf": {
        "protocol": "SEMI2K",
        "field": "FM64"
      },
      "batched": false
    }
  }

  Field Details:
    - catalog: Database catalog metadata (see api/interpreter.proto for Catalog message)
    - security_conf: Security configuration (see api/v1alpha1/compiler.proto for CompilerSecurityConfig)
    - compile_opts: Compilation options (see api/v1alpha1/compiler.proto for CompileOptions)

Examples:
  ./opencore-demo --config example_config.json

Options:
  --config string
        Path to config JSON file (required)
  --sql string
        SQL query to execute, if provided will override the SQL in the config file (optional)
  --help
        Show this help message`)
}
