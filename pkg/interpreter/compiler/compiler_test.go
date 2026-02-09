// Copyright 2025 Ant Group Co., Ltd.
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

package compiler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	v1 "github.com/secretflow/scql/pkg/proto-gen/scql/v1alpha1"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
	"github.com/secretflow/scql/pkg/util/mock"
)

func TestCompiler(t *testing.T) {
	r := require.New(t)

	file, err := os.Open("data/test_queries.json")
	r.NoError(err)
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	r.NoError(err)
	var queryCases []QueryCase
	err = json.Unmarshal(byteValue, &queryCases)
	r.NoError(err)

	for _, queryCase := range queryCases {
		t.Run(queryCase.Name, func(t *testing.T) {
			r := require.New(t)

			req, err := createCompileRequest(queryCase.Query)
			r.NoError(err)

			executionPlan, err := Compile(context.Background(), req)
			r.NoError(err)
			r.NotNil(executionPlan)
		})
	}
}

func createCompileRequest(query string) (*v1.CompileSQLRequest, error) {
	return createCompileRequestWithConf(query, testConf{groupThreshold: 4})
}

func createCompileRequestWithConf(query string, conf testConf) (*v1.CompileSQLRequest, error) {
	catalog, err := mock.MockCatalog()
	if err != nil {
		return nil, err
	}

	securityConfig, err := getTestSecurityConfig()
	if err != nil {
		return nil, err
	}

	// Update ResultSecurityConf based on testConf
	if securityConfig.ResultSecurityConf != nil {
		securityConfig.ResultSecurityConf.GroupbyThreshold = int64(conf.groupThreshold)
	}

	compileOpts := getTestCompileOptions()
	compileOpts.Batched = conf.batched

	return &v1.CompileSQLRequest{
		Query: query,
		Db:    "test",
		Issuer: &pb.PartyId{
			Code: "alice",
		},
		Catalog:        catalog,
		CompileOpts:    compileOpts,
		IssueTime:      timestamppb.New(time.Now()),
		SecurityConfig: securityConfig,
		AdditionalInfo: &v1.AdditionalInfoSpec{
			NeedOperatorGraph: true,
		},
	}, nil
}

// getTestCompileOptions returns standard compile options for testing
func getTestCompileOptions() *v1.CompileOptions {
	return &v1.CompileOptions{
		SpuConf: &spu.RuntimeConfig{
			Protocol: spu.ProtocolKind_SEMI2K,
			Field:    spu.FieldType_FM128,
		},
		Batched:          false,
		PsiAlgorithmType: pb.PsiAlgorithmType_AUTO,
	}
}

func getTestSecurityConfig() (*v1.CompilerSecurityConfig, error) {
	config := &v1.CompilerSecurityConfig{
		GlobalRelaxation: &v1.GlobalSecurityRelaxation{
			RevealGroupCount:   false,
			RevealGroupMark:    false,
			RevealKeyAfterJoin: true,
			RevealFilterMask:   true,
		},
		ColumnRelaxationList: nil,
		ReverseInferenceConf: &v1.ReverseInferenceConfig{
			EnableReverseInference: true,
		},
		ResultSecurityConf: &v1.ResultSecurityConfig{
			GroupbyThreshold: 4,
		},
	}

	// TODO: only include used columns
	columnVisibilityList, err := mock.MockColumnVisibility()
	if err != nil {
		return nil, err
	}
	config.ColumnVisibilityList = columnVisibilityList

	return config, nil
}

func TestCompileToExecutionGraph(t *testing.T) {
	r := require.New(t)

	// Test cases can be added here
	testCases := []sPair{}
	// Test with the new generated test cases
	testCases = append(testCases, executionGraphTestCases...)

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			// TODO: remove skipping logic when test cases are ready
			// Skip empty test cases that were generated due to compilation errors
			if tc.dotGraph == "" {
				t.Skip("Skipping test case with empty graph")
				return
			}

			req, err := createCompileRequestWithConf(tc.sql, tc.conf)
			r.NoError(err)

			// Compile to ExecutionGraph
			executionGraph, err := CompileToExecutionGraph(context.Background(), req)
			r.NoError(err)
			r.NotNil(executionGraph)

			// Get graph visualization
			graphStr := executionGraph.DumpGraphviz()

			// Get pipeline info if batched
			actualPipe := ""
			if tc.conf.batched {
				for _, pipeline := range executionGraph.Pipelines {
					if pipeline.Batched {
						actualPipe = fmt.Sprintf("Batched pipeline with %d nodes", len(pipeline.Nodes))
					}
				}
			} else {
				r.Len(executionGraph.Pipelines, 1, "for non-batched execution, should have 1 pipeline")
				r.False(executionGraph.Pipelines[0].Batched, "pipeline should not be batched")
				actualPipe = executionGraph.DumpBriefPipeline()
			}

			// Verify basic properties
			r.Greater(executionGraph.NodeCnt, 0)
			r.NotEmpty(executionGraph.Pipelines)
			if executionGraph.PartyInfo != nil {
				r.NotEmpty(executionGraph.PartyInfo.GetParties())
			}

			// Verify graph structure
			for _, pipeline := range executionGraph.Pipelines {
				r.NotNil(pipeline)
				r.NotNil(pipeline.Nodes)
			}

			// Compare with expected
			r.Equal(tc.dotGraph, graphStr, "graph should match expected")
			if !tc.conf.batched {
				r.Equal(tc.briefPipeline, actualPipe, "pipeline info should match expected")
			}
		})
	}
}

func TestCompilerPlayground(t *testing.T) {
	sql := `select count(plain_int_0) from (select plain_int_0, groupby_int_0 from alice.tbl_0 union all select plain_int_0, groupby_int_0 from bob.tbl_1) as u;`

	r := require.New(t)

	req, err := createCompileRequest(sql)
	r.NoError(err)

	compilationDetails, err := DetailedCompile(context.Background(), req)
	r.NoError(err)

	fmt.Println(compilationDetails)
}
