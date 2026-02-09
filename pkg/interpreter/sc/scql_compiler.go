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

package sc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/secretflow/scql/pkg/interpreter/compiler"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	v1 "github.com/secretflow/scql/pkg/proto-gen/scql/v1alpha1"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
	"github.com/secretflow/scql/pkg/util/message"
)

type ColumnSlice []Column

type CompileResult struct {
	OperatorGraph string
	MarshaledPlan []byte
}

type Catalog struct {
	TableEntries []*TableEntry
}

func (catalog *Catalog) AddTableEntry(table *TableEntry) {
	catalog.TableEntries = append(catalog.TableEntries, table)
}

type TableEntry struct {
	Name        string
	Db          string
	DbType      string
	Owner       string
	RefTable    string
	RefTableUri string
	Columns     []*Column
}

type Column struct {
	Name string
	Type string
}

type CompileInputs struct {
	Query   string
	Issuer  string
	Db      string // TODO: remove this field
	Catalog *Catalog
}

func (te *TableEntry) AddColumn(col *Column) {
	te.Columns = append(te.Columns, col)
}

func Compile(inputs *CompileInputs) (CompileResult, error) {
	compileResult := CompileResult{}

	req, err := createCompileRequest(inputs)
	if err != nil {
		return compileResult, err
	}

	goResult, err := compiler.DetailedCompile(context.Background(), req)
	if err != nil {
		return compileResult, err
	}

	compileResult.OperatorGraph = goResult.OperatorGraph.String()

	compileResult.MarshaledPlan, err = message.ProtoMarshal(goResult.ExecutionPlan)
	if err != nil {
		return compileResult, err
	}

	return compileResult, nil
}

func createCompileRequest(inputs *CompileInputs) (*v1.CompileSQLRequest, error) {
	return &v1.CompileSQLRequest{
		Query: inputs.Query,
		Db:    inputs.Db,
		Issuer: &pb.PartyId{
			Code: inputs.Issuer,
		},
		Catalog:        createCatalog(inputs.Catalog),
		CompileOpts:    crateCompileOptions(),
		IssueTime:      timestamppb.New(time.Now()),
		SecurityConfig: createSecurityConfig(),
		AdditionalInfo: &v1.AdditionalInfoSpec{
			NeedOperatorGraph: true,
		},
	}, nil
}

func createCatalog(catalog *Catalog) *pb.Catalog {
	catalogPb := &pb.Catalog{}

	for _, table := range catalog.TableEntries {
		tableEntry := &pb.TableEntry{
			TableName:   fmt.Sprintf("%s.%s", table.Db, table.Name),
			RefTable:    table.RefTable,
			RefTableUri: table.RefTableUri,
			DbType:      table.DbType,
			Owner:       &pb.PartyId{Code: table.Owner},
		}
		for _, column := range table.Columns {
			col := &pb.TableEntry_Column{
				Name: column.Name,
				Type: column.Type,
			}
			tableEntry.Columns = append(tableEntry.Columns, col)
		}
		catalogPb.Tables = append(catalogPb.Tables, tableEntry)
	}

	return catalogPb
}

func crateCompileOptions() *v1.CompileOptions {
	return &v1.CompileOptions{
		SpuConf: &spu.RuntimeConfig{
			Protocol:                                spu.ProtocolKind_SEMI2K,
			Field:                                   spu.FieldType_FM128,
			ExperimentalEnableColocatedOptimization: true,
		},
		Batched:          false,
		PsiAlgorithmType: pb.PsiAlgorithmType_AUTO,
	}
}

func createSecurityConfig() *v1.CompilerSecurityConfig {
	config := &v1.CompilerSecurityConfig{
		GlobalRelaxation: &v1.GlobalSecurityRelaxation{
			RevealGroupCount:   true,
			RevealGroupMark:    true,
			RevealKeyAfterJoin: true,
			RevealFilterMask:   true,
		},
		ColumnRelaxationList: nil,
		ReverseInferenceConf: &v1.ReverseInferenceConfig{
			EnableReverseInference: true,
		},
	}

	return config
}
