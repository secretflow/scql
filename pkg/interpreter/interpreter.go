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

package interpreter

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"slices"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/interpreter/graph/optimizer"
	"github.com/secretflow/scql/pkg/interpreter/translator"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
)

type Interpreter struct{}

func NewInterpreter() *Interpreter {
	return &Interpreter{}
}

func (intr *Interpreter) Compile(ctx context.Context, req *pb.CompileQueryRequest) (*pb.CompiledPlan, error) {
	p := parser.New()
	stmts, _, err := p.Parse(req.GetQuery(), "", "")
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("only support one query one time, but got %d queries", len(stmts))
	}

	is, err := buildInfoSchemaFromCatalogProto(req.GetCatalog())
	if err != nil {
		return nil, err
	}

	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &stmtctx.StatementContext{}
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	sctx.GetSessionVars().CurrentDB = req.GetDbName()

	lp, _, err := core.BuildLogicalPlanWithOptimization(ctx, sctx, stmts[0], is)
	if err != nil {
		return nil, err
	}
	var intoPartyCodes []string
	if lp.IntoOpt() != nil {
		for _, partyFile := range lp.IntoOpt().PartyFiles {
			if partyFile.PartyCode == "" {
				intoPartyCodes = append(intoPartyCodes, req.GetIssuer().GetCode())
			} else {
				intoPartyCodes = append(intoPartyCodes, partyFile.PartyCode)
			}
		}
	}
	enginesInfo, err := buildEngineInfo(lp, req.GetCatalog(), req.GetDbName(), req.GetIssuer().GetCode(), req.GetIssuerAsParticipant(), intoPartyCodes)
	if err != nil {
		return nil, err
	}
	plan, err := intr.compileCore(enginesInfo, req, lp, false)
	if err != nil {
		// Because streaming mode has not been fully implemented, if compiling fails, using non-streaming mode instead
		plan, err = intr.compileCore(enginesInfo, req, lp, true)
		if err != nil {
			return nil, err
		}
	}

	return plan, nil
}

func (*Interpreter) compileCore(enginesInfo *graph.EnginesInfo, req *pb.CompileQueryRequest, lp core.LogicalPlan, forceToUnBatched bool) (*pb.CompiledPlan, error) {
	t, err := translator.NewTranslator(enginesInfo, req.GetSecurityConf(), req.GetIssuer().GetCode(), req.GetCompileOpts(), req.CreatedAt.AsTime())
	if err != nil {
		return nil, err
	}
	if forceToUnBatched {
		t.CompileOpts.Batched = false
	}
	ep, err := t.Translate(lp)
	if err != nil {
		return nil, err
	}

	graphOptimizer := graph.NewGraphOptimizer()
	if err := graphOptimizer.Optimize(ep); err != nil {
		return nil, err
	}

	graphChecker := graph.NewGraphChecker()
	if err := graphChecker.Check(ep); err != nil {
		return nil, err
	}

	partitioner := optimizer.NewGraphPartitioner(ep)
	if err := partitioner.NaivePartition(); err != nil {
		return nil, err
	}

	mapper := optimizer.NewGraphMapper(ep, partitioner.Pipelines)
	mapper.Map()

	graphvizOutput := ep.DumpGraphviz()
	plan := buildCompiledPlan(req.GetCompileOpts().GetSpuConf(), ep, mapper.Codes)
	if req.GetCompileOpts().GetDumpExeGraph() {
		plan.Explain = &pb.ExplainInfo{
			ExeGraphDot: graphvizOutput,
		}
	}
	// calculate whole graph checksum
	tableSchemaCrypt := sha256.New()
	tableSchemaCrypt.Write([]byte(graphvizOutput))
	plan.WholeGraphChecksum = fmt.Sprintf("%x", tableSchemaCrypt.Sum(nil))

	plan.Warning = &pb.Warning{
		MayAffectedByGroupThreshold: t.AffectedByGroupThreshold,
	}
	return plan, nil
}

func buildCompiledPlan(spuConf *spu.RuntimeConfig, eGraph *graph.Graph, execPlans map[string]*optimizer.ExecutionPlan) *pb.CompiledPlan {
	plan := &pb.CompiledPlan{
		Schema:         &pb.TableSchema{},
		SpuRuntimeConf: spuConf,
		SubGraphs:      make(map[string]*pb.SubGraph),
	}

	{
		// Fill Schema
		for _, out := range eGraph.OutputNames {
			plan.Schema.Columns = append(plan.Schema.Columns, &pb.ColumnDesc{
				Name: out,
				// TODO: populate Field Type
				// Type: <column data type>
			})
		}
		// Fill Parties
		for _, party := range eGraph.GetParties() {
			plan.Parties = append(plan.Parties, &pb.PartyId{
				Code: party,
			})
		}

		// Fill Subgraphs
		for party, subGraph := range execPlans {
			graphProto := &pb.SubGraph{
				Nodes: make(map[string]*pb.ExecNode),
				Policy: &pb.SchedulingPolicy{
					WorkerNum: int32(subGraph.Policy.WorkerNumber),
				},
			}
			// Fill Nodes
			for k, v := range subGraph.Nodes {
				graphProto.Nodes[strconv.Itoa(k)] = v.ToProto()
			}
			// Fill Policy subdags
			for _, pipelineJobs := range subGraph.Policy.PipelineJobs {
				pipeline := &pb.Pipeline{}
				for _, job := range pipelineJobs.Jobs {
					subdag := &pb.SubDAG{
						Jobs:                     make([]*pb.SubDAG_Job, 0),
						NeedCallBarrierAfterJobs: job.NeedCallBarrierAfterJobs,
					}
					for k, v := range job.Jobs {
						var ids []string
						for _, id := range v {
							ids = append(ids, strconv.Itoa(id))
						}

						j := &pb.SubDAG_Job{
							WorkerId: int32(k),
							NodeIds:  ids,
						}
						subdag.Jobs = append(subdag.Jobs, j)
					}
					pipeline.Subdags = append(pipeline.Subdags, subdag)
				}
				if pipelineJobs.Batched {
					pipeline.Batched = true
					for _, t := range pipelineJobs.InputTensors {
						pipeline.Inputs = append(pipeline.Inputs, t.ToProto())
					}
					for _, t := range pipelineJobs.OutputTensors {
						pipeline.Outputs = append(pipeline.Outputs, t.ToProto())
					}
				}
				graphProto.Policy.Pipelines = append(graphProto.Policy.Pipelines, pipeline)
			}
			// calculate checksum for each party
			tableSchemaCrypt := sha256.New()
			subGraphStr := graphProto.String()
			logrus.Infof("subgraph of %s: %s", party, subGraphStr)
			// write graph
			tableSchemaCrypt.Write([]byte(subGraphStr))
			graphProto.SubGraphChecksum = fmt.Sprintf("%x", tableSchemaCrypt.Sum(nil))
			plan.SubGraphs[party] = graphProto
		}
	}
	return plan
}

func buildInfoSchemaFromCatalogProto(catalog *pb.Catalog) (infoschema.InfoSchema, error) {
	tblInfoMap := make(map[string][]*model.TableInfo)
	for i, tblEntry := range catalog.GetTables() {
		dbTable, err := core.NewDbTableFromString(tblEntry.GetTableName())
		if err != nil {
			return nil, err
		}
		tblInfo := &model.TableInfo{
			ID:          int64(i),
			TableId:     fmt.Sprint(i),
			Name:        model.NewCIStr(dbTable.GetTableName()),
			Columns:     []*model.ColumnInfo{},
			Indices:     []*model.IndexInfo{},
			ForeignKeys: []*model.FKInfo{},
			State:       model.StatePublic,
			PKIsHandle:  false,
		}

		if tblEntry.GetIsView() {
			tblInfo.View = &model.ViewInfo{
				Algorithm:  model.AlgorithmMerge,
				SelectStmt: tblEntry.SelectString,
			}
		}
		// sort columns by ordinal position
		sort.Slice(tblEntry.Columns, func(i, j int) bool {
			return tblEntry.Columns[i].OrdinalPosition < tblEntry.Columns[j].OrdinalPosition
		})

		for idx, col := range tblEntry.GetColumns() {
			colTp := strings.ToLower(col.GetType())
			defaultVal, err := infoschema.TypeDefaultValue(colTp)
			if err != nil {
				return nil, err
			}
			fieldTp, err := infoschema.TypeConversion(colTp)
			if err != nil {
				return nil, err
			}
			colInfo := &model.ColumnInfo{
				ID:                 int64(idx),
				Name:               model.NewCIStr(col.GetName()),
				Offset:             idx,
				OriginDefaultValue: defaultVal,
				DefaultValue:       defaultVal,
				DefaultValueBit:    []byte{},
				Dependences:        map[string]struct{}{},
				FieldType:          fieldTp,
				State:              model.StatePublic,
			}
			tblInfo.Columns = append(tblInfo.Columns, colInfo)
		}
		tblInfoMap[dbTable.GetDbName()] = append(tblInfoMap[dbTable.GetDbName()], tblInfo)
	}
	return infoschema.MockInfoSchema(tblInfoMap), nil
}

func collectDataSourceNode(lp core.LogicalPlan) []*core.DataSource {
	var result []*core.DataSource
	for _, child := range lp.Children() {
		dsList := collectDataSourceNode(child)
		result = append(result, dsList...)
	}

	if len(lp.Children()) > 0 {
		return result
	}

	if ds, ok := lp.(*core.DataSource); ok {
		return []*core.DataSource{ds}
	}
	return nil
}

func buildEngineInfo(lp core.LogicalPlan, catalog *pb.Catalog, currentDb string, queryIssuer string, issuerAsParticipant bool, intoPartyCodes []string) (*graph.EnginesInfo, error) {
	// construct catalog map
	catalogMap := make(map[string]*pb.TableEntry)
	for _, table := range catalog.GetTables() {
		tn := table.GetTableName()
		if _, exists := catalogMap[tn]; exists {
			return nil, fmt.Errorf("duplicate table exists in catalog")
		}
		catalogMap[tn] = table
	}

	party2Tables := make(map[string][]core.DbTable)
	tableToRefs := make(map[core.DbTable]core.DbTable)

	dsList := collectDataSourceNode(lp)
	if len(dsList) == 0 {
		return nil, fmt.Errorf("no data source in query")
	}

	// NOTE: no view include in dsList
	for _, ds := range dsList {
		dbName := ds.DBName.String()
		tblName := ds.TableInfo().Name.String()

		if len(dbName) == 0 {
			dbName = currentDb
		}
		dbTable := core.NewDbTable(dbName, tblName)
		tn := dbTable.String()

		tblEntry, exists := catalogMap[tn]
		if !exists {
			return nil, fmt.Errorf("table `%s` not found in catalog", tn)
		}

		tblOwner := tblEntry.GetOwner().GetCode()
		party2Tables[tblOwner] = append(party2Tables[tblOwner], dbTable)

		refTblName := tblEntry.GetRefTable()
		// Note: ref table name empty means it is the same with itself
		if len(refTblName) == 0 {
			refTblName = tblEntry.GetTableName()
		}
		refDbTable, err := core.NewDbTableFromString(refTblName)
		if err != nil {
			return nil, fmt.Errorf("failed to create DbTable from %s: %+v", tblEntry.GetRefTable(), err)
		}

		err = refDbTable.SetDBTypeFromString(tblEntry.GetDbType(), tblEntry.TableName)
		if err != nil {
			return nil, fmt.Errorf("buildEngineInfo: %v", err)
		}

		tableToRefs[dbTable] = refDbTable
	}

	parties := make([]*graph.Participant, 0)
	for party := range party2Tables {
		parties = append(parties, &graph.Participant{
			PartyCode: party,
			// NOTE: For translator, other information (endpoint, token, pubkey...) is not important
			// TODO: remove unneeded fields
		})
	}

	if _, exists := party2Tables[queryIssuer]; !exists {
		if issuerAsParticipant {
			parties = append(parties, &graph.Participant{
				PartyCode: queryIssuer,
			})
		}
	}

	if len(intoPartyCodes) > 0 {
		for _, partyCode := range intoPartyCodes {
			parties = append(parties, &graph.Participant{
				PartyCode: partyCode,
			})
		}
	}

	// sort parties by party code for deterministic in p2p
	sort.Slice(parties, func(i, j int) bool {
		return parties[i].PartyCode < parties[j].PartyCode
	})
	parties = slices.CompactFunc(parties, func(i, j *graph.Participant) bool {
		return i.PartyCode == j.PartyCode
	})

	partyInfo := graph.NewPartyInfo(parties)

	engineInfo := graph.NewEnginesInfo(partyInfo, party2Tables)
	engineInfo.UpdateTableToRefs(tableToRefs)

	return engineInfo, nil
}
