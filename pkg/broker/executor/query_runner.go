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

package executor

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/constant"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/executor"
	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/interpreter"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/planner/util"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

type QueryRunner struct {
	session *application.Session
	// running info
	info   *graph.EnginesInfo
	is     infoschema.InfoSchema
	ccls   []*pb.SecurityConfig_ColumnControl
	tables []storage.TableMeta
	// need update info/is/ccls
	prepareAgain bool
}

func NewQueryRunner(session *application.Session) *QueryRunner {
	return &QueryRunner{
		session: session,
	}
}

func (r *QueryRunner) Clear() {
	r.info = nil
	r.is = nil
	r.ccls = nil
}

func (r *QueryRunner) GetEnginesInfo() *graph.EnginesInfo {
	return r.info
}

func (r *QueryRunner) SetPrepareAgain() {
	r.prepareAgain = true
}

func (r *QueryRunner) CreateChecksum() (map[string]application.Checksum, error) {
	s := r.session
	checksumMap := make(map[string]application.Checksum)
	for _, p := range s.ExecuteInfo.DataParties {
		hasher := application.NewChecksumHasher()
		tables := r.info.GetTablesByParty(p)
		sort.Slice(tables, func(i, j int) bool {
			return tables[i].String() < tables[j].String()
		})
		for _, t := range tables {
			tableSchema, err := r.is.TableByName(model.NewCIStr(t.GetDbName()), model.NewCIStr(t.GetTableName()))
			if err != nil {
				return nil, err
			}
			columnInfos := tableSchema.Meta().Columns
			hasher.InfeedTable(t.String(), columnInfos)

			var cclsForP []*pb.SecurityConfig_ColumnControl
			for _, ccl := range r.ccls {
				if ccl.TableName == t.GetTableName() && ccl.DatabaseName == t.GetDbName() {
					cclsForP = append(cclsForP, ccl)
				}
			}
			hasher.InfeedCCLs(cclsForP)
		}
		checksumMap[p] = hasher.Finalize()
	}
	return checksumMap, nil
}

func (r *QueryRunner) prepareData(usedTableNames, intoParties []string) (dataParties []string, workParties []string, err error) {
	session := r.session
	txn := session.App.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()
	var notFoundTables []string
	r.tables, notFoundTables, err = txn.GetTableMetasByTableNames(session.ExecuteInfo.ProjectID, usedTableNames)
	if err != nil {
		return
	}
	if len(notFoundTables) > 0 && !r.prepareAgain {
		var members []string
		members, err = txn.GetProjectMembers(session.ExecuteInfo.ProjectID)
		if err != nil {
			return
		}
		// finish old transaction
		err = txn.Finish(nil)
		if err != nil {
			logrus.Errorf("prepareData: finish old transaction err: %s", err)
		}
		err = common.AskProjectInfoFromParties(session.App, session.ExecuteInfo.ProjectID, notFoundTables, []string{}, sliceutil.Subtraction(members, []string{session.App.Conf.PartyCode}))
		if err != nil {
			logrus.Warningf("prepareData: get not found tables %+v err: %s", notFoundTables, err)
		}
		// use new transaction
		txn = session.App.MetaMgr.CreateMetaTransaction()
		// get tables schema again
		r.tables, notFoundTables, err = txn.GetTableMetasByTableNames(session.ExecuteInfo.ProjectID, usedTableNames)
		if err != nil {
			return
		}
		if len(notFoundTables) > 0 {
			return nil, nil, fmt.Errorf("prepareData: table %+v not found", notFoundTables)
		}
	}

	r.tables, err = common.ExpandTablesInView(txn, session.ExecuteInfo.ProjectID, r.tables)
	if err != nil {
		return nil, nil, fmt.Errorf("prepareData: expand tables in view err: %s", err)
	}

	var parties []string
	party2Tables := make(map[string][]core.DbTable)
	tableToRefs := make(map[core.DbTable]core.DbTable)
	for _, t := range r.tables {
		parties = append(parties, t.Table.Owner)
		if _, exist := party2Tables[t.Table.Owner]; !exist {
			party2Tables[t.Table.Owner] = []core.DbTable{}
		}
		// currently use project id as db name
		dbTable := core.NewDbTable(t.Table.ProjectID, t.Table.TableName)
		party2Tables[t.Table.Owner] = append(party2Tables[t.Table.Owner], dbTable)
		var refDbTable core.DbTable
		refDbTable, err = core.NewDbTableFromString(t.Table.RefTable)
		if err != nil {
			return
		}

		err = refDbTable.SetDBTypeFromString(t.Table.DBType, t.Table.TableName)
		if err != nil {
			return
		}

		tableToRefs[dbTable] = refDbTable
	}
	// SliceDeDup sort parties and compact
	dataParties = sliceutil.SliceDeDup(parties)
	workParties = append(dataParties, session.ExecuteInfo.Issuer.Code)
	workParties = sliceutil.SliceDeDup(append(workParties, intoParties...))
	partyInfo, err := session.App.PartyMgr.GetPartyInfoByParties(workParties)
	if err != nil {
		return
	}
	r.info = graph.NewEnginesInfo(partyInfo, party2Tables)
	r.info.UpdateTableToRefs(tableToRefs)
	// get ccls
	var actualUsedTableNames []string
	for _, t := range r.tables {
		actualUsedTableNames = append(actualUsedTableNames, t.Table.TableName)
	}
	columnPrivs, err := txn.ListColumnConstraints(session.ExecuteInfo.ProjectID, actualUsedTableNames, workParties)
	r.ccls = storage.ColumnPrivs2ColumnControls(columnPrivs)
	return
}

func (r *QueryRunner) Prepare(usedTables []core.DbTable) (dataParties []string, workParties []string, err error) {
	// clear before preparing
	r.Clear()
	// get data from storage
	var usedTableNames []string
	for _, t := range usedTables {
		usedTableNames = append(usedTableNames, t.GetTableName())
	}
	intoParties, err := util.CollectIntoParties(r.session.ExecuteInfo.Query)
	if err != nil {
		return nil, nil, fmt.Errorf("collect select into parties err: %s", err)
	}
	if err := r.checkWritePrivilege(intoParties); err != nil {
		return nil, nil, fmt.Errorf("Prepare: check write privilege err: %s", err)
	}

	dataParties, workParties, err = r.prepareData(usedTableNames, intoParties)
	if err != nil {
		return
	}
	// create info schema
	r.is, err = r.CreateInfoSchema(r.tables)
	if err != nil {
		return
	}
	return
}

func (r *QueryRunner) CreateInfoSchema(tables []storage.TableMeta) (result infoschema.InfoSchema, err error) {
	s := r.session
	info := make(map[string][]*model.TableInfo)
	var tableInfos []*model.TableInfo
	for i, tbl := range tables {
		tblInfo := &model.TableInfo{
			ID:          int64(i),
			TableId:     fmt.Sprint(i),
			Name:        model.NewCIStr(tbl.Table.TableName),
			Columns:     []*model.ColumnInfo{},
			Indices:     []*model.IndexInfo{},
			ForeignKeys: []*model.FKInfo{},
			State:       model.StatePublic,
			PKIsHandle:  false,
		}
		if tbl.Table.IsView {
			tblInfo.View = &model.ViewInfo{
				Algorithm:  model.AlgorithmMerge,
				SelectStmt: tbl.Table.SelectString,
			}
		}

		for i, col := range tbl.Columns {
			colTyp := strings.ToLower(col.DType)
			defaultVal, err := infoschema.TypeDefaultValue(colTyp)
			if err != nil {
				return nil, err
			}
			fieldTp, err := infoschema.TypeConversion(colTyp)
			if err != nil {
				return nil, err
			}
			colInfo := &model.ColumnInfo{
				ID:                 int64(i),
				Name:               model.NewCIStr(col.ColumnName),
				Offset:             i,
				OriginDefaultValue: defaultVal,
				DefaultValue:       defaultVal,
				DefaultValueBit:    []byte{},
				Dependences:        map[string]struct{}{},
				FieldType:          fieldTp,
				State:              model.StatePublic,
			}
			tblInfo.Columns = append(tblInfo.Columns, colInfo)
		}
		tableInfos = append(tableInfos, tblInfo)
	}
	info[s.ExecuteInfo.ProjectID] = tableInfos
	return infoschema.MockInfoSchema(info), nil
}

func (r *QueryRunner) buildCompileQueryRequest() *pb.CompileQueryRequest {
	s := r.session
	catalog := buildCatalog(r.tables)
	req := &pb.CompileQueryRequest{
		Query:  s.ExecuteInfo.Query,
		DbName: s.ExecuteInfo.ProjectID,
		Issuer: s.ExecuteInfo.Issuer,
		// In p2p, `IssuerAsParticipant` is always true.
		IssuerAsParticipant: true,
		SecurityConf: &pb.SecurityConfig{
			ColumnControlList: r.ccls,
		},
		Catalog:     catalog,
		CompileOpts: s.ExecuteInfo.CompileOpts,
		CreatedAt:   timestamppb.New(s.ExecuteInfo.CreatedAt),
	}
	return req
}

func buildCatalog(tables []storage.TableMeta) *pb.Catalog {
	catalog := &pb.Catalog{}
	for _, tbl := range tables {
		tblEntry := &pb.TableEntry{
			TableName: fmt.Sprintf("%s.%s", tbl.Table.ProjectID, tbl.Table.TableName),
			IsView:    tbl.Table.IsView,
			RefTable:  tbl.Table.RefTable,
			DbType:    tbl.Table.DBType,
			Owner: &pb.PartyId{
				Code: tbl.Table.Owner,
			},
			SelectString: tbl.Table.SelectString,
		}
		for _, col := range tbl.Columns {
			colEntry := &pb.TableEntry_Column{
				Name: col.ColumnName,
				Type: col.DType,
				// TODO: populate OrdinalPosition
				// OrdinalPosition: <pos>,
			}
			tblEntry.Columns = append(tblEntry.Columns, colEntry)
		}
		catalog.Tables = append(catalog.Tables, tblEntry)
	}
	return catalog
}

func (r *QueryRunner) CreateExecutor(plan *pb.CompiledPlan) (*executor.Executor, error) {
	// create JobStartParams
	session := r.session
	conf := session.App.Conf
	linkCfg := session.ExecuteInfo.SessionOptions.LinkConfig
	psiCfg := session.ExecuteInfo.SessionOptions.PsiConfig
	logCfg := session.ExecuteInfo.SessionOptions.LogConfig

	startParams := &pb.JobStartParams{
		PartyCode:     conf.PartyCode,
		JobId:         session.ExecuteInfo.JobID,
		SpuRuntimeCfg: plan.GetSpuRuntimeConf(),
		LinkCfg:       linkCfg,
		PsiCfg:        psiCfg,
		LogCfg:        logCfg,
		TimeZone:      session.ExecuteInfo.SessionOptions.TimeZone,
	}
	partyToRank := make(map[string]string)
	for i, p := range plan.Parties {
		endpoint, err := session.GetEndpoint(p.GetCode())
		if err != nil {
			return nil, err
		}
		pubKey, err := session.App.PartyMgr.GetPubKeyByParty(p.GetCode())
		if err != nil {
			return nil, err
		}
		startParams.Parties = append(startParams.Parties, &pb.JobStartParams_Party{
			Code:      p.GetCode(),
			Name:      p.GetCode(),
			Rank:      int32(i),
			Host:      endpoint,
			PublicKey: pubKey,
		})
		partyToRank[p.GetCode()] = strconv.Itoa(i)
	}

	myGraph, exists := plan.GetSubGraphs()[conf.PartyCode]
	if !exists {
		return nil, fmt.Errorf("could not find my graph")
	}

	graphChecksums := make(map[string]string)
	for code, graph := range plan.GetSubGraphs() {
		graphChecksums[partyToRank[code]] = graph.SubGraphChecksum
	}

	req := &pb.RunExecutionPlanRequest{
		JobParams:     startParams,
		Graph:         myGraph,
		Async:         false,
		DebugOpts:     session.ExecuteInfo.DebugOpts,
		GraphChecksum: &pb.GraphChecksum{CheckGraphChecksum: true, WholeGraphChecksum: plan.WholeGraphChecksum, SubGraphChecksums: graphChecksums},
	}

	planReqs := map[string]*pb.RunExecutionPlanRequest{
		conf.PartyCode: req,
	}

	// create sync executor
	myPubKey, err := session.App.PartyMgr.GetPubKeyByParty(conf.PartyCode)
	if err != nil {
		return nil, err
	}

	myself := &graph.Participant{
		PartyCode: conf.PartyCode,
		Endpoints: []string{session.Engine.GetEndpointForSelf()},
		PubKey:    myPubKey,
	}

	engineStub := executor.NewEngineStub(
		session.ExecuteInfo.JobID,
		conf.IntraServer.Protocol,
		session.CallBackHost,
		constant.EngineCallbackPath,
		session.ExecuteInfo.EngineClient,
	)

	// p2p: party code who is not issuer doesn't have output tensors
	var outputNames []string
	if session.IsIssuer() {
		for _, col := range plan.GetSchema().GetColumns() {
			outputNames = append(outputNames, col.GetName())
		}
	}

	return executor.NewExecutor(planReqs, outputNames, engineStub, r.session.ExecuteInfo.JobID, graph.NewPartyInfo([]*graph.Participant{myself}))
}

func (r *QueryRunner) Execute(usedTables []core.DbTable) (syncResult *pb.QueryResponse, err error) {
	s := r.session
	if r.prepareAgain {
		logrus.Infof("ask info has been triggered, get data from storage again")
		_, _, err := r.Prepare(usedTables)
		if err != nil {
			return nil, err
		}
		localChecksums, err := r.CreateChecksum()
		if err != nil {
			return nil, err
		}
		for code, checksum := range localChecksums {
			err = s.SaveLocalChecksum(code, checksum)
			if err != nil {
				return nil, fmt.Errorf("failed to save local checksum: %v", err)
			}
		}
		// check checksum again
		if err := s.CheckChecksum(); err != nil {
			return nil, err
		}
	}

	compileReq := r.buildCompileQueryRequest()
	intrpr := interpreter.NewInterpreter()
	compiledPlan, err := intrpr.Compile(context.Background(), compileReq)
	if err != nil {
		return nil, fmt.Errorf("failed to compile query to plan: %w", err)
	}

	logrus.Infof("Execution Plan:\n%s\n", compiledPlan.GetExplain().GetExeGraphDot())

	executor, err := r.CreateExecutor(compiledPlan)
	if err != nil {
		return nil, err
	}
	s.OutputNames = executor.OutputNames
	s.Warning = compiledPlan.Warning
	err = s.App.UpdateSession(s)
	if err != nil {
		return nil, err
	}
	// TODO: sync err to issuer
	ret, err := executor.RunExecutionPlan(s.Ctx, s.AsyncMode)
	if err != nil {
		return nil, err
	}
	if ret.GetStatus().GetCode() != 0 {
		return nil, fmt.Errorf("status: %s", ret)
	}
	if s.AsyncMode {
		// Only change the session status to running if it is submitted.
		// The jobwatcher only monitors jobs in the running state.
		err = s.App.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
			return txn.UpdateSessionInfoStatusWithCondition(s.ExecuteInfo.JobID, storage.SessionSubmitted, storage.SessionRunning)
		})
		if err != nil {
			return nil, fmt.Errorf("updated session info after run execution plan err: %v", err)
		}
		return nil, nil
	}

	result := &pb.QueryResponse{
		Status: ret.Status,
		Result: &pb.QueryResult{
			OutColumns:   ret.GetOutColumns(),
			AffectedRows: ret.GetAffectedRows(),
			CostTimeS:    time.Since(s.CreatedAt).Seconds(),
		},
	}
	if compiledPlan.Warning.MayAffectedByGroupThreshold {
		reason := fmt.Sprintf("for safety, we filter the results for groups which contain less than %d items.", compileReq.CompileOpts.SecurityCompromise.GroupByThreshold)
		logrus.Infof("%v", reason)
		result.Result.Warnings = append(result.Result.Warnings, &pb.SQLWarning{Reason: reason})
	}

	return result, nil
}

func (r *QueryRunner) DryRun(usedTables []core.DbTable) error {
	// 1. check data consistency
	if err := r.session.CheckChecksum(); err != nil {
		return err
	}
	// 2. try compile query
	compileReq := r.buildCompileQueryRequest()
	intrpr := interpreter.NewInterpreter()
	_, err := intrpr.Compile(context.TODO(), compileReq)
	if err != nil {
		return fmt.Errorf("failed to compile query: %w", err)
	}
	return nil
}

func (r *QueryRunner) GetPlan(usedTables []core.DbTable) (*pb.CompiledPlan, error) {
	// 1. check data consistency
	if err := r.session.CheckChecksum(); err != nil {
		return nil, err
	}
	// 2. try compile query
	compileReq := r.buildCompileQueryRequest()
	intrpr := interpreter.NewInterpreter()
	plan, err := intrpr.Compile(context.TODO(), compileReq)
	if err != nil {
		return nil, fmt.Errorf("failed to compile query: %w", err)
	}
	return plan, nil
}

func (r *QueryRunner) checkWritePrivilege(intoParties []string) error {
	if !slices.Contains(intoParties, r.session.GetSelfPartyCode()) {
		return nil
	}

	issuer := r.session.ExecuteInfo.Issuer.Code
	// If any of the following conditions are met, issuer owns write privilege:
	// 1. issuer is self party
	// 2. in the config: write_authorized_parties
	if r.session.IsIssuer() {
		return nil
	}
	if slices.Contains(r.session.App.Conf.AuthorizedWritableParties, issuer) {
		return nil
	}

	return fmt.Errorf("issuer '%s' does not have privilege to write file to '%s'", issuer, r.session.GetSelfPartyCode())
}
