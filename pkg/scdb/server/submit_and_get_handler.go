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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/audit"
	bcfg "github.com/secretflow/scql/pkg/broker/config"
	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/executor"
	"github.com/secretflow/scql/pkg/interpreter"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/config"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/logutil"
	"github.com/secretflow/scql/pkg/util/message"
)

func (app *App) SubmitAndGetHandler(c *gin.Context) {
	timeStart := time.Now()
	logEntry := &logutil.MonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "SCDBSubmitAndGetHandler", c.FullPath()),
	}

	request := &scql.SCDBQueryRequest{}
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, request, c.Request.Header.Get("Content-Type"))
	if err != nil {
		logEntry.Reason = constant.ReasonInvalidRequestFormat
		logEntry.ErrorMsg = err.Error()
		logEntry.CostTime = time.Since(timeStart)
		logrus.Errorf("%v|ClientIP:%v", logEntry, c.ClientIP())
		c.String(http.StatusOK, errorResponse(scql.Code_BAD_REQUEST, "invalid request body", message.EncodingTypeJson))
		audit.RecordUncategorizedEvent(status.New(scql.Code_BAD_REQUEST, "invalid request body"), c.ClientIP(), "submitAndGet")
		return
	}

	resp := app.submitAndGet(c.Request.Context(), request)
	body, _ := message.SerializeTo(resp, inputEncodingType)
	c.String(http.StatusOK, body)

	logEntry.RequestID = request.BizRequestId
	logEntry.SessionID = resp.ScdbSessionId
	logEntry.RawRequest = SCDBQueryRequestToLogString(request)
	logEntry.CostTime = time.Since(timeStart)

	audit.RecordRunSyncQueryEvent(request, resp, timeStart, c.ClientIP())
	if resp.Status.Code != int32(scql.Code_OK) {
		logEntry.Reason = constant.ReasonInvalidRequest
		logEntry.ErrorMsg = resp.Status.Message
		logrus.Errorf("%v|ClientIP:%v", logEntry, c.ClientIP())
		return
	}
	logrus.Infof("%v|ClientIP:%v", logEntry, c.ClientIP())
}

// submitAndGet implements core logic of SubmitAndGetHandler
func (app *App) submitAndGet(ctx context.Context, req *scql.SCDBQueryRequest) *scql.SCDBQueryResultResponse {
	session, err := newSession(ctx, req, app.storage)
	if err != nil {
		return newErrorSCDBQueryResultResponse(scql.Code_INTERNAL, err.Error())
	}

	// authentication
	if err = session.authenticateUser(req.User); err != nil {
		return newErrorSCDBQueryResultResponse(scql.Code_UNAUTHENTICATED, err.Error())
	}

	isDQL, err := isDQL(req.Query)
	if err != nil {
		return newErrorSCDBQueryResultResponse(scql.Code_SQL_PARSE_ERROR, err.Error())
	}
	session.isDQLRequest = isDQL

	if isDQL {
		return app.submitAndGetDQL(ctx, session)
	}
	app.runSQL(session)
	return session.result
}

func (app *App) buildCompileRequest(ctx context.Context, s *session) (*scql.CompileQueryRequest, error) {
	issuer := s.GetSessionVars().User
	if issuer.Username == storage.DefaultRootName && issuer.Hostname == storage.DefaultHostName {
		return nil, fmt.Errorf("user root has no privilege to execute dql")
	}
	issuerPartyCode, err := storage.QueryUserPartyCode(s.GetSessionVars().Storage, issuer.Username, issuer.Hostname)
	if err != nil {
		return nil, fmt.Errorf("failed to query issuer party code: %v", err)
	}

	dsList, err := app.extractDataSourcesInDQL(ctx, s)
	if err != nil {
		return nil, err
	}

	// check datasource
	if len(dsList) == 0 {
		return nil, fmt.Errorf("no data source specified in the query")
	}
	dbName := dsList[0].DBName.String()
	s.GetSessionVars().CurrentDB = dbName
	// check if referenced tables are in the same db
	if len(dsList) > 1 {
		for _, ds := range dsList[1:] {
			if ds.DBName.String() != dbName {
				return nil, fmt.Errorf("query is not allowed to execute across multiply databases")
			}
		}
	}

	// collect referenced table schemas
	tableNames := make([]string, 0, len(dsList))
	for _, ds := range dsList {
		tableNames = append(tableNames, ds.TableInfo().Name.String())
	}

	// collect all view in db
	// TODO: possible optimization: Analysis AST to find all reference tables (including real data source & view)
	views, err := storage.QueryAllViewsInDb(s.GetSessionVars().Storage, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to query all views in db `%s`: %v", dbName, err)
	}
	tableNames = append(tableNames, views...)

	catalog, err := app.buildCatalog(s.GetSessionVars().Storage, dbName, tableNames)
	if err != nil {
		return nil, fmt.Errorf("failed to build catalog: %v", err)
	}

	securityConfig, err := buildSecurityConfig(s.GetSessionVars().Storage, issuerPartyCode, catalog.Tables)
	if err != nil {
		return nil, err
	}

	spuRuntimeCfg, err := config.NewSpuRuntimeCfg(app.config.Engine.SpuRuntimeCfg)
	if err != nil {
		return nil, err
	}

	groupByThreshold := constant.DefaultGroupByThreshold
	if app.config.SecurityCompromise.GroupByThreshold > 0 {
		groupByThreshold = app.config.SecurityCompromise.GroupByThreshold
	}

	req := &scql.CompileQueryRequest{
		Query:  s.request.GetQuery(),
		DbName: dbName,
		Issuer: &scql.PartyId{
			Code: issuerPartyCode,
		},
		IssuerAsParticipant: false,
		Catalog:             catalog,
		SecurityConf:        securityConfig,
		CompileOpts: &scql.CompileOptions{
			SpuConf: spuRuntimeCfg,
			SecurityCompromise: &scql.SecurityCompromiseConfig{
				RevealGroupMark:  app.config.SecurityCompromise.RevealGroupMark,
				GroupByThreshold: groupByThreshold,
			},
			DumpExeGraph: true,
		},
	}

	return req, nil
}

func buildSecurityConfig(store *gorm.DB, issuerPartyCode string, tables []*scql.TableEntry) (*scql.SecurityConfig, error) {
	parties := make([]string, 0, len(tables))
	for _, tbl := range tables {
		if !tbl.IsView {
			parties = append(parties, tbl.GetOwner().GetCode())
		}
	}

	var ccl []*scql.SecurityConfig_ColumnControl
	foundQueryIssuer := false
	for _, party := range parties {
		if party == issuerPartyCode {
			foundQueryIssuer = true
		}
		cc, err := collectCCLForParty(store, party, tables)
		if err != nil {
			return nil, err
		}
		ccl = append(ccl, cc...)
	}

	// SecurityConfig should contains the query issuer
	if !foundQueryIssuer {
		cc, err := collectCCLForParty(store, issuerPartyCode, tables)
		if err != nil {
			return nil, err
		}
		ccl = append(ccl, cc...)
	}
	return &scql.SecurityConfig{
		ColumnControlList: ccl,
	}, nil
}

func QueryTableSchemas(store *gorm.DB, dbName string, tableNames []string) (tables []*scql.TableEntry, err error) {
	callFc := func(tx *gorm.DB) error {
		tables, err = queryTableSchemas(tx, dbName, tableNames)
		return err
	}
	if err := store.Transaction(callFc, &sql.TxOptions{ReadOnly: true}); err != nil {
		return nil, fmt.Errorf("queryTableSchemas: %v", err)
	}
	return tables, nil
}

func queryTableSchemas(store *gorm.DB, dbName string, tableNames []string) ([]*scql.TableEntry, error) {
	var tables []storage.Table
	result := store.Model(&storage.Table{}).Where("db = ? AND table_name in ?", dbName, tableNames).Find(&tables)
	if result.Error != nil {
		return nil, result.Error
	}

	userPartyMap := make(map[string]string)
	getPartyCodeByUser := func(store *gorm.DB, username, host string) (string, error) {
		if p, ok := userPartyMap[username]; ok {
			return p, nil
		}
		p, err := storage.QueryUserPartyCode(store, username, host)
		if err == nil {
			userPartyMap[username] = p
		}
		return p, err
	}

	var tblEntries []*scql.TableEntry
	for _, tbl := range tables {
		ownerPartyCode, err := getPartyCodeByUser(store, tbl.Owner, tbl.Host)
		if err != nil {
			return nil, err
		}
		tblEntry := scql.TableEntry{
			TableName:    fmt.Sprintf("%s.%s", tbl.Db, tbl.Table),
			IsView:       tbl.IsView,
			SelectString: tbl.SelectString,
			RefTable:     fmt.Sprintf("%s.%s", tbl.RefDb, tbl.RefTable),
			Owner: &scql.PartyId{
				Code: ownerPartyCode,
			},
			DbType: core.DBType(tbl.DBType).String(),
		}

		var cols []storage.Column
		result := store.Model(&storage.Column{}).Where("db = ? AND table_name = ?", dbName, tbl.Table).Find(&cols)
		if result.Error != nil {
			return nil, result.Error
		}
		for _, col := range cols {
			tblEntry.Columns = append(tblEntry.Columns, &scql.TableEntry_Column{
				Name:            col.ColumnName,
				Type:            col.Type,
				OrdinalPosition: int32(col.OrdinalPosition),
			})
		}
		tblEntries = append(tblEntries, &tblEntry)
	}
	return tblEntries, nil
}

// If parameter async is true, the query result will be notified by engine, this function will always return nil
func (app *App) runDQL(ctx context.Context, s *session, async bool) (*scql.SCDBQueryResultResponse, error) {
	compileReq, err := app.buildCompileRequest(ctx, s)
	if err != nil {
		return nil, err
	}
	intrpr := interpreter.NewInterpreter()
	compiledPlan, err := intrpr.Compile(ctx, compileReq)

	if err != nil {
		return nil, err
	}

	s.GetSessionVars().AffectedByGroupThreshold = compiledPlan.Warning.GetMayAffectedByGroupThreshold()
	s.GetSessionVars().GroupByThreshold = compileReq.CompileOpts.SecurityCompromise.GroupByThreshold
	logrus.Infof("Execution Plan:\n%s\n", compiledPlan.GetExplain().GetExeGraphDot())

	sessionStartParams := &scql.JobStartParams{
		JobId:         s.id,
		SpuRuntimeCfg: compiledPlan.GetSpuRuntimeConf(),
		TimeZone:      s.GetSessionVars().GetTimeZone(),
	}
	var partyCodes []string
	for _, p := range compiledPlan.Parties {
		partyCodes = append(partyCodes, p.GetCode())
	}

	var partyInfo *graph.PartyInfo
	{
		db := s.GetSessionVars().Storage
		var users []storage.User
		result := db.Model(&storage.User{}).Where("party_code in ?", partyCodes).Find(&users)
		if result.Error != nil {
			return nil, result.Error
		}
		partyMap := make(map[string]*graph.Participant)
		for _, u := range users {
			participant := &graph.Participant{
				PartyCode: u.PartyCode,
				Endpoints: strings.Split(u.EngineEndpoints, ";"),
				Token:     u.EngineToken,
				PubKey:    u.EnginePubKey,
			}
			partyMap[u.PartyCode] = participant
		}
		participants := make([]*graph.Participant, 0, len(partyCodes))
		for i, code := range partyCodes {
			party, exists := partyMap[code]
			if !exists {
				return nil, fmt.Errorf("could not find info for party %s", code)
			}
			participants = append(participants, party)
			sessionStartParams.Parties = append(sessionStartParams.Parties, &scql.JobStartParams_Party{
				Code:      code,
				Name:      code,
				Host:      party.Endpoints[0],
				Rank:      int32(i),
				PublicKey: party.PubKey,
			})
		}
		partyInfo = graph.NewPartyInfo(participants)
	}

	pbRequests := make(map[string]*scql.RunExecutionPlanRequest)
	for party, graph := range compiledPlan.SubGraphs {
		startParams, ok := proto.Clone(sessionStartParams).(*scql.JobStartParams)
		if !ok {
			return nil, fmt.Errorf("failed to clone session start params")
		}
		startParams.PartyCode = party
		cbURL := url.URL{
			Scheme: app.config.Protocol,
			Host:   app.config.SCDBHost,
			Path:   engineCallbackPath,
		}
		pbRequests[party] = &scql.RunExecutionPlanRequest{
			JobParams:     startParams,
			Graph:         graph,
			Async:         async,
			CallbackUrl:   cbURL.String(),
			GraphChecksum: &scql.GraphChecksum{CheckGraphChecksum: false},
		}
	}

	engineClient := executor.NewEngineClient(
		executor.EngineClientTypeHTTP,
		app.config.Engine.ClientTimeout,
		&bcfg.TLSConf{
			Mode:       app.config.Engine.TLSCfg.Mode,
			CertPath:   app.config.Engine.TLSCfg.CertFile,
			KeyPath:    app.config.Engine.TLSCfg.KeyFile,
			CACertPath: app.config.Engine.TLSCfg.CACertFile,
		},
		app.config.Engine.ContentType,
		app.config.Engine.Protocol,
	)
	s.engineStub = executor.NewEngineStub(
		s.id,
		app.config.Protocol,
		app.config.SCDBHost,
		engineCallbackPath,
		engineClient,
	)

	var outputNames []string
	for _, col := range compiledPlan.GetSchema().GetColumns() {
		outputNames = append(outputNames, col.GetName())
	}

	exec, err := executor.NewExecutor(pbRequests, outputNames, s.engineStub, s.id, partyInfo)
	if err != nil {
		return nil, err
	}
	s.executor = exec
	resp, err := exec.RunExecutionPlan(ctx, async)
	if err != nil {
		return nil, err
	}

	if async {
		// In async mode, result will be set in callback
		return nil, nil
	}
	return resp, nil
}

// submitAndGetDQL executes query and gets result back
func (app *App) submitAndGetDQL(ctx context.Context, s *session) *scql.SCDBQueryResultResponse {
	resp, err := app.runDQL(ctx, s, false)
	if err != nil {
		var st *status.Status
		if errors.As(err, &st) {
			return newErrorFetchResponse(s.id, st.Code(), st.Message())
		}
		return newErrorFetchResponse(s.id, scql.Code_INTERNAL, err.Error())
	}

	s.setResultWithOutputColumnsAndAffectedRows(resp.OutColumns, resp.AffectedRows)

	return s.result
}
