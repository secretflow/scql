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

package audit

import (
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/proto-gen/audit"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/stringutil"
)

var auditLogger = logrus.New()
var detailLogger = logrus.New()
var enableAudit = false

type AuditFormatter struct {
}

func (f *AuditFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	return []byte(entry.Message + "\n"), nil
}

type AuditConf struct {
	AuditLogFile            string `yaml:"audit_log_file"`
	AuditDetailFile         string `yaml:"audit_detail_file"`
	AuditMaxSizeInMegaBytes int    `yaml:"audit_max_size"`
	AuditMaxBackupsCount    int    `yaml:"audit_max_backups"`
	AuditMaxAgeInDays       int    `yaml:"audit_max_age_days"`
	AuditMaxCompress        bool   `yaml:"audit_max_compress"`
}

func InitAudit(config *AuditConf) error {
	if st, err := os.Stat(config.AuditLogFile); err == nil {
		if st.IsDir() {
			return fmt.Errorf("can't use directory as log file name")
		}
	}
	auditWriter := &lumberjack.Logger{
		Filename:   config.AuditLogFile,
		MaxSize:    config.AuditMaxSizeInMegaBytes, // megabytes
		MaxBackups: config.AuditMaxBackupsCount,
		MaxAge:     config.AuditMaxAgeInDays, //days
		Compress:   config.AuditMaxCompress,
	}
	auditLogger.SetFormatter(&AuditFormatter{})
	auditLogger.SetOutput(auditWriter)

	if st, err := os.Stat(config.AuditDetailFile); err == nil {
		if st.IsDir() {
			return fmt.Errorf("can't use directory as log file name")
		}
	}
	detailWriter := &lumberjack.Logger{
		Filename:   config.AuditDetailFile,
		MaxSize:    config.AuditMaxSizeInMegaBytes, // megabytes
		MaxBackups: config.AuditMaxBackupsCount,
		MaxAge:     config.AuditMaxAgeInDays, //days
		Compress:   config.AuditMaxCompress,
	}
	detailLogger.SetFormatter(&AuditFormatter{})
	detailLogger.SetOutput(detailWriter)
	enableAudit = true
	return nil
}

func recordAuditLog(auditLog *audit.AuditLog) error {
	if !enableAudit {
		return nil
	}
	if auditLog == nil {
		return fmt.Errorf("empty audit log message")
	}
	m := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}
	buf, err := m.Marshal(auditLog)
	if err != nil {
		return fmt.Errorf("marshal failed while record audit with err %v", err)
	}

	switch auditLog.Body.GetBody().(type) {
	case *audit.AuditBody_PlanDetail, *audit.AuditBody_DagDetail, *audit.AuditBody_CclDetail:
		detailLogger.Warn(string(buf))
	case *audit.AuditBody_SessionParams, *audit.AuditBody_RunAsyncQuery, *audit.AuditBody_RunSyncQuery, *audit.AuditBody_AsyncComplete, *audit.AuditBody_FetchResult, *audit.AuditBody_Uncategorized:
		auditLogger.Warn(string(buf))
	default:
		return fmt.Errorf("unknown audit log type: %T", auditLog.GetBody())
	}
	return nil
}

func RecordUncategorizedEvent(status *status.Status, sourceIp, urlPath string) {
	if !enableAudit {
		return
	}
	auditLog := &audit.AuditLog{
		Header: &audit.AuditHeader{
			Status: &scql.Status{
				Code:    int32(status.Code()),
				Message: status.Message(),
			},
			Time:      timestamppb.New(time.Now()),
			EventName: audit.EventName_UNCATEGORIZED,
		},
		Body: &audit.AuditBody{
			Body: &audit.AuditBody_Uncategorized{
				Uncategorized: &audit.UncategorizedEvent{
					SourceIp: sourceIp,
					UrlPath:  urlPath,
				},
			},
		},
	}
	err := recordAuditLog(auditLog)
	if err != nil {
		logrus.Warn(fmt.Printf("failed to record uncategorized event with error: %v", err))
	}
}

func RecordRunASyncQueryEvent(request *scql.SCDBQueryRequest, response *scql.SCDBSubmitResponse, timeStart time.Time, sourceIp string) {
	if !enableAudit {
		return
	}
	auditLog := &audit.AuditLog{
		Header: &audit.AuditHeader{
			Status:    response.GetStatus(),
			SessionId: response.GetScdbSessionId(),
			Time:      timestamppb.New(timeStart),
			EventName: audit.EventName_RUN_ASYNC_QUERY,
		},
		Body: &audit.AuditBody{
			Body: &audit.AuditBody_RunAsyncQuery{
				RunAsyncQuery: &audit.RunAsyncQueryEvent{
					UserName: request.GetUser().GetUser().GetNativeUser().GetName(),
					HostName: "%",
					SourceIp: sourceIp,
					Query:    stringutil.RemoveSensitiveInfo(request.Query),
					Type:     getQueryType(request.Query),
					CostTime: int64(time.Since(timeStart).Milliseconds()),
				},
			},
		},
	}
	err := recordAuditLog(auditLog)
	if err != nil {
		logrus.Warn(fmt.Printf("failed to record async query event with error: %v", err))
	}
}

func RecordRunSyncQueryEvent(request *scql.SCDBQueryRequest, response *scql.SCDBQueryResultResponse, timeStart time.Time, sourceIp string) {
	if !enableAudit {
		return
	}
	numRows, err := getNumRowsFromOutColumns(response.GetOutColumns())
	if err != nil {
		logrus.Warn(fmt.Printf("failed to get num rows when record sync query event with error: %v", err))
	}
	auditLog := &audit.AuditLog{
		Header: &audit.AuditHeader{
			Status:    response.GetStatus(),
			SessionId: response.GetScdbSessionId(),
			Time:      timestamppb.New(timeStart),
			EventName: audit.EventName_RUN_SYNC_QUERY,
		},
		Body: &audit.AuditBody{
			Body: &audit.AuditBody_RunSyncQuery{
				RunSyncQuery: &audit.RunSyncQueryEvent{
					UserName:     request.GetUser().GetUser().GetNativeUser().GetName(),
					HostName:     "%",
					SourceIp:     sourceIp,
					Query:        stringutil.RemoveSensitiveInfo(request.Query),
					Type:         getQueryType(request.Query),
					CostTime:     int64(time.Since(timeStart).Milliseconds()),
					AffectedRows: response.GetAffectedRows(),
					NumRows:      numRows,
				},
			},
		},
	}
	err = recordAuditLog(auditLog)
	if err != nil {
		logrus.Warn(fmt.Printf("failed to record sync query event with error: %v", err))
	}
}

func RecordFetchResultEvent(request *scql.SCDBFetchRequest, response *scql.SCDBQueryResultResponse, sourceIp string) {
	if !enableAudit {
		return
	}
	if response.GetStatus().GetCode() == int32(scql.Code_NOT_READY) {
		return
	}
	numRows, err := getNumRowsFromOutColumns(response.GetOutColumns())
	if err != nil {
		logrus.Warn(fmt.Printf("failed to get num rows when record fetch result event with error: %v", err))
	}

	auditLog := &audit.AuditLog{
		Header: &audit.AuditHeader{
			Status:    response.GetStatus(),
			SessionId: request.GetScdbSessionId(),
			Time:      timestamppb.New(time.Now()),
			EventName: audit.EventName_FETCH_RESULT,
		},
		Body: &audit.AuditBody{
			Body: &audit.AuditBody_FetchResult{
				FetchResult: &audit.FetchResultEvent{
					UserName:     request.GetUser().GetUser().GetNativeUser().GetName(),
					HostName:     "%",
					SourceIp:     sourceIp,
					AffectedRows: response.GetAffectedRows(),
					NumRows:      numRows,
				},
			},
		},
	}
	err = recordAuditLog(auditLog)
	if err != nil {
		logrus.Warn(fmt.Printf("failed to record fetch result event with error: %v", err))
	}
}

func RecordAsyncCompleteEvent(result *scql.SCDBQueryResultResponse) {
	if !enableAudit {
		return
	}
	numRows, err := getNumRowsFromOutColumns(result.GetOutColumns())
	if err != nil {
		logrus.Warn(fmt.Printf("failed to get num rows when record async complete event with error: %v", err))
	}
	auditLog := &audit.AuditLog{
		Header: &audit.AuditHeader{
			Status:    result.GetStatus(),
			SessionId: result.GetScdbSessionId(),
			Time:      timestamppb.New(time.Now()),
			EventName: audit.EventName_ASYNC_COMPLETE,
		},
		Body: &audit.AuditBody{
			Body: &audit.AuditBody_AsyncComplete{
				AsyncComplete: &audit.AsyncCompleteEvent{
					AffectedRows: result.GetAffectedRows(),
					NumRows:      numRows,
				},
			},
		},
	}
	err = recordAuditLog(auditLog)
	if err != nil {
		logrus.Warn(fmt.Printf("failed to record async complete event with error: %v", err))
	}
}

func RecordPlanDetail(partyCode, targetUrl string, req *scql.RunExecutionPlanRequest) {
	if !enableAudit {
		return
	}
	details := &audit.QueryPlanDetail{
		TargetUrl: targetUrl,
		PartyCode: partyCode,
	}
	for _, node := range req.GetGraph().GetNodes() {
		nodeInfo := getSimpleInfoFromExecNode(node)
		details.NodeList = append(details.NodeList, nodeInfo)
	}

	auditLog := &audit.AuditLog{
		Header: &audit.AuditHeader{
			Time:      timestamppb.New(time.Now()),
			SessionId: req.GetJobParams().GetJobId(),
			EventName: audit.EventName_PLAN_DETAIL,
		},
		Body: &audit.AuditBody{
			Body: &audit.AuditBody_PlanDetail{
				PlanDetail: details,
			},
		},
	}
	err := recordAuditLog(auditLog)
	if err != nil {
		logrus.Warn(fmt.Printf("failed to record plan details with error: %v", err))
	}
}

func RecordSessionParameters(params *scql.JobStartParams, targetUrl string, sync bool) {
	if !enableAudit {
		return
	}
	auditLog := &audit.AuditLog{
		Header: &audit.AuditHeader{
			Time:      timestamppb.New(time.Now()),
			SessionId: params.GetJobId(),
			EventName: audit.EventName_SESSION_PARAMS,
		},
		Body: &audit.AuditBody{
			Body: &audit.AuditBody_SessionParams{
				SessionParams: &audit.SessionParameters{
					Sync:       sync,
					Parameters: params,
				},
			},
		},
	}
	err := recordAuditLog(auditLog)
	if err != nil {
		logrus.Warn(fmt.Printf("failed to record session parameters with error: %v", err))
	}
}

func RecordSecurityConfig(cclInfo *scql.SecurityConfig, sessionId string) {
	if !enableAudit {
		return
	}
	auditLog := &audit.AuditLog{
		Header: &audit.AuditHeader{
			Time:      timestamppb.New(time.Now()),
			SessionId: sessionId,
			EventName: audit.EventName_CCL_DETAIL,
		},
		Body: &audit.AuditBody{
			Body: &audit.AuditBody_CclDetail{
				CclDetail: cclInfo,
			},
		},
	}
	err := recordAuditLog(auditLog)
	if err != nil {
		logrus.Warn(fmt.Printf("failed to record session parameters with error: %v", err))
	}
}

func getQueryType(query string) audit.QueryType {
	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return audit.QueryType_UNKNOWN
	}
	switch stmt.(type) {
	case *ast.SelectStmt, *ast.UnionStmt:
		return audit.QueryType_DQL
	case *ast.GrantStmt:
		return audit.QueryType_GRANT_PRIV
	case *ast.RevokeStmt:
		return audit.QueryType_REVOKE_PRIV
	case *ast.ShowStmt:
		return audit.QueryType_SHOW_STMT
	case *ast.CreateUserStmt:
		return audit.QueryType_CREATE_USER
	case *ast.DropUserStmt:
		return audit.QueryType_DROP_USER
	case *ast.AlterUserStmt:
		return audit.QueryType_ALTER_USER
	case *ast.CreateDatabaseStmt:
		return audit.QueryType_CREATE_DATABASE
	case *ast.CreateTableStmt:
		return audit.QueryType_CREATE_TABLE
	case *ast.DropDatabaseStmt:
		return audit.QueryType_DROP_DATABASE
	case *ast.DropTableStmt:
		return audit.QueryType_DROP_TABLE
	case *ast.CreateViewStmt:
		return audit.QueryType_CREATE_VIEW
	case *ast.ExplainStmt:
		return audit.QueryType_EXPLAIN_STMT
	default:
		return audit.QueryType_UNKNOWN
	}
}

func getNodeInfoFromTensorList(tensorList map[string]*scql.TensorList) map[string]*audit.Strings {
	result := map[string]*audit.Strings{}
	for key, tensors := range tensorList {
		var names []string
		for _, t := range tensors.GetTensors() {
			names = append(names, t.Name)
		}
		result[key] = &audit.Strings{
			Ss: names,
		}
	}
	return result
}

func getSimpleInfoFromExecNode(node *scql.ExecNode) *audit.NodeInfo {
	dagInfo := &audit.NodeInfo{
		Name:    node.NodeName,
		Inputs:  getNodeInfoFromTensorList(node.Inputs),
		Outputs: getNodeInfoFromTensorList(node.Outputs),
	}
	return dagInfo
}

func getNumRowsFromOutColumns(outColumns []*scql.Tensor) (int64, error) {
	var rows int64
	for _, column := range outColumns {
		switch x := column.Shape.Dim[0].Value.(type) {
		case *scql.TensorShape_Dimension_DimValue:
			{
				if rows == 0 && x.DimValue != 0 {
					rows = x.DimValue
				} else if rows != x.DimValue {
					return 0, fmt.Errorf("all outColumn should be same length")
				}

			}
		case *scql.TensorShape_Dimension_DimParam:
			return 0, fmt.Errorf("unexpected type:%T", x)
		}

	}
	return rows, nil
}
