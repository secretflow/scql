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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/secretflow/scql/pkg/proto-gen/audit"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

var config = &AuditConf{
	AuditMaxSizeInMegaBytes: 500,
	AuditMaxBackupsCount:    0,
	AuditMaxAgeInDays:       180,
	AuditMaxCompress:        false,
}

// protojson.Marshal is unstable
func JsonStringEqual(expected, actual string) bool {
	expected_without_space := strings.Replace(expected, " ", "", -1)
	actual_without_space := strings.Replace(actual, " ", "", -1)
	return expected_without_space == actual_without_space
}

func TestAuditHeaderToJsonString(t *testing.T) {
	// GIVEN
	r := require.New(t)
	getQueryType("DESCRIBE demo.ta")
	auditLog := &audit.AuditLog{
		Header: &audit.AuditHeader{
			Time:      timestamppb.New(time.Date(2022, 10, 1, 2, 0, 0, 0, time.UTC)),
			Status:    &scql.Status{Code: int32(scql.Code_OK)},
			SessionId: "a0b72d96-f305-11ed-833c-0242c0a82005",
			EventName: audit.EventName_UNCATEGORIZED,
		},
	}
	m := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}
	buf, err := m.Marshal(auditLog)
	r.NoError(err)

	actual := string(buf)
	fmt.Println(actual)
	expected := `{"header":{"time":"2022-10-01T02:00:00Z", "status":{"code":0, "message":"", "details":[]}, "event_name":"UNCATEGORIZED", "session_id":"a0b72d96-f305-11ed-833c-0242c0a82005"}, "body":null}`

	r.True(JsonStringEqual(expected, actual))
}

func TestRunSyncQueryEventToJsonString(t *testing.T) {
	// GIVEN
	r := require.New(t)
	auditLog := &audit.AuditLog{
		Header: &audit.AuditHeader{
			Time:      timestamppb.New(time.Date(2022, 10, 1, 2, 0, 0, 0, time.UTC)),
			Status:    &scql.Status{Code: int32(scql.Code_OK)},
			SessionId: "a0b72d96-f305-11ed-833c-0242c0a82005",
			EventName: audit.EventName_RUN_SYNC_QUERY,
		},
		Body: &audit.AuditBody{
			Body: &audit.AuditBody_RunSyncQuery{
				RunSyncQuery: &audit.RunSyncQueryEvent{
					UserName: "alice",
					HostName: "%",
					SourceIp: "127.0.0.1",
					Query:    "select plain_int_0 from scdb.alice_tbl_1",
					Type:     audit.QueryType_DQL,
					NumRows:  30,
					CostTime: 3000,
				},
			},
		},
	}
	m := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}
	buf, err := m.Marshal(auditLog)
	r.NoError(err)

	r.NoError(err)

	actual := string(buf)
	fmt.Println(actual)
	expected := `{"header":{"time":"2022-10-01T02:00:00Z","status":{"code":0,"message":"","details":[]},"event_name":"RUN_SYNC_QUERY","session_id":"a0b72d96-f305-11ed-833c-0242c0a82005"},"body":{"run_sync_query":{"user_name":"alice","host_name":"%","source_ip":"127.0.0.1","query":"select plain_int_0 from scdb.alice_tbl_1","type":"DQL","num_rows":"30","affected_rows":"0","cost_time":"3000"}}}`
	r.True(JsonStringEqual(expected, actual))
}

func TestAsyncCompleteEventToJsonString(t *testing.T) {
	// GIVEN
	r := require.New(t)
	auditLog := &audit.AuditLog{
		Header: &audit.AuditHeader{
			Time:      timestamppb.New(time.Date(2022, 10, 1, 2, 0, 0, 0, time.UTC)),
			Status:    &scql.Status{Code: int32(scql.Code_OK)},
			SessionId: "a0b72d96-f305-11ed-833c-0242c0a82005",
			EventName: audit.EventName_ASYNC_COMPLETE,
		},
		Body: &audit.AuditBody{
			Body: &audit.AuditBody_AsyncComplete{
				AsyncComplete: &audit.AsyncCompleteEvent{
					NumRows: 10,
				},
			},
		},
	}
	m := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}
	buf, err := m.Marshal(auditLog)
	r.NoError(err)

	actual := string(buf)
	fmt.Println(actual)
	expected := `{"header":{"time":"2022-10-01T02:00:00Z", "status":{"code":0, "message":"", "details":[]}, "event_name":"ASYNC_COMPLETE", "session_id":"a0b72d96-f305-11ed-833c-0242c0a82005"}, "body":{"async_complete":{"num_rows":"10", "affected_rows":"0"}}}`
	r.True(JsonStringEqual(expected, actual))
}

func TestFetchResultEventToJsonString(t *testing.T) {
	// GIVEN
	r := require.New(t)
	auditLog := &audit.AuditLog{
		Header: &audit.AuditHeader{
			Time:      timestamppb.New(time.Date(2022, 10, 1, 2, 0, 0, 0, time.UTC)),
			Status:    &scql.Status{Code: int32(scql.Code_OK)},
			SessionId: "a0b72d96-f305-11ed-833c-0242c0a82005",
			EventName: audit.EventName_FETCH_RESULT,
		},
		Body: &audit.AuditBody{
			Body: &audit.AuditBody_FetchResult{
				FetchResult: &audit.FetchResultEvent{
					UserName: "alice",
					HostName: "%",
					SourceIp: "127.0.0.1",
					NumRows:  2,
				},
			},
		},
	}
	m := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}
	buf, err := m.Marshal(auditLog)
	r.NoError(err)

	actual := string(buf)
	fmt.Println(actual)
	expected := `{"header":{"time":"2022-10-01T02:00:00Z","status":{"code":0,"message":"","details":[]},"event_name":"FETCH_RESULT","session_id":"a0b72d96-f305-11ed-833c-0242c0a82005"},"body":{"fetch_result":{"user_name":"alice","host_name":"%","source_ip":"127.0.0.1","num_rows":"2","affected_rows":"0"}}}`
	r.True(JsonStringEqual(expected, actual))
}

func TestUncategorizedEventToJsonString(t *testing.T) {
	// GIVEN
	r := require.New(t)
	auditLog := &audit.AuditLog{
		Header: &audit.AuditHeader{
			Time: timestamppb.New(time.Date(2022, 10, 1, 2, 0, 0, 0, time.UTC)),
			Status: &scql.Status{
				Code:    int32(scql.Code_BAD_REQUEST),
				Message: "invalid request body",
			},
			EventName: audit.EventName_UNCATEGORIZED,
		},
		Body: &audit.AuditBody{
			Body: &audit.AuditBody_Uncategorized{
				Uncategorized: &audit.UncategorizedEvent{
					UrlPath:  "SubmitAndGet",
					SourceIp: "127.0.0.1",
				},
			},
		},
	}

	m := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}
	buf, err := m.Marshal(auditLog)
	r.NoError(err)

	actual := string(buf)
	fmt.Println(actual)
	expected := `{"header":{"time":"2022-10-01T02:00:00Z", "status":{"code":100, "message":"invalid request body", "details":[]}, "event_name":"UNCATEGORIZED", "session_id":""}, "body":{"uncategorized":{"url_path":"SubmitAndGet", "source_ip":"127.0.0.1"}}}`
	r.True(JsonStringEqual(expected, actual))
}

func TestDeserializeFromAuditJson(t *testing.T) {
	r := require.New(t)
	t.Run("DeserializeFromUncategorized", func(t *testing.T) {
		var auditLog audit.AuditLog
		uncategorizedEvent := `{"header":{"time":"2022-10-01T02:00:00Z", "status":{"code":100, "message":"invalid request body", "details":[]}, "event_name":"UNCATEGORIZED", "session_id":""}, "body":{"uncategorized":{"url_path":"SubmitAndGet", "source_ip":"127.0.0.1"}}}`
		err := message.ProtoUnmarshal([]byte(uncategorizedEvent), &auditLog)
		r.NoError(err)
		r.Equal(audit.EventName_UNCATEGORIZED, auditLog.Header.EventName)
		r.Equal("invalid request body", auditLog.Header.Status.Message)
		r.Equal("127.0.0.1", auditLog.Body.GetUncategorized().SourceIp)
	})
	t.Run("DeserializeFromFetchResult", func(t *testing.T) {
		var auditLog audit.AuditLog
		fetchResultEvent := `{"header":{"time":"2022-10-01T02:00:00Z","status":{"code":0,"message":"","details":[]},"event_name":"FETCH_RESULT","session_id":"a0b72d96-f305-11ed-833c-0242c0a82005"},"body":{"fetch_result":{"user_name":"alice","host_name":"%","source_ip":"127.0.0.1","num_rows":"2","affected_rows":"0"}}}`
		err := message.ProtoUnmarshal([]byte(fetchResultEvent), &auditLog)
		r.NoError(err)
		r.Equal(audit.EventName_FETCH_RESULT, auditLog.Header.EventName)
		r.Equal(int64(2), auditLog.Body.GetFetchResult().NumRows)
	})
	t.Run("DeserializeFromAsyncComplete", func(t *testing.T) {
		var auditLog audit.AuditLog
		asyncCompleteEvent := `{"header":{"time":"2022-10-01T02:00:00Z", "status":{"code":0, "message":"", "details":[]}, "event_name":"ASYNC_COMPLETE", "session_id":"a0b72d96-f305-11ed-833c-0242c0a82005"}, "body":{"async_complete":{"num_rows":"10", "affected_rows":"0"}}}`
		err := message.ProtoUnmarshal([]byte(asyncCompleteEvent), &auditLog)
		r.NoError(err)
		r.Equal(audit.EventName_ASYNC_COMPLETE, auditLog.Header.EventName)
		r.Equal(int64(10), auditLog.Body.GetAsyncComplete().NumRows)
	})

	t.Run("DeserializeFromRunSyncQuery", func(t *testing.T) {
		var auditLog audit.AuditLog
		runQueryEvent := `{"header":{"time":"2022-10-01T02:00:00Z","status":{"code":0,"message":"","details":[]},"event_name":"RUN_SYNC_QUERY","session_id":"a0b72d96-f305-11ed-833c-0242c0a82005"},"body":{"run_sync_query":{"user_name":"alice","host_name":"%","source_ip":"127.0.0.1","query":"select plain_int_0 from scdb.alice_tbl_1","type":"DQL","num_rows":"30","affected_rows":"0","cost_time":"3000"}}}`
		err := message.ProtoUnmarshal([]byte(runQueryEvent), &auditLog)
		r.NoError(err)
		r.Equal(audit.EventName_RUN_SYNC_QUERY, auditLog.Header.EventName)
		r.Equal("select plain_int_0 from scdb.alice_tbl_1", auditLog.Body.GetRunSyncQuery().Query)
	})

}

func TestRecordAuditLog(t *testing.T) {
	r := require.New(t)
	// GIVE
	auditLogFile, err := os.CreateTemp("", "*.log")
	r.NoError(err)
	detailLogFile, err := os.CreateTemp("", "*.log")
	r.NoError(err)
	defer os.Remove(auditLogFile.Name())
	defer os.Remove(detailLogFile.Name())
	config.AuditLogFile = auditLogFile.Name()
	config.AuditDetailFile = detailLogFile.Name()
	err = InitAudit(config)
	r.NoError(err)

	t.Run("RecordRunSyncQueryEvent", func(t *testing.T) {
		auditLog := &audit.AuditLog{
			Header: &audit.AuditHeader{
				Time:      timestamppb.New(time.Date(2022, 10, 1, 2, 0, 0, 0, time.UTC)),
				Status:    &scql.Status{Code: int32(scql.Code_OK)},
				SessionId: "a0b72d96-f305-11ed-833c-0242c0a82005",
				EventName: audit.EventName_RUN_SYNC_QUERY,
			},
			Body: &audit.AuditBody{
				Body: &audit.AuditBody_RunSyncQuery{
					RunSyncQuery: &audit.RunSyncQueryEvent{
						UserName: "alice",
						HostName: "%",
						SourceIp: "127.0.0.1",
						Query:    "select plain_int_0 from scdb.alice_tbl_1",
						NumRows:  30,
						CostTime: 3000,
					},
				},
			},
		}
		r.NoError(recordAuditLog(auditLog))
		expected := `{"header":{"time":"2022-10-01T02:00:00Z","status":{"code":0,"message":"","details":[]},"event_name":"RUN_SYNC_QUERY","session_id":"a0b72d96-f305-11ed-833c-0242c0a82005"},"body":{"run_sync_query":{"user_name":"alice","host_name":"%","source_ip":"127.0.0.1","query":"select plain_int_0 from scdb.alice_tbl_1","type":"UNKNOWN","num_rows":"30","affected_rows":"0","cost_time":"3000"}}}` + "\n"
		bytes, err := os.ReadFile(config.AuditLogFile)
		r.NoError(err)
		actual := string(bytes)
		fmt.Println(actual)
		r.True(JsonStringEqual(expected, actual))
	})

	t.Run("RecordDagDetailEvent", func(t *testing.T) {
		auditLog := &audit.AuditLog{
			Header: &audit.AuditHeader{
				Time:      timestamppb.New(time.Date(2022, 10, 1, 2, 0, 0, 0, time.UTC)),
				Status:    &scql.Status{Code: int32(scql.Code_OK)},
				SessionId: "a0b72d96-f305-11ed-833c-0242c0a82005",
				EventName: audit.EventName_DAG_DETAIL,
			},
			Body: &audit.AuditBody{
				Body: &audit.AuditBody_DagDetail{
					DagDetail: &audit.QueryDagDetail{
						PartyCode: "alice",
						DagId:     3,
						NodeList: []*audit.NodeInfo{
							{
								Name: "join.2",
								Inputs: map[string]*audit.Strings{
									"Left": {
										Ss: []string{"demo.tb.id.4"},
									},
									"Right": {
										Ss: []string{"demo.tb.id.0"},
									},
								},
								Outputs: map[string]*audit.Strings{
									"LeftJoinIndex": {
										Ss: []string{"demo.tb.id.7"},
									},
									"RightJoinIndex": {
										Ss: []string{"demo.tb.id.8"},
									},
								},
							},
							{
								Name: "make_share.9",
								Inputs: map[string]*audit.Strings{
									"In": {
										Ss: []string{"demo.ta.income.11"},
									},
								},
								Outputs: map[string]*audit.Strings{
									"Out": {
										Ss: []string{"demo.ta.income.20"},
									},
								},
							},
						},
					},
				},
			},
		}

		r.NoError(recordAuditLog(auditLog))
		expected := `{"header":{"time":"2022-10-01T02:00:00Z", "status":{"code":0, "message":"", "details":[]}, "event_name":"DAG_DETAIL", "session_id":"a0b72d96-f305-11ed-833c-0242c0a82005"}, "body":{"dag_detail":{"party_code":"alice", "target_url":"", "dag_id":3, "node_list":[{"name":"join.2", "inputs":{"Left":{"ss":["demo.tb.id.4"]}, "Right":{"ss":["demo.tb.id.0"]}}, "outputs":{"LeftJoinIndex":{"ss":["demo.tb.id.7"]}, "RightJoinIndex":{"ss":["demo.tb.id.8"]}}}, {"name":"make_share.9", "inputs":{"In":{"ss":["demo.ta.income.11"]}}, "outputs":{"Out":{"ss":["demo.ta.income.20"]}}}]}}}` + "\n"
		bytes, err := os.ReadFile(config.AuditDetailFile)
		r.NoError(err)
		actual := string(bytes)
		fmt.Println(actual)
		r.True(JsonStringEqual(expected, actual))
	})
}
