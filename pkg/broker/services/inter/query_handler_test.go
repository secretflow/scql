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

package inter_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/constant"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/services/inter"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
	"github.com/secretflow/scql/pkg/util/brokerutil"
	"github.com/secretflow/scql/pkg/util/message"
)

type interTestSuite struct {
	suite.Suite
	svcAlice       *inter.InterSvc
	svcBob         *inter.InterSvc
	svcCarol       *inter.InterSvc
	ctx            context.Context
	testAppBuilder *brokerutil.TestAppBuilder
}

func TestServerSuit(t *testing.T) {
	suite.Run(t, new(interTestSuite))
}

func (s *interTestSuite) SetupSuite() {
	s.testAppBuilder = &brokerutil.TestAppBuilder{}
	s.NoError(s.testAppBuilder.BuildAppTests(s.Suite.T().TempDir()))
	s.svcAlice = inter.NewInterSvc(s.testAppBuilder.AppAlice)
	s.svcBob = inter.NewInterSvc(s.testAppBuilder.AppBob)
	s.svcCarol = inter.NewInterSvc(s.testAppBuilder.AppCarol)
	s.ctx = context.Background()
	s.testAppBuilder.ServerAlice.Start()
	s.testAppBuilder.ServerBob.Start()
	s.testAppBuilder.ServerCarol.Start()
	s.testAppBuilder.ServerEngine.Start()
}

func (s *interTestSuite) bootstrap(manager *storage.MetaManager) {
	txn := manager.CreateMetaTransaction()
	defer txn.Finish(nil)
	projConf, _ := message.ProtoMarshal(&scql.ProjectConfig{SpuRuntimeCfg: &spu.RuntimeConfig{Protocol: spu.ProtocolKind_SEMI2K, Field: spu.FieldType_FM64}, SessionExpireSeconds: 86400})
	s.NoError(txn.CreateProject(storage.Project{ID: "1", Name: "test project", Creator: "bob", ProjectConf: string(projConf)}))
	s.NoError(txn.AddProjectMembers([]storage.Member{storage.Member{ProjectID: "1", Member: "alice"}}))
	s.NoError(txn.AddProjectMembers([]storage.Member{storage.Member{ProjectID: "1", Member: "carol"}}))
	// mock table/ccl
	metaA := storage.TableMeta{
		Table: storage.Table{
			TableIdentifier: storage.TableIdentifier{TableName: "ta", ProjectID: "1"},
			RefTable:        "real.ta",
			DBType:          "MYSQL",
			Owner:           "alice",
		},
		Columns: []storage.ColumnMeta{{ColumnName: "id", DType: "int"}, {ColumnName: "data", DType: "int"}},
	}
	s.NoError(txn.AddTable(metaA))
	metaB := storage.TableMeta{
		Table: storage.Table{
			TableIdentifier: storage.TableIdentifier{TableName: "tb", ProjectID: "1"},
			RefTable:        "real.tb",
			DBType:          "MYSQL",
			Owner:           "bob",
		},
		Columns: []storage.ColumnMeta{{ColumnName: "id", DType: "int"}, {ColumnName: "data", DType: "int"}},
	}
	s.NoError(txn.AddTable(metaB))
	metaC := storage.TableMeta{
		Table: storage.Table{
			TableIdentifier: storage.TableIdentifier{TableName: "tc", ProjectID: "1"},
			RefTable:        "real.tc",
			DBType:          "MYSQL",
			Owner:           "carol",
		},
		Columns: []storage.ColumnMeta{{ColumnName: "id", DType: "int"}, {ColumnName: "data", DType: "int"}},
	}
	s.NoError(txn.AddTable(metaC))
	c1Identifier := storage.ColumnPrivIdentifier{ProjectID: "1", TableName: "ta", ColumnName: "id"}
	c2Identifier := storage.ColumnPrivIdentifier{ProjectID: "1", TableName: "ta", ColumnName: "data"}
	c3Identifier := storage.ColumnPrivIdentifier{ProjectID: "1", TableName: "tb", ColumnName: "id"}
	c4Identifier := storage.ColumnPrivIdentifier{ProjectID: "1", TableName: "tb", ColumnName: "data"}
	c5Identifier := storage.ColumnPrivIdentifier{ProjectID: "1", TableName: "tc", ColumnName: "id"}
	c6Identifier := storage.ColumnPrivIdentifier{ProjectID: "1", TableName: "tc", ColumnName: "data"}
	tableNameToOwners := map[string]string{
		"ta": "alice",
		"tb": "bob",
		"tc": "carol",
	}
	for _, identifier := range []storage.ColumnPrivIdentifier{c1Identifier, c2Identifier, c3Identifier, c4Identifier, c5Identifier, c6Identifier} {
		privs := []storage.ColumnPriv{}
		for _, p := range []string{"bob", "alice", "carol"} {
			identifier.DestParty = p
			privs = append(privs, storage.ColumnPriv{ColumnPrivIdentifier: identifier, Priv: "plaintext"})
		}
		s.NoError(common.GrantColumnConstraintsWithCheck(txn, "1", privs, common.OwnerChecker{Owner: tableNameToOwners[identifier.TableName]}))
	}
}

func (s *interTestSuite) SetupTest() {
	s.NoError(s.testAppBuilder.AppAlice.MetaMgr.Bootstrap())
	s.bootstrap(s.testAppBuilder.AppAlice.MetaMgr)

	s.NoError(s.testAppBuilder.AppBob.MetaMgr.Bootstrap())
	s.bootstrap(s.testAppBuilder.AppBob.MetaMgr)

	s.NoError(s.testAppBuilder.AppCarol.MetaMgr.Bootstrap())
	s.bootstrap(s.testAppBuilder.AppCarol.MetaMgr)
}

func (s *interTestSuite) TearDownTest() {
	s.NoError(s.testAppBuilder.AppAlice.MetaMgr.DropTables())
	s.testAppBuilder.AppAlice.Sessions.Flush()

	s.NoError(s.testAppBuilder.AppBob.MetaMgr.DropTables())
	s.testAppBuilder.AppBob.Sessions.Flush()

	s.NoError(s.testAppBuilder.AppCarol.MetaMgr.DropTables())
	s.testAppBuilder.AppCarol.Sessions.Flush()
}

func (s *interTestSuite) TestDistributeQueryErrorQuery() {
	serverAlice := s.testAppBuilder.ServerAlice
	serverCarol := s.testAppBuilder.ServerCarol
	mockReq := &scql.DistributeQueryRequest{
		ClientProtocol: application.Version,
		ClientId:       &scql.PartyId{Code: "alice"},
		EngineEndpoint: s.testAppBuilder.ServerEngineUrl,
		ProjectId:      "1",
		JobId:          "1",
		ClientChecksum: &scql.Checksum{TableSchema: []byte{0x1}, Ccl: []byte{0x1}},
		ServerChecksum: &scql.Checksum{TableSchema: []byte{0x1}, Ccl: []byte{0x1}},
		JobConfig:      &scql.JobConfig{},
		RunningOpts:    &scql.RunningOptions{Batched: true},
	}
	// failed to parse query
	mockReq.Query = "select error query"
	res, err := s.svcBob.DistributeQuery(s.ctx, mockReq)
	s.Error(err)
	s.Equal(err.Error(), "line 1 column 12 near \"error query\" ")
	s.Nil(res)

	serverAlice.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.AskInfoPath {
			var req scql.AskInfoRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req, r.Header.Get("Content-Type"))
			s.NoError(err)
			resp, err := s.svcAlice.AskInfo(context.Background(), &req)
			s.NoError(err)
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})
	serverCarol.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.AskInfoPath {
			var req scql.AskInfoRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req, r.Header.Get("Content-Type"))
			s.NoError(err)
			resp, err := s.svcCarol.AskInfo(context.Background(), &req)
			s.NoError(err)
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})

	// use new job id to avoid conflicting with other tests
	mockReq.JobId = "2"
	// table not found
	mockReq.Query = "select xxx from table_not_found"
	res, err = s.svcBob.DistributeQuery(s.ctx, mockReq)
	s.Error(err)
	s.Equal(err.Error(), "prepareData: table [table_not_found] not found")
	s.Nil(res)
}

func (s *interTestSuite) TestDistributeQueryChecksumNotEqual2P() {
	serverAlice := s.testAppBuilder.ServerAlice
	engine := s.testAppBuilder.ServerEngine
	// prepare
	chBroker := make(chan bool)
	chEngine := make(chan bool)
	serverAlice.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.AskInfoPath {
			chBroker <- true
			var req scql.AskInfoRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req, r.Header.Get("Content-Type"))
			s.NoError(err)
			resp, err := s.svcAlice.AskInfo(context.Background(), &req)
			s.NoError(err)
			s.Equal(2, len(req.ResourceSpecs))
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})
	engine.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/SCQLEngineService/RunExecutionPlan" {
			chEngine <- true
		}
	})
	// error in ClientChecksum and ServerChecksum
	// so ask info must be triggered but engine shouldn't be triggered
	mockReq := &scql.DistributeQueryRequest{
		ClientProtocol: application.Version,
		ClientId:       &scql.PartyId{Code: "alice"},
		EngineEndpoint: s.testAppBuilder.ServerEngineUrl,
		ProjectId:      "1",
		JobId:          "1",
		Query:          "select id from ta join tb on ta.id = tb.id",
		ClientChecksum: &scql.Checksum{TableSchema: []byte{0x1}, Ccl: []byte{0x1}},
		ServerChecksum: &scql.Checksum{TableSchema: []byte{0x1}, Ccl: []byte{0x1}},
		JobConfig:      &scql.JobConfig{},
	}
	// server checksum not equal
	res, err := s.svcBob.DistributeQuery(s.ctx, mockReq)
	s.NoError(err)
	s.Equal(res.Status.Code, int32(scql.Code_DATA_INCONSISTENCY))
	s.Equal(res.ServerChecksumResult, scql.ChecksumCompareResult_TABLE_CCL_NOT_EQUAL)
	// make sure AskInfo has been triggered
	select {
	case <-chBroker:
	case <-time.After(100 * time.Millisecond):
		s.Fail("ask info should be triggered")
	}
	select {
	case <-chEngine:
		s.Fail("engine shouldn't be triggered")
	case <-time.After(100 * time.Millisecond):
	}
}

func (s *interTestSuite) TestDistributeQueryServerChecksumEqual2P() {
	serverAlice := s.testAppBuilder.ServerAlice
	engine := s.testAppBuilder.ServerEngine
	// prepare
	chEngine := make(chan bool)
	serverAlice.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.AskInfoPath {
			s.Fail("ask info shouldn't be triggered")
		}
	})
	engine.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/SCQLEngineService/RunExecutionPlan" {
			chEngine <- true
		}
	})
	mockReq := &scql.DistributeQueryRequest{
		ClientProtocol: application.Version,
		ClientId:       &scql.PartyId{Code: "alice"},
		EngineEndpoint: s.testAppBuilder.ServerEngineUrl,
		ProjectId:      "1",
		JobId:          "1",
		Query:          "select ta.id from ta join tb on ta.id = tb.id",
		ClientChecksum: &scql.Checksum{TableSchema: []byte{0x1}, Ccl: []byte{0x1}},
		ServerChecksum: &scql.Checksum{TableSchema: []byte{0x1}, Ccl: []byte{0x1}},
		JobConfig:      &scql.JobConfig{},
	}
	mockReq.ClientChecksum = &scql.Checksum{
		TableSchema: []byte{157, 69, 36, 57, 100, 26, 130, 58, 54, 50, 8, 177, 99, 131, 55, 216, 224, 17, 47, 117, 60, 139, 238, 187, 128, 75, 250, 27, 233, 99, 20, 182},
		Ccl:         []byte{184, 18, 172, 143, 162, 41, 63, 71, 56, 104, 25, 92, 104, 227, 14, 111, 90, 33, 122, 119, 226, 35, 78, 166, 226, 40, 91, 52, 31, 208, 10, 167},
	}
	res, err := s.svcBob.DistributeQuery(s.ctx, mockReq)
	s.NoError(err)
	s.Equal(res.Status.Code, int32(scql.Code_DATA_INCONSISTENCY))
	select {
	case <-chEngine:
	case <-time.After(100 * time.Millisecond):
		s.Fail("engine should be triggered")
	}
}

func (s *interTestSuite) TestDistributeQueryChecksumEqual3P() {
	serverAlice := s.testAppBuilder.ServerAlice
	serverCarol := s.testAppBuilder.ServerCarol
	engine := s.testAppBuilder.ServerEngine
	// prepare
	checksumAlice := scql.Checksum{
		TableSchema: []byte{157, 69, 36, 57, 100, 26, 130, 58, 54, 50, 8, 177, 99, 131, 55, 216, 224, 17, 47, 117, 60, 139, 238, 187, 128, 75, 250, 27, 233, 99, 20, 182},
		Ccl:         []byte{102, 38, 78, 138, 51, 44, 140, 77, 226, 196, 111, 237, 6, 38, 44, 93, 16, 126, 116, 131, 34, 12, 116, 155, 81, 203, 108, 23, 16, 92, 76, 120},
	}
	checksumBob := scql.Checksum{
		TableSchema: []byte{66, 229, 33, 139, 212, 61, 12, 145, 141, 231, 81, 19, 164, 223, 9, 107, 137, 235, 54, 172, 197, 106, 192, 94, 54, 29, 124, 77, 121, 35, 9, 38},
		Ccl:         []byte{74, 8, 174, 216, 193, 43, 146, 180, 211, 244, 173, 42, 73, 132, 253, 139, 25, 26, 175, 167, 43, 197, 248, 198, 172, 5, 255, 108, 218, 248, 0, 77},
	}
	checksumCarol := scql.Checksum{
		TableSchema: []byte{151, 138, 113, 25, 178, 22, 116, 92, 114, 200, 38, 122, 210, 99, 111, 195, 213, 163, 98, 101, 29, 233, 122, 93, 13, 39, 11, 38, 110, 252, 63, 87},
		Ccl:         []byte{40, 98, 49, 91, 134, 87, 50, 208, 250, 161, 12, 91, 1, 214, 87, 76, 114, 249, 1, 80, 58, 9, 138, 97, 157, 203, 70, 133, 171, 203, 164, 155},
	}
	chEngine := make(chan bool, 1)
	chCarol := make(chan bool, 1)
	serverAlice.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.AskInfoPath {
			s.Fail("ask info shouldn't be triggered")
		}
		if r.URL.Path == constant.ExchangeJobInfoPath {
			s.Fail("exchange job info shouldn't be triggered")
		}
	})
	serverCarol.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.AskInfoPath {
			s.Fail("ask info shouldn't be triggered")
		}
		if r.URL.Path == constant.ExchangeJobInfoPath {
			chCarol <- true
			var req scql.ExchangeJobInfoRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req, r.Header.Get("Content-Type"))
			s.NoError(err)
			info := &application.ExecutionInfo{
				ProjectID: req.ProjectId,
				JobID:     req.JobId,
				Issuer:    &scql.PartyId{Code: "alice"},
				SessionOptions: &application.SessionOptions{
					SessionExpireSeconds: 86400,
				},
			}
			session, err := application.NewSession(context.Background(), info, s.testAppBuilder.AppCarol, false, false)
			if err != nil {
				return
			}
			s.testAppBuilder.AppCarol.AddSession(req.JobId, session)
			session.ExecuteInfo.Checksums.SaveLocal("bob", application.NewChecksumFromProto(&checksumBob))
			session.ExecuteInfo.Checksums.SaveLocal("carol", application.NewChecksumFromProto(&checksumCarol))
			session.ExecuteInfo.DataParties = []string{"alice", "bob", "carol"}
			session.ExecuteInfo.WorkParties = []string{"alice", "bob", "carol"}
			s.testAppBuilder.AppCarol.PersistSessionInfo(session)
			resp, err := s.svcCarol.ExchangeJobInfo(context.Background(), &req)
			s.NoError(err)
			s.Equal(scql.ChecksumCompareResult_EQUAL, resp.ServerChecksumResult)
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})
	engine.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/SCQLEngineService/RunExecutionPlan" {
			chEngine <- true
		}
	})
	// exchange job info then run execution plan
	mockReq := &scql.DistributeQueryRequest{
		ClientProtocol: application.Version,
		ClientId:       &scql.PartyId{Code: "alice"},
		EngineEndpoint: s.testAppBuilder.ServerEngineUrl,
		ProjectId:      "1",
		JobId:          "1",
		Query:          "select ta.id from ta join tb on ta.id = tb.id join tc on tc.id = ta.id",
		ClientChecksum: &scql.Checksum{TableSchema: []byte{0x1}, Ccl: []byte{0x1}},
		ServerChecksum: &scql.Checksum{TableSchema: []byte{0x1}, Ccl: []byte{0x1}},
		JobConfig:      &scql.JobConfig{},
	}
	// server checksum equal
	mockReq.JobId = "2"
	mockReq.ClientChecksum = &checksumAlice
	mockReq.ServerChecksum = &checksumBob
	res, err := s.svcBob.DistributeQuery(s.ctx, mockReq)
	s.NoError(err)
	s.Equal(res.Status.Code, int32(scql.Code_OK))
	select {
	case <-chEngine:
	case <-time.After(500 * time.Millisecond):
		s.Fail("engine should be triggered")
	}
	select {
	case <-chCarol:
	case <-time.After(100 * time.Millisecond):
		s.Fail("carol exchange job info should be triggered")
	}
}

func (s *interTestSuite) TestDistributeQueryOtherPartyChecksumNotEqual3P() {
	serverAlice := s.testAppBuilder.ServerAlice
	serverCarol := s.testAppBuilder.ServerCarol
	engine := s.testAppBuilder.ServerEngine
	// prepare
	checksumAlice := scql.Checksum{
		TableSchema: []byte{157, 69, 36, 57, 100, 26, 130, 58, 54, 50, 8, 177, 99, 131, 55, 216, 224, 17, 47, 117, 60, 139, 238, 187, 128, 75, 250, 27, 233, 99, 20, 182},
		Ccl:         []byte{102, 38, 78, 138, 51, 44, 140, 77, 226, 196, 111, 237, 6, 38, 44, 93, 16, 126, 116, 131, 34, 12, 116, 155, 81, 203, 108, 23, 16, 92, 76, 120},
	}
	checksumBob := scql.Checksum{
		TableSchema: []byte{66, 229, 33, 139, 212, 61, 12, 145, 141, 231, 81, 19, 164, 223, 9, 107, 137, 235, 54, 172, 197, 106, 192, 94, 54, 29, 124, 77, 121, 35, 9, 38},
		Ccl:         []byte{74, 8, 174, 216, 193, 43, 146, 180, 211, 244, 173, 42, 73, 132, 253, 139, 25, 26, 175, 167, 43, 197, 248, 198, 172, 5, 255, 108, 218, 248, 0, 77},
	}
	checksumCarol := scql.Checksum{
		TableSchema: []byte{151, 138, 113, 25, 178, 22, 116, 92, 114, 200, 38, 122, 210, 99, 111, 195, 213, 163, 98, 101, 29, 233, 122, 93, 13, 39, 11, 38, 110, 252, 63, 87},
		Ccl:         []byte{40, 98, 49, 91, 134, 87, 50, 208, 250, 161, 12, 91, 1, 214, 87, 76, 114, 249, 1, 80, 58, 9, 138, 97, 157, 203, 70, 133, 171, 203, 164, 155},
	}
	chEngine := make(chan bool, 1)
	chCarolAskInfo := make(chan bool, 1)
	chCarolExchange := make(chan bool, 1)
	serverAlice.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.AskInfoPath {
			s.Fail("ask info shouldn't be triggered")
		}
		if r.URL.Path == constant.ExchangeJobInfoPath {
			s.Fail("exchange job info shouldn't be triggered")
		}
	})
	serverCarol.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.AskInfoPath {
			chCarolAskInfo <- true
			var req scql.AskInfoRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req, r.Header.Get("Content-Type"))
			s.NoError(err)
			s.Equal(1, len(req.ResourceSpecs))
			resp, err := s.svcCarol.AskInfo(context.Background(), &req)
			s.NoError(err)
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
		if r.URL.Path == constant.ExchangeJobInfoPath {
			chCarolExchange <- true
			var req scql.ExchangeJobInfoRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req, r.Header.Get("Content-Type"))
			s.NoError(err)
			info := &application.ExecutionInfo{
				ProjectID: req.ProjectId,
				JobID:     req.JobId,
				Issuer:    &scql.PartyId{Code: "alice"},
				SessionOptions: &application.SessionOptions{
					SessionExpireSeconds: 86400,
				},
			}
			session, err := application.NewSession(context.Background(), info, s.testAppBuilder.AppCarol, false, false)
			if err != nil {
				return
			}
			s.testAppBuilder.AppCarol.AddSession(req.JobId, session)
			session.ExecuteInfo.Checksums.SaveLocal("bob", application.NewChecksumFromProto(&checksumBob))
			session.ExecuteInfo.Checksums.SaveLocal("carol", application.NewChecksumFromProto(&checksumCarol))
			session.ExecuteInfo.DataParties = []string{"bob", "carol", "alice"}
			s.testAppBuilder.AppCarol.PersistSessionInfo(session)
			resp, err := s.svcCarol.ExchangeJobInfo(context.Background(), &req)
			s.NoError(err)
			// mock error to trigger ask info
			resp.ServerChecksumResult = scql.ChecksumCompareResult_CCL_NOT_EQUAL
			resp.Status.Code = int32(scql.Code_DATA_INCONSISTENCY)
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})
	engine.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/SCQLEngineService/RunExecutionPlan" {
			chEngine <- true
		}
	})
	mockReq := &scql.DistributeQueryRequest{
		ClientProtocol: application.Version,
		ClientId:       &scql.PartyId{Code: "alice"},
		EngineEndpoint: s.testAppBuilder.ServerEngineUrl,
		ProjectId:      "1",
		JobId:          "1",
		Query:          "select ta.id from ta join tb on ta.id = tb.id join tc on tc.id = ta.id",
		ClientChecksum: &scql.Checksum{TableSchema: []byte{0x1}, Ccl: []byte{0x1}},
		ServerChecksum: &scql.Checksum{TableSchema: []byte{0x1}, Ccl: []byte{0x1}},
	}
	mockReq.ClientChecksum = &checksumAlice
	mockReq.ServerChecksum = &checksumBob
	res, err := s.svcBob.DistributeQuery(s.ctx, mockReq)
	s.NoError(err)
	s.Equal(res.Status.Code, int32(scql.Code_OK))
	select {
	case <-chEngine:
	case <-time.After(500 * time.Millisecond):
		s.Fail("engine should be triggered")
	}
	select {
	case <-chCarolExchange:
	case <-time.After(100 * time.Millisecond):
		s.Fail("carol exchange job info should be triggered")
	}
	select {
	case <-chCarolAskInfo:
	case <-time.After(100 * time.Millisecond):
		s.Fail("carol ask info should be triggered")
	}
}

func (s *interTestSuite) TearDownSuite() {
	s.testAppBuilder.ServerAlice.Close()
	s.testAppBuilder.ServerBob.Close()
	s.testAppBuilder.ServerCarol.Close()
	s.testAppBuilder.ServerEngine.Close()
}
