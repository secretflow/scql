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

package application

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/pkg/broker/config"
	"github.com/secretflow/scql/pkg/broker/partymgr"
	"github.com/secretflow/scql/pkg/broker/storage"
	exe "github.com/secretflow/scql/pkg/executor"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/sessionctx/variable"
)

const (
	Version pb.BrokerProtocolVersion = 1
)

func GenerateJobID() (string, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// remove hyphens due to project id may work as db name.
func GenerateProjectID() (string, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	return strings.ReplaceAll(fmt.Sprintf("proj_%s", id.String()), "-", ""), nil
}

// session stores information that is unrelated to the query.
type Session struct {
	CreatedAt   time.Time
	SessionVars *variable.SessionVars
	Ctx         context.Context

	// all sessions have independent stub to avoid concurrent problems
	MetaMgr     *storage.MetaManager
	PartyMgr    partymgr.PartyMgr
	ExecuteInfo *ExecutionInfo
	Conf        *config.Config

	// current broker host for local engine to call back
	CallBackHost string

	Values map[fmt.Stringer]interface{}

	AsyncMode   bool
	OutputNames []string
	// mu protects the Result when running in async mode
	mu     sync.Mutex
	Result *pb.QueryResponse
}

func (s *Session) GetResultSafely() *pb.QueryResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Result
}

func (s *Session) SetResultSafely(result *pb.QueryResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Result = result
}

func (s *Session) GetSelfPartyCode() string {
	return s.Conf.PartyCode
}

func (s *Session) GetOneSelfEngineUriForPeer() string {
	return s.Conf.Engine.Uris[0].ForPeer
}

func (s *Session) GetOneSelfEngineUriForSelf() string {
	engineUri := s.Conf.Engine.Uris[0]
	if engineUri.ForSelf == "" {
		logrus.Info("GetOneSelfEngineUriForSelf: not set for_self uri, using for_peer by default")
		return engineUri.ForPeer
	}
	return engineUri.ForSelf
}

type ExecutionInfo struct {
	Issuer        *pb.PartyId
	ProjectID     string
	JobID         string
	Query         string
	SpuRuntimeCfg *spu.RuntimeConfig
	// map from party to engine endpoint
	EngineEndpoints sync.Map
	EngineClient    exe.EngineClient
	InterStub       *InterStub
	// sorted parties who's data has been used in current session
	DataParties []string
	// sorted parties who work in current session
	WorkParties []string

	Checksums ChecksumStorage
}

func (e *ExecutionInfo) CheckProjectConf() error {
	if e.SpuRuntimeCfg == nil {
		return fmt.Errorf("spu runtime config is nil")
	}
	partyNum := len(e.WorkParties)
	if partyNum == 0 {
		return fmt.Errorf("work parties is empty")
	}
	if partyNum == 1 {
		return nil
	}
	if e.SpuRuntimeCfg.Protocol == spu.ProtocolKind_CHEETAH && partyNum != 2 {
		return fmt.Errorf("protocol cheetah only support 2 parties, but got %d", partyNum)
	}
	if e.SpuRuntimeCfg.Protocol == spu.ProtocolKind_ABY3 && partyNum != 3 {
		return fmt.Errorf("protocol cheetah only support 3 parties, but got %d", partyNum)
	}
	return nil
}

func NewSession(ctx context.Context, info *ExecutionInfo, app *App, asyncMode bool) (session *Session, err error) {
	session = &Session{
		CreatedAt:    time.Now(),
		ExecuteInfo:  info,
		SessionVars:  variable.NewSessionVars(),
		Ctx:          ctx,
		MetaMgr:      app.MetaMgr,
		PartyMgr:     app.PartyMgr,
		Conf:         app.Conf,
		CallBackHost: app.Conf.IntraHost,
		AsyncMode:    asyncMode,
	}
	// set self engine endpoint work in current session
	session.SaveEndpoint(session.GetSelfPartyCode(), session.GetOneSelfEngineUriForPeer())
	session.ExecuteInfo.InterStub = app.InterStub
	session.GetSessionVars().StmtCtx = &stmtctx.StatementContext{}
	// use project id as default db name
	session.GetSessionVars().CurrentDB = info.ProjectID

	txn := session.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()
	project, err := txn.GetProject(info.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("NewSession: project %v not exists", project.ID)
	}
	session.ExecuteInfo.SpuRuntimeCfg = &spu.RuntimeConfig{}
	err = protojson.Unmarshal([]byte(project.ProjectConf.SpuConf), info.SpuRuntimeCfg)
	if err != nil {
		return
	}
	return
}

// GetSessionVars implements the context.Context interface.
func (s *Session) GetSessionVars() *variable.SessionVars {
	return s.SessionVars
}

// SetValue implements sessionctx.Context SetValue interface.
func (s *Session) SetValue(key fmt.Stringer, value interface{}) {
	s.Values[key] = value
}

// Value implements sessionctx.Context Value interface.
func (s *Session) Value(key fmt.Stringer) interface{} {
	value := s.Values[key]
	return value
}

func (s *Session) GetSelfChecksum() (Checksum, error) {
	selfCode := s.GetSelfPartyCode()
	return s.ExecuteInfo.Checksums.GetLocal(selfCode)
}

func (s *Session) GetLocalChecksum(partyCode string) (Checksum, error) {
	return s.ExecuteInfo.Checksums.GetLocal(partyCode)
}

func (s *Session) GetRemoteChecksum(partyCode string) (Checksum, error) {
	return s.ExecuteInfo.Checksums.GetRemote(partyCode)
}

func (s *Session) SaveLocalChecksum(partyCode string, sum Checksum) error {
	return s.ExecuteInfo.Checksums.SaveLocal(partyCode, sum)
}

func (s *Session) SaveRemoteChecksum(partyCode string, pbChecksum *pb.Checksum) error {
	return s.ExecuteInfo.Checksums.SaveRemote(partyCode, pbChecksum)
}

func (s *Session) GetEndpoint(partyCode string) (string, error) {
	value, ok := s.ExecuteInfo.EngineEndpoints.Load(partyCode)
	if !ok {
		return "", fmt.Errorf("endpoint of %s not found", partyCode)
	}
	endpoint, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("failed to parse checksum from %+v", value)
	}
	return endpoint, nil
}

func (s *Session) SaveEndpoint(partyCode, endpoint string) {
	logrus.Infof("add endpoint %s for party %s", endpoint, partyCode)
	s.ExecuteInfo.EngineEndpoints.Store(partyCode, endpoint)
}

func (s *Session) IsIssuer() bool {
	if s.ExecuteInfo.Issuer.Code == s.GetSelfPartyCode() {
		return true
	}
	return false
}
