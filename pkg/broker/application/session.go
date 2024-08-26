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

	"github.com/secretflow/scql/pkg/broker/scheduler"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/constant"
	exe "github.com/secretflow/scql/pkg/executor"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/sessionctx/variable"
	"github.com/secretflow/scql/pkg/util/message"
	"github.com/secretflow/scql/pkg/util/sliceutil"
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
	App         *App
	// all sessions have independent stub to avoid concurrent problems
	ExecuteInfo *ExecutionInfo
	Engine      scheduler.EngineInstance
	// current broker host for local engine to call back
	CallBackHost string

	Values map[fmt.Stringer]interface{}

	DryRun bool

	AsyncMode   bool
	OutputNames []string
	Warning     *pb.Warning
	// mu protects the Result when running in async mode
	mu            sync.Mutex
	Result        *pb.QueryResponse
	ExpireSeconds int64
}

func (s *Session) GetResultSafely() *pb.QueryResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Result
}

func (s *Session) SetResultSafely(result *pb.QueryResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Result == nil {
		s.Result = result
	} else {
		logrus.Warnf("session %s already has result, ignore new one", s.ExecuteInfo.JobID)
	}
}

func (s *Session) GetSelfPartyCode() string {
	return s.App.Conf.PartyCode
}

// OnError is called when query job failed
func (s *Session) OnError(err error) {
	logrus.Warnf("error occurred during executing query job %s: %v", s.ExecuteInfo.JobID, err)

	if s.Engine != nil {
		if err := s.Engine.Stop(); err != nil {
			logrus.Warnf("failed to stop engine on query job %s: %v", s.ExecuteInfo.JobID, err)
		}
	}
	// TODO: add more actions on error
}

// OnSuccess is called when query job success
func (s *Session) OnSuccess() {
	if s.Engine != nil {
		if err := s.Engine.Stop(); err != nil {
			logrus.Warnf("failed to stop engine on query job %s: %v", s.ExecuteInfo.JobID, err)
		}
	}
}

type SessionOptions struct {
	SessionExpireSeconds int64
	LinkConfig           *pb.LinkConfig
	PsiConfig            *pb.PsiConfig
	LogConfig            *pb.LogConfig
	TimeZone             string
}

type ExecutionInfo struct {
	Issuer    *pb.PartyId
	ProjectID string
	JobID     string
	Query     string
	// map from party to engine endpoint
	EngineEndpoints sync.Map
	EngineClient    exe.EngineClient
	InterStub       *InterStub
	// sorted parties who's data has been used in current session
	DataParties []string
	// sorted parties who work in current session
	WorkParties []string

	Checksums ChecksumStorage

	CompileOpts *pb.CompileOptions
	// for debug
	DebugOpts *pb.DebugOptions

	SessionOptions *SessionOptions
}

func (e *ExecutionInfo) CheckProjectConf() error {
	spuConf := e.CompileOpts.SpuConf
	if spuConf == nil {
		return fmt.Errorf("spu runtime config is nil")
	}
	partyNum := len(e.WorkParties)
	if partyNum == 0 {
		return fmt.Errorf("work parties is empty")
	}
	if partyNum == 1 {
		return nil
	}
	if spuConf.Protocol == spu.ProtocolKind_CHEETAH && partyNum != 2 {
		return fmt.Errorf("protocol cheetah only support 2 parties, but got %d", partyNum)
	}
	if spuConf.Protocol == spu.ProtocolKind_ABY3 && partyNum != 3 {
		return fmt.Errorf("protocol aby3 only support 3 parties, but got %d", partyNum)
	}
	return nil
}

func NewSession(ctx context.Context, info *ExecutionInfo, app *App, asyncMode, dryRun bool) (session *Session, err error) {
	spuConf := &spu.RuntimeConfig{}
	var projConf pb.ProjectConfig
	err = app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
		project, err := txn.GetProject(info.ProjectID)
		if err != nil {
			return fmt.Errorf("NewSession: project %v not exists", project.ID)
		}
		err = message.ProtoUnmarshal([]byte(project.ProjectConf), &projConf)
		if err != nil {
			return fmt.Errorf("NewSession: failed to deserialize project config stored in db: %v", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	revealGroupMark := app.Conf.SecurityCompromise.RevealGroupMark
	if projConf.RevealGroupMark != nil {
		revealGroupMark = projConf.GetRevealGroupMark()
	}
	groupByThreshold := app.Conf.SecurityCompromise.GroupByThreshold
	if projConf.GroupByThreshold != nil {
		groupByThreshold = projConf.GetGroupByThreshold()
	}
	if groupByThreshold == 0 {
		groupByThreshold = constant.DefaultGroupByThreshold
	}
	spuConf = projConf.SpuRuntimeCfg
	info.CompileOpts = &pb.CompileOptions{
		SpuConf: spuConf,
		SecurityCompromise: &pb.SecurityCompromiseConfig{
			RevealGroupMark:  revealGroupMark,
			GroupByThreshold: groupByThreshold,
		},
		DumpExeGraph: true,
		OptimizerHints: &pb.OptimizerHints{
			PsiAlgorithmType: pb.PsiAlgorithmType_AUTO,
		},
		Batched: app.Conf.Batched,
	}
	// NOTE(xiaoyuan): use ecdh psi when EnablePsiDetailLog is true
	if info.DebugOpts != nil && info.DebugOpts.EnablePsiDetailLog {
		info.CompileOpts.OptimizerHints.PsiAlgorithmType = pb.PsiAlgorithmType_ECDH
	}
	session = &Session{
		CreatedAt:     time.Now(),
		ExecuteInfo:   info,
		SessionVars:   variable.NewSessionVars(),
		Ctx:           ctx,
		App:           app,
		CallBackHost:  app.Conf.IntraHost,
		AsyncMode:     asyncMode,
		DryRun:        dryRun,
		ExpireSeconds: info.SessionOptions.SessionExpireSeconds,
	}

	session.ExecuteInfo.InterStub = app.InterStub
	session.GetSessionVars().StmtCtx = &stmtctx.StatementContext{}
	// use project id as default db name
	session.GetSessionVars().CurrentDB = info.ProjectID

	if !dryRun {
		eng, err := app.Scheduler.Schedule(info.JobID)
		if err != nil {
			return nil, fmt.Errorf("failed to schedule scqlengine for job %v: %v", info.JobID, err)
		}
		session.Engine = eng
		// set self engine endpoint work in current session
		session.SaveEndpoint(session.GetSelfPartyCode(), eng.GetEndpointForSelf())
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

// CheckChecksum checks data consistency with other parties via comparing checksum
func (s *Session) CheckChecksum() error {
	for _, p := range sliceutil.Subtraction(s.ExecuteInfo.DataParties, []string{s.GetSelfPartyCode()}) {
		compareResult, err := s.ExecuteInfo.Checksums.CompareChecksumFor(p)
		if err != nil {
			return err
		}
		if compareResult != pb.ChecksumCompareResult_EQUAL {
			return fmt.Errorf("checksum not equal with party %s", p)
		}
	}
	return nil
}

func (s *Session) GetEndpoint(partyCode string) (string, error) {
	value, ok := s.ExecuteInfo.EngineEndpoints.Load(partyCode)
	if !ok {
		return "", fmt.Errorf("endpoint of %s not found", partyCode)
	}
	endpoint, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("failed to parse endpoint from %+v of party %s", value, partyCode)
	}
	return endpoint, nil
}

func (s *Session) SaveEndpoint(partyCode, endpoint string) {
	logrus.Infof("add endpoint %s for party %s", endpoint, partyCode)
	s.ExecuteInfo.EngineEndpoints.Store(partyCode, endpoint)
}

func (s *Session) IsIssuer() bool {
	return s.ExecuteInfo.Issuer.Code == s.GetSelfPartyCode()
}
