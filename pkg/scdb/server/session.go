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
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/executor"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser/auth"
	"github.com/secretflow/scql/pkg/privilege"
	"github.com/secretflow/scql/pkg/privilege/privileges"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/sessionctx/variable"
)

const (
	DefaultHostName = `%`
)

func generateSessionID() (string, error) {
	id, err := uuid.NewUUID()
	if err == nil {
		return fmt.Sprint(id), nil
	}
	return "", err
}

type session struct {
	id               string
	createdAt        time.Time
	createdBy        string
	request          *scql.SCDBQueryRequest
	queryResultCbURL string
	isDQLRequest     bool
	sessionVars      *variable.SessionVars
	values           map[fmt.Stringer]interface{}
	result           *scql.SCDBQueryResultResponse
	ctx              context.Context

	executor *executor.Executor

	// all sessions have independent stub to avoid concurrent problems
	engineStub *executor.EngineStub
	partyInfo  *graph.PartyInfo
	parties    []*scql.JobStartParams_Party
}

// SetValue implements sessionctx.Context SetValue interface.
func (s *session) SetValue(key fmt.Stringer, value interface{}) {
	s.values[key] = value
}

// Value implements sessionctx.Context Value interface.
func (s *session) Value(key fmt.Stringer) interface{} {
	value := s.values[key]
	return value
}

// GetSessionVars implements the context.Context interface.
func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

func newSession(ctx context.Context, req *scql.SCDBQueryRequest, s *gorm.DB) (*session, error) {
	sessionId, err := generateSessionID()
	if nil != err {
		return nil, err
	}

	session := &session{
		id:               sessionId,
		createdAt:        time.Now(),
		createdBy:        req.GetUser().GetUser().GetNativeUser().GetName(),
		request:          req,
		queryResultCbURL: req.QueryResultCallbackUrl,
		sessionVars:      variable.NewSessionVars(),
		values:           make(map[fmt.Stringer]interface{}),
		ctx:              ctx,
	}

	session.GetSessionVars().Storage = s
	session.GetSessionVars().StmtCtx = &stmtctx.StatementContext{}
	session.GetSessionVars().CurrentDB = req.DbName

	return session, nil
}

func (s *session) authenticateUser(user *scql.SCDBCredential) error {
	pm := &privileges.UserPrivileges{
		Handle: privileges.NewHandle(s),
	}

	userName := user.GetUser().GetNativeUser().Name
	password := user.GetUser().GetNativeUser().Password
	if userName == "" || password == "" {
		return fmt.Errorf("authentication failed due to empty userName or password")
	}
	if _, _, ok := pm.ConnectionVerification(userName, DefaultHostName, password, nil, nil); !ok {
		return fmt.Errorf("user %s authentication failed", userName)
	}
	privilege.BindPrivilegeManager(s, pm)

	s.GetSessionVars().User = &auth.UserIdentity{Username: userName, Hostname: DefaultHostName}

	return nil
}

func (s *session) setResultWithOutputColumnsAndAffectedRows(columns []*scql.Tensor, affectedRows int64) {
	s.result = &scql.SCDBQueryResultResponse{
		Status: &scql.Status{
			Code: int32(scql.Code_OK),
		},
		OutColumns:    columns,
		ScdbSessionId: s.id,
		AffectedRows:  affectedRows,
	}
	if s.GetSessionVars().AffectedByGroupThreshold {
		reason := fmt.Sprintf("for safety, we filter the results for groups which contain less than %d items.", s.GetSessionVars().GroupByThreshold)
		logrus.Infof("%v", reason)
		s.result.Warnings = append(s.result.Warnings, &scql.SQLWarning{Reason: reason})
	}
}

func (sc *session) fillPartyInfo(enginesInfo *graph.EnginesInfo) {
	sc.partyInfo = enginesInfo.GetPartyInfo()
	for i, participant := range sc.partyInfo.GetParticipants() {
		sc.parties = append(
			sc.parties, &scql.JobStartParams_Party{
				Code:      participant.PartyCode,
				Name:      participant.PartyCode,
				Rank:      int32(i),
				Host:      participant.Endpoints[0],
				PublicKey: participant.PubKey,
			})
	}
}
