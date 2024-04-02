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
	"encoding/base64"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/pkg/broker/config"
	"github.com/secretflow/scql/pkg/broker/partymgr"
	"github.com/secretflow/scql/pkg/broker/scheduler"
	"github.com/secretflow/scql/pkg/broker/services/auth"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/executor"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/kusciaclient"
	"github.com/secretflow/scql/pkg/util/message"
)

type App struct {
	Sessions     *cache.Cache
	PartyMgr     partymgr.PartyMgr
	MetaMgr      *storage.MetaManager
	Conf         *config.Config
	Auth         *auth.Auth
	EngineClient executor.EngineClient
	InterStub    *InterStub

	Scheduler scheduler.EngineScheduler
}

func NewApp(partyMgr partymgr.PartyMgr, metaMgr *storage.MetaManager, cfg *config.Config) (*App, error) {
	var pemData []byte
	if cfg.PrivateKeyPath != "" {
		data, err := os.ReadFile(cfg.PrivateKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read private key file %s: %w", cfg.PrivateKeyPath, err)
		}
		pemData = data
	} else if cfg.PrivateKeyData != "" {
		data, err := base64.StdEncoding.DecodeString(cfg.PrivateKeyData)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64 encoded private key data: %w", err)
		}
		pemData = data
	} else {
		return nil, fmt.Errorf("private key path and content are both empty, provide at least one")
	}
	auth, err := auth.NewAuth(pemData)
	if err != nil {
		return nil, fmt.Errorf("create auth from private key: %v", err)
	}

	app := &App{
		Sessions: cache.New(time.Duration(cfg.SessionExpireTime),
			time.Duration(cfg.SessionCheckInterval)),
		PartyMgr:     partyMgr,
		MetaMgr:      metaMgr,
		Conf:         cfg,
		Auth:         auth,
		EngineClient: executor.NewEngineClient(cfg.Engine.ClientTimeout),
		InterStub: &InterStub{
			Timeout:      cfg.InterTimeout,
			EncodingType: message.EncodingTypeProtobuf,
			Auth:         auth,
		},
	}

	switch strings.ToLower(cfg.Engine.Scheduler) {
	case "kuscia":
		kusciaSchedulerOpt := cfg.Engine.KusciaSchedulerOption

		conn, err := kusciaclient.NewKusciaClientConn(kusciaSchedulerOpt.Endpoint, kusciaSchedulerOpt.TLSMode, kusciaSchedulerOpt.Cert, kusciaSchedulerOpt.Key, kusciaSchedulerOpt.CaCert, kusciaSchedulerOpt.Token)
		if err != nil {
			return nil, fmt.Errorf("failed to create kuscia client conn: %v", err)
		}

		scheduler, err := scheduler.NewKusciaJobScheduler(conn, cfg.PartyCode,
			scheduler.WithMaxPollTimes(kusciaSchedulerOpt.MaxPollTimes),
			scheduler.WithMaxWaitTime(kusciaSchedulerOpt.MaxWaitTime),
			scheduler.WithPollInterval(kusciaSchedulerOpt.PollInterval),
			scheduler.WithKeepJobAliveForDebug(kusciaSchedulerOpt.KeepJobAliveForDebug))
		if err != nil {
			return nil, fmt.Errorf("failed to create kuscia job scheduler: %v", err)
		}
		app.Scheduler = scheduler
	case "", "naive":
		scheduler, err := scheduler.NewNaiveScheduler(cfg.Engine.Uris)
		if err != nil {
			return nil, fmt.Errorf("failed to create naive scheduler: %v", err)
		}
		app.Scheduler = scheduler
	default:
		return nil, fmt.Errorf("unsupported engine scheduler %s", cfg.Engine.Scheduler)
	}

	if cfg.PersistSession {
		go app.StorageGc()
		go app.SessionGc()
	}

	return app, nil
}

func (app *App) GetSession(sid string) (*Session, bool) {
	value, exists := app.Sessions.Get(sid)
	if !exists {
		return nil, false
	}
	if result, ok := value.(*Session); ok {
		return result, exists
	}
	return nil, false
}

func (app *App) DeleteSession(sid string) {
	app.Sessions.Delete(sid)
}

func (app *App) AddSession(sid string, session *Session) {
	app.Sessions.SetDefault(sid, session)
}

func (app *App) PersistSessionInfo(session *Session) error {
	if !app.Conf.PersistSession {
		return nil
	}

	info, err := sessionInfo(session)
	if err != nil {
		return fmt.Errorf("PersistSessionInfo: SessionInfo failed: %v", err)
	}
	return app.MetaMgr.SetSessionInfo(*info)
}

func (app *App) PersistSessionResult(sid string, resp *pb.QueryResponse) error {
	if !app.Conf.PersistSession {
		return nil
	}

	ser, err := protojson.Marshal(resp)
	if err != nil {
		return fmt.Errorf("PersistSessionResult: marshal resp failed: %v", err)
	}
	return app.MetaMgr.SetSessionResult(storage.SessionResult{
		SessionID: sid,
		Result:    ser,
	})
}

func (app *App) GetSessionInfo(sid string) (*storage.SessionInfo, error) {
	if app.Conf.PersistSession {
		// TODO: possible optimization: check memory before checking DB
		info, err := app.MetaMgr.GetSessionInfo(sid)
		if err != nil {
			return nil, err
		}
		return &info, nil
	}

	session, ok := app.GetSession(sid)
	if !ok {
		return nil, fmt.Errorf("session{%s} not found", sid)
	}
	return sessionInfo(session)
}

func (app *App) GetSessionResult(sid string) (*pb.QueryResponse, error) {
	var result *pb.QueryResponse
	session, ok := app.GetSession(sid)
	if ok {
		result = session.GetResultSafely()
	}

	if result == nil && app.Conf.PersistSession {
		res, err := app.MetaMgr.GetSessionResult(sid)
		if err == nil {
			var response pb.QueryResponse
			err = protojson.Unmarshal(res.Result, &response)
			if err != nil {
				return nil, fmt.Errorf("unmarshal session result failed: %v", err)
			}
			result = &response
		}
	}

	return result, nil
}

// cancel session to release memory and engine resource
func (app *App) CancelSession(info *storage.SessionInfo) error {
	// release local engine resource
	engine, err := app.Scheduler.ParseEngineInstance(info.JobInfo)
	if err != nil {
		return fmt.Errorf("failed to get engine from info: %v", err)
	}
	if err := engine.Stop(); err != nil {
		logrus.Warnf("failed to stop query job: %v", err)
	}

	// release memory
	_, ok := app.GetSession(info.SessionID)
	if ok {
		app.DeleteSession(info.SessionID)
	}

	if app.Conf.PersistSession {
		// NOTE: only clear SessionResult, the SessionInfo is not cleared currently
		err := app.MetaMgr.ClearSessionResult(info.SessionID)
		if err != nil {
			return fmt.Errorf("CancelSession: ClearSessionResult failed: %v", err)
		}
	}
	return nil
}

func sessionInfo(session *Session) (info *storage.SessionInfo, err error) {
	if session == nil {
		err = fmt.Errorf("session is nil")
		return
	}
	var checksum Checksum
	if slices.Contains(session.ExecuteInfo.DataParties, session.GetSelfPartyCode()) {
		checksum, err = session.ExecuteInfo.Checksums.GetLocal(session.GetSelfPartyCode())
		if err != nil {
			err = fmt.Errorf("PersistSessionInfo: get local self checksum failed: %v", err)
			return
		}
	}
	var jobInfo []byte
	engineUrl := ""
	if !session.DryRun {
		jobInfo, err = session.Engine.MarshalToText()
		if err != nil {
			err = fmt.Errorf("PersistSessionInfo: get job info failed: %v", err)
			return
		}
		engineUrl = session.Engine.GetEndpointForPeer()
	}

	warn, err := protojson.Marshal(session.Warning)
	if err != nil {
		err = fmt.Errorf("PersistSessionInfo: marsha warning failed: %v", err)
		return
	}
	return &storage.SessionInfo{
		SessionID:     session.ExecuteInfo.JobID,
		Status:        0,
		TableChecksum: checksum.TableSchema,
		CCLChecksum:   checksum.CCL,
		EngineUrl:     engineUrl,
		JobInfo:       jobInfo,
		WorkParties:   strings.Join(session.ExecuteInfo.WorkParties, ";"),
		OutputNames:   strings.Join(session.OutputNames, ";"),
		Warning:       warn,
		CreatedAt:     session.CreatedAt,
	}, nil
}
