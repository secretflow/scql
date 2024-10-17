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
	"slices"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"

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

	JobWatcher *executor.JobWatcher
}

func NewApp(partyMgr partymgr.PartyMgr, metaMgr *storage.MetaManager, cfg *config.Config) (*App, error) {
	auth, err := auth.NewAuth(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create auth from config: %v", err)
	}

	app := &App{
		Sessions: cache.New(time.Duration(cfg.SessionExpireTime),
			time.Duration(cfg.SessionCheckInterval)),
		PartyMgr: partyMgr,
		MetaMgr:  metaMgr,
		Conf:     cfg,
		Auth:     auth,
		EngineClient: executor.NewEngineClient(
			cfg.Engine.ClientMode,
			cfg.Engine.ClientTimeout,
			&cfg.Engine.TLSCfg,
			cfg.Engine.ContentType,
			cfg.Engine.Protocol,
		),
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
		scheduler, err := scheduler.NewNaiveScheduler(cfg.Engine)
		if err != nil {
			return nil, fmt.Errorf("failed to create naive scheduler: %v", err)
		}
		app.Scheduler = scheduler
	default:
		return nil, fmt.Errorf("unsupported engine scheduler %s", cfg.Engine.Scheduler)
	}

	jobWatcher, err := executor.NewJobWatcher(app.MetaMgr, app.Conf.PartyCode, func(jobId, url, reason string) {
		app.onJobDead(jobId, url, reason)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create jobwatcher: %v", err)
	}

	app.JobWatcher = jobWatcher

	if cfg.PersistSession {
		err := app.startGc()
		if err != nil {
			return nil, fmt.Errorf("failed to start gc tasks: %v", err)
		}
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

func (app *App) onJobDead(jobId, url, reason string) {
	session, ok := app.GetSession(jobId)
	if !ok {
		logrus.Warnf("App.onJobDead warning: session not found for job id %s, job may come from other broker", jobId)
		logrus.Warnf("error occurred during executing query job %s: %v", jobId, reason)
		err := stopExternalEngine(jobId, url)
		if err != nil {
			logrus.Warnf("failed to stop engine on query job %s: %v", jobId, err)
		} else {
			logrus.Infof("success to stop engine on query job %s", jobId)
		}
	} else {
		result := &pb.QueryResponse{
			Status: &pb.Status{
				Code:    int32(pb.Code_INTERNAL),
				Message: reason,
			},
		}
		session.SetResultSafely(result)
		session.OnError(fmt.Errorf(reason))
	}
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
	// The job has only been submitted, it has not yet started to be executed by the engine.
	info.Status = int8(storage.SessionSubmitted)

	return app.MetaMgr.SetSessionInfo(*info)
}

func (app *App) PersistSessionResult(sid string, resp *pb.QueryResponse, expiredAt time.Time) error {
	if !app.Conf.PersistSession {
		return nil
	}

	ser, err := message.ProtoMarshal(resp)
	if err != nil {
		return fmt.Errorf("PersistSessionResult: marshal resp failed: %v", err)
	}
	return app.MetaMgr.SetSessionResult(storage.SessionResult{
		SessionID: sid,
		Result:    ser,
		ExpiredAt: expiredAt,
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
			err = message.ProtoUnmarshal(res.Result, &response)
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

func (app *App) startGc() error {
	locker, err := storage.NewDistributedLocker(app.MetaMgr, storage.GcLockID, app.Conf.PartyCode, app.Conf.SessionCheckInterval)
	if err != nil {
		return fmt.Errorf("fail to create distributed locker: %v", err)
	}

	storageGcScheduler, err := gocron.NewScheduler(
		gocron.WithDistributedLocker(locker),
	)
	if err != nil {
		return fmt.Errorf("fail to create storageGc scheduler: %v", err)
	}

	_, err = storageGcScheduler.NewJob(
		gocron.DurationJob(app.Conf.SessionCheckInterval),
		gocron.NewTask(app.StorageGc),
		gocron.WithName("storageGc"),
	)
	if err != nil {
		storageGcScheduler.Shutdown()
		return fmt.Errorf("fail to create storageGc task: %v", err)
	}

	storageGcScheduler.Start()

	sessionGcScheduler, err := gocron.NewScheduler()
	if err != nil {
		return fmt.Errorf("fail to create sessionGc scheduler: %v", err)
	}

	_, err = sessionGcScheduler.NewJob(
		gocron.DurationJob(app.Conf.SessionCheckInterval),
		gocron.NewTask(app.SessionGc),
	)
	if err != nil {
		storageGcScheduler.Shutdown()
		sessionGcScheduler.Shutdown()
		return fmt.Errorf("fail to create sessionGc task: %v", err)
	}

	sessionGcScheduler.Start()
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
	engineUrlForSelf := ""
	if !session.DryRun {
		jobInfo, err = session.Engine.MarshalToText()
		if err != nil {
			err = fmt.Errorf("PersistSessionInfo: get job info failed: %v", err)
			return
		}
		engineUrl = session.Engine.GetEndpointForPeer()
		engineUrlForSelf = session.Engine.GetEndpointForSelf()
	}

	warn, err := message.ProtoMarshal(session.Warning)
	if err != nil {
		err = fmt.Errorf("PersistSessionInfo: marsha warning failed: %v", err)
		return
	}
	return &storage.SessionInfo{

		SessionID:        session.ExecuteInfo.JobID,
		Status:           0,
		TableChecksum:    checksum.TableSchema,
		CCLChecksum:      checksum.CCL,
		EngineUrl:        engineUrl,
		EngineUrlForSelf: engineUrlForSelf,
		JobInfo:          jobInfo,
		WorkParties:      strings.Join(session.ExecuteInfo.WorkParties, ";"),
		OutputNames:      strings.Join(session.OutputNames, ";"),
		Warning:          warn,
		CreatedAt:        session.CreatedAt,
		ExpiredAt:        session.CreatedAt.Add(time.Duration(session.ExpireSeconds) * time.Second),
	}, nil
}

func stopExternalEngine(jobId, url string) error {
	conn, err := executor.NewEngineClientConn(url, "", nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewSCQLEngineServiceClient(conn)

	req := pb.StopJobRequest{
		JobId: jobId,
	}

	status, err := client.StopJob(context.TODO(), &req)
	if err != nil {
		return err
	}

	if status.GetCode() != int32(pb.Code_OK) {
		return fmt.Errorf("stop failed, response: %s", status.String())
	}

	return nil
}
