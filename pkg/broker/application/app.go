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
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"

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
			scheduler.WithMaxWaitTime(kusciaSchedulerOpt.MaxWaitTime),
			scheduler.WithPollInterval(kusciaSchedulerOpt.PollInterval),
			scheduler.WithAppImage(kusciaSchedulerOpt.AppImage),
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
	// TODO: fix default unavailable time for job is 3 minutes
	jobWatcher, err := executor.NewJobWatcher(app.MetaMgr, func(jobId, reason string) {
		app.onJobDead(jobId, reason)
	}, executor.WithMaxUnavailableDuration(time.Minute*3))
	if err != nil {
		return nil, fmt.Errorf("failed to create jobwatcher: %v", err)
	}

	app.JobWatcher = jobWatcher
	w, err := NewCronJobWorker(time.Minute, app.MetaMgr)
	if err != nil {
		return nil, fmt.Errorf("failed to create worker: %v", err)
	}
	w.addTask(&task{name: "storage gc", interval: app.Conf.SessionCheckInterval, fn: app.StorageGc})
	w.addTask(&task{name: "job watcher", interval: time.Minute, fn: app.JobWatcher.CheckJobs})
	err = w.start()
	if err != nil {
		return nil, fmt.Errorf("failed to start cron job worker: %v", err)
	}

	return app, nil
}

func (app *App) onJobDead(jobId, reason string) {
	logrus.Warnf("error occurred during executing query job %s: %v", jobId, reason)
	result := &pb.QueryResponse{
		Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: reason,
		},
	}
	info, err := app.SetSessionResult(jobId, result)
	if err != nil {
		logrus.Errorf("failed to set session result: %v", err)
	}
	engine, err := app.Scheduler.ParseEngineInstance(info.JobInfo)
	if err != nil {
		logrus.Warnf("failed to get engine from info: %v", err)
	}
	if err := engine.Stop(); err != nil {
		logrus.Warnf("failed to stop query job: %v", err)
	}
	logrus.Infof("success to stop query job %s", jobId)
}

func (app *App) AddSession(session *Session) error {
	info, err := sessionInfo(session)
	if err != nil {
		return fmt.Errorf("PersistSessionInfo: SessionInfo failed: %v", err)
	}
	// The job has only been submitted, it has not yet started to be executed by the engine.
	info.Status = int8(storage.SessionSubmitted)
	return app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		return txn.SetSessionInfo(info)
	})
}

func (app *App) UpdateSession(session *Session) error {
	info, err := sessionInfo(session)
	if err != nil {
		return fmt.Errorf("PersistSessionInfo: SessionInfo failed: %v", err)
	}
	return app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		return txn.UpdateSessionInfo(info)
	})
}

func (app *App) SetSessionResult(sid string, resp *pb.QueryResponse) (info *storage.SessionInfo, err error) {
	ser, err := message.ProtoMarshal(resp)
	if err != nil {
		return nil, fmt.Errorf("PersistSessionResult: marshal resp failed: %v", err)
	}
	err = app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		info, err = txn.GetSessionInfo(sid)
		if err != nil {
			return err
		}
		if info.Status != int8(storage.SessionRunning) && info.Status != int8(storage.SessionSubmitted) {
			return fmt.Errorf("failed to set session result due to session %s status is not running", sid)
		}
		if resp.Status.Code == 0 {
			err = txn.SetSessionStatus(sid, storage.SessionFinished)
		} else {
			err = txn.SetSessionStatus(sid, storage.SessionFailed)
		}
		if err != nil {
			return err
		}
		return txn.SetSessionResult(storage.SessionResult{
			SessionID: sid,
			Result:    ser,
			ExpiredAt: info.ExpiredAt,
		})
	})
	return
}

func (app *App) SetSessionStatus(sid string, status storage.SessionStatus) error {
	return app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		return txn.SetSessionStatus(sid, status)
	})
}

func (app *App) GetSessionInfo(sid string) (*storage.SessionInfo, error) {
	var info *storage.SessionInfo
	err := app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		info, err = txn.GetSessionInfo(sid)
		return err
	})
	if err != nil {
		return nil, err
	}
	return info, nil

}

func (app *App) GetSessionResult(sid string) (*pb.QueryResponse, error) {
	var res storage.SessionResult
	err := app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		res, err = txn.GetSessionResult(sid)
		return err
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	var response pb.QueryResponse
	err = message.ProtoUnmarshal(res.Result, &response)
	if err != nil {
		return nil, fmt.Errorf("unmarshal session result failed: %v", err)
	}
	return &response, nil

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

	// NOTE: only clear SessionResult, the SessionInfo is not cleared currently
	err = app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		return txn.ClearSessionResult(info.SessionID)
	})
	if err != nil {
		return fmt.Errorf("CancelSession: ClearSessionResult failed: %v", err)
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
