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

package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlog "gorm.io/gorm/logger"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

func TestSession(t *testing.T) {
	// setup db
	r := require.New(t)
	id, err := uuid.NewUUID()
	r.NoError(err)
	connStr := fmt.Sprintf("file:%s?mode=memory&cache=shared", id)
	db, err := gorm.Open(sqlite.Open(connStr),
		&gorm.Config{
			SkipDefaultTransaction: true,
			Logger: gormlog.New(
				logrus.StandardLogger(),
				gormlog.Config{
					SlowThreshold: 200 * time.Millisecond,
					Colorful:      false,
					LogLevel:      gormlog.Info,
				}),
		})

	r.NoError(err)
	manager := NewMetaManager(db, true)
	err = manager.Bootstrap()
	r.NoError(err)

	// -----test SessionInfo------
	sessionID := "123456789"
	info := SessionInfo{
		SessionID:     sessionID,
		Status:        0,
		EngineUrl:     "engine_alice.com",
		TableChecksum: []byte("table_checksum"),
		CCLChecksum:   []byte("ccl_checksum"),
		JobInfo:       []byte("{}"),
	}

	err = manager.SetSessionInfo(info)
	r.NoError(err)

	infoFetch, err := manager.GetSessionInfo(sessionID)
	r.NoError(err)
	info.CreatedAt = infoFetch.CreatedAt // update timestamp before comparison
	info.UpdatedAt = infoFetch.UpdatedAt
	r.Equal(info, infoFetch)

	err = manager.UpdateSessionInfoStatusWithCondition(sessionID, SessionRunning, SessionFailed)
	r.NoError(err)
	infoFetch, err = manager.GetSessionInfo(sessionID)
	r.NoError(err)
	r.Equal(int8(SessionFailed), infoFetch.Status)
	err = manager.UpdateSessionInfoStatusWithCondition(sessionID, SessionFailed, SessionRunning)
	r.NoError(err)

	// -----test SessionResult-----
	resp := &pb.QueryResponse{
		Status: &pb.Status{Code: 0},
		Result: &pb.QueryResult{
			OutColumns: []*pb.Tensor{{
				Name:       "ta",
				StringData: []string{"a", "b"},
			}},
			CostTimeS: 1.2345,
		},
	}
	str, err := message.ProtoMarshal(resp)
	r.NoError(err)
	// set SessionResult
	err = manager.SetSessionResult(SessionResult{
		SessionID: sessionID,
		Result:    str,
	})
	r.NoError(err)

	// get SessionResult
	info2, err := manager.GetSessionInfo(sessionID)
	r.NoError(err)
	r.Equal(info2.Status, int8(SessionFinished))
	resultFetch, err := manager.GetSessionResult(sessionID)
	r.NoError(err)
	var respFetch pb.QueryResponse
	err = message.ProtoUnmarshal([]byte(resultFetch.Result), &respFetch)
	r.NoError(err)
	r.Equal(resp.Status, respFetch.Status)
	r.Equal(resp.GetResult().GetOutColumns(), respFetch.GetResult().GetOutColumns())
	r.Equal(resp.GetResult().GetCostTimeS(), respFetch.GetResult().GetCostTimeS())

	// clear SessionResult
	err = manager.ClearSessionResult(sessionID)
	r.NoError(err)
	err = manager.ClearSessionResult(sessionID) // clear repeatedly should be ok
	r.NoError(err)

	info3, err := manager.GetSessionInfo(sessionID)
	r.NoError(err)
	r.Equal(info3.Status, int8(SessionCanceled))

	_, err = manager.GetSessionResult(sessionID)
	r.Error(err)

	// -----test gc------
	err = manager.InitDistributedLockIfNecessary(GcLockID)
	r.NoError(err)
	err = manager.InitDistributedLockIfNecessary(GcLockID)
	r.NoError(err)
	// test HoldGcLock
	err = manager.HoldDistributedLock(GcLockID, "alice", time.Second)
	r.NoError(err)
	// lock can be reentrant
	err = manager.HoldDistributedLock(GcLockID, "alice", time.Second)
	r.NoError(err)
	err = manager.HoldDistributedLock(GcLockID, "bob", time.Second)
	r.Equal(err.Error(), "hold distributed lock gc_lock(100) failed: {row affected: 0 ; err: <nil>}")

	time.Sleep(time.Second)
	err = manager.HoldDistributedLock(GcLockID, "alice", time.Second)
	r.NoError(err)
	// test ClearExpiredResults
	expireSeconds := 5
	err = manager.SetSessionResult(SessionResult{
		SessionID: sessionID,
		Result:    str,
		CreatedAt: time.Now(),
		ExpiredAt: time.Now().Add(time.Duration(expireSeconds) * time.Second),
	})
	r.NoError(err)
	err = manager.ClearExpiredSessions()
	r.NoError(err)
	_, err = manager.GetSessionResult(sessionID)
	r.NoError(err)
	time.Sleep(time.Duration(2*expireSeconds) * time.Second)
	err = manager.ClearExpiredSessions()
	r.NoError(err)
	_, err = manager.GetSessionResult(sessionID)
	r.Equal(err.Error(), "record not found")

	// test CheckIdCanceled
	err = manager.SetSessionInfo(SessionInfo{
		SessionID: "canceled id",
		Status:    int8(SessionCanceled),
	})
	r.NoError(err)
	canceledIds, err := manager.CheckIdCanceled([]string{"not exist id", sessionID, "canceled id"})
	r.NoError(err)
	r.Equal(canceledIds, []string{"canceled id"})

	// test watch job related
	jobId := "jobId"
	err = manager.SetSessionInfo(SessionInfo{
		SessionID: jobId,
		Status:    int8(SessionRunning),
	})
	r.NoError(err)

	jobStatus, err := manager.GetWatchedJobs()
	r.NoError(err)
	r.Equal(len(jobStatus), 1)

	err = manager.SetSessionStatus(jobId, SessionCanceled)
	r.NoError(err)
	status, err := manager.GetSessionStatus(jobId)
	r.NoError(err)
	r.Equal(status, SessionCanceled)

	secondJobId := "jobId2"
	err = manager.SetSessionInfo(SessionInfo{
		SessionID: secondJobId,
		Status:    int8(SessionRunning),
	})
	r.NoError(err)

	ids := []string{jobId, secondJobId}
	err = manager.SetMultipleSessionStatus(ids, SessionFailed)
	r.NoError(err)
	status, err = manager.GetSessionStatus(jobId)
	r.NoError(err)
	r.Equal(status, SessionFailed)
	status, err = manager.GetSessionStatus(secondJobId)
	r.NoError(err)
	r.Equal(status, SessionFailed)
}
