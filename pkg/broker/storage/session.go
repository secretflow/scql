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

	"time"

	"github.com/sirupsen/logrus"
)

type SessionStatus int8

const (
	SessionRunning  SessionStatus = 0
	SessionFinished SessionStatus = 1
	SessionCanceled SessionStatus = 2
)

func (m *MetaManager) SetSessionInfo(info SessionInfo) error {
	result := m.db.Create(&info)
	return result.Error
}

func (m *MetaManager) GetSessionInfo(id string) (SessionInfo, error) {
	var info SessionInfo
	result := m.db.Model(&SessionInfo{}).Where(&SessionInfo{SessionID: id}).First(&info)
	return info, result.Error
}

func (m *MetaManager) CheckIdCanceled(ids []string) ([]string, error) {
	// TODO: limit length of ids
	var canceledIds []string
	if len(ids) == 0 {
		return canceledIds, nil
	}
	result := m.db.Model(&SessionInfo{}).Select("session_id").Where("session_id in ?", ids).Where("status", int8(SessionCanceled)).Scan(&canceledIds)
	return canceledIds, result.Error
}

// NOTE: check SessionInfo exist before calling
func (m *MetaManager) SetSessionResult(sr SessionResult) (err error) {
	txn := m.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()
	// update session status to finished
	result := txn.db.Model(&SessionInfo{}).Where(&SessionInfo{SessionID: sr.SessionID}).Update("status", int8(SessionFinished))
	if result.Error != nil {
		return result.Error
	}

	result = txn.db.Create(&sr)
	return result.Error
}

func (m *MetaManager) GetSessionResult(id string) (SessionResult, error) {
	var sr SessionResult
	result := m.db.Model(&SessionResult{}).Where(&SessionResult{SessionID: id}).First(&sr)
	return sr, result.Error
}

func (m *MetaManager) ClearSessionResult(id string) (err error) {
	txn := m.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()

	// update session status to canceled
	result := txn.db.Model(&SessionInfo{}).Where(&SessionInfo{SessionID: id}).Update("status", int8(SessionCanceled))
	if result.Error != nil {
		return result.Error
	}

	// clear SessionResult
	result = txn.db.Where("session_id = ?", id).Delete(&SessionResult{})
	return result.Error
}

func (m *MetaManager) InitGcLockIfNecessary() error {
	var lk Lock
	// If a lock with id == LockID does not exist, create one and set updated_at to now - 24h to ensure expiration so that it can be acquired immediately
	result := m.db.Where(Lock{ID: GcLockID}).Attrs(Lock{UpdatedAt: time.Now().Add(-24 * time.Hour)}).FirstOrCreate(&lk)
	if result.Error == nil {
		switch result.RowsAffected {
		case 0:
			logrus.Info("gc locking info existing")
		case 1:
			logrus.Infof("create gc locking info: %v", lk)
		default:
			return fmt.Errorf("first or create error, row affected=%d", result.RowsAffected)
		}
	}
	return result.Error
}

// NOTE: 1. FirstOrCreateGcLock must be called once before call HoldGcLock
//  2. lock will expired after ttl, please make sure ttl is long enough for finished tasks
func (m *MetaManager) HoldGcLock(owner string, ttl time.Duration) error {
	result := m.db.Model(&Lock{}).Where(&Lock{ID: GcLockID}).Where("updated_at < ?", time.Now().Add(-ttl)).Updates(&Lock{Owner: owner, UpdatedAt: time.Now()})
	if result.Error == nil && result.RowsAffected == 1 {
		return nil
	}
	return fmt.Errorf("hold lock failed: {row affected: %d ; err: %+v}", result.RowsAffected, result.Error)
}

func (m *MetaManager) ClearExpiredResults(ttl time.Duration) error {
	result := m.db.Where("created_at < ?", time.Now().Add(-ttl)).Limit(100).Delete(&SessionResult{})
	if result.Error == nil && result.RowsAffected > 0 {
		logrus.Infof("GC: clear %d expired results", result.RowsAffected)
	}
	return result.Error
}
