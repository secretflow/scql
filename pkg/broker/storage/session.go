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
	"strings"

	"time"

	"github.com/sirupsen/logrus"
)

type SessionStatus int8

const (
	SessionRunning   SessionStatus = 0
	SessionFinished  SessionStatus = 1
	SessionCanceled  SessionStatus = 2
	SessionTimeout   SessionStatus = 3
	SessionFailed    SessionStatus = 4
	SessionSubmitted SessionStatus = 5
)

func (m *MetaManager) SetSessionInfo(info SessionInfo) error {
	result := m.db.Create(&info)
	return result.Error
}

func (m *MetaManager) UpdateSessionInfoStatusWithCondition(sessionID string, conditionStatus, targetStatus SessionStatus) error {
	result := m.db.Model(&SessionInfo{}).Where(&SessionInfo{SessionID: sessionID, Status: int8(conditionStatus)}).Update("status", targetStatus)
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

func (m *MetaManager) CheckIdExpired(ids []string) ([]string, error) {
	var expiredIds []string
	if len(ids) == 0 {
		return expiredIds, nil
	}

	result := m.db.Model(&SessionInfo{}).Select("session_id").Where("session_id in ?", ids).Where("expired_at < ?", time.Now()).Scan(&expiredIds)

	return expiredIds, result.Error
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

func (m *MetaManager) InitDistributedLockIfNecessary(id DistLockID) error {
	var lk Lock
	// If a lock with id does not exist, create one and set expired_at to now - 24h to ensure expiration so that it can be acquired immediately
	result := m.db.Where(Lock{ID: int8(id)}).Attrs(Lock{ExpiredAt: time.Now().Add(-24 * time.Hour)}).FirstOrCreate(&lk)
	if result.Error == nil {
		switch result.RowsAffected {
		case 0:
			logrus.Infof("distributed lock %d existing", id)
		case 1:
			logrus.Infof("create distributed locking info: %v", lk)
		default:
			return fmt.Errorf("first or create error, row affected=%d", result.RowsAffected)
		}
	}
	return result.Error
}

// NOTE: 1. InitDistributedLockIfNecessary must be called once before call HoldDistributedLock
//  2. lock will expired after expired_at
func (m *MetaManager) HoldDistributedLock(id DistLockID, owner string, ttl time.Duration) error {
	result := m.db.Model(&Lock{}).
		Where(&Lock{ID: int8(id)}).
		Where("expired_at < ?", time.Now()).Or("owner = ?", owner).
		Updates(map[string]interface{}{
			"expired_at": time.Now().Add(ttl),
			"owner":      owner,
		})

	if result.Error == nil && result.RowsAffected == 1 {
		return nil
	}
	return fmt.Errorf("hold distributed lock %s(%d) failed: {row affected: %d ; err: %+v}", id, int8(id), result.RowsAffected, result.Error)
}

// UpdateDistributedLock only succeeds if holding the distributed lock
func (m *MetaManager) UpdateDistributedLock(id DistLockID, owner string, ttl time.Duration) error {
	result := m.db.Model(&Lock{}).
		Where(&Lock{ID: int8(id), Owner: owner}).
		Updates(map[string]interface{}{
			"expired_at": time.Now().Add(ttl),
			"owner":      owner,
		})
	if result.Error == nil && result.RowsAffected == 1 {
		return nil
	}
	return fmt.Errorf("update distributed lock %d failed: {row affected: %d ; err: %+v}", id, result.RowsAffected, result.Error)
}

func (m *MetaManager) GetDistributedLockOwner(id DistLockID) (string, error) {
	var owner string
	result := m.db.Model(&Lock{}).
		Select("owner").
		Where(&Lock{ID: int8(id)}).
		Scan(&owner)
	return owner, result.Error
}

func (m *MetaManager) ClearExpiredSessions() error {
	var expiredResults []SessionResult

	result := m.db.Where("expired_at < ?", time.Now()).Limit(100).Delete(&expiredResults)

	logrus.Infof("ClearExpiredSessions rows:%d", result.RowsAffected)

	if result.Error == nil && result.RowsAffected > 0 {
		logrus.Infof("GC: clear %d expired results", result.RowsAffected)
	}

	if result.Error != nil {
		return result.Error
	}

	result = m.db.Model(&SessionInfo{}).Where("expired_at < ? AND status <> ?", time.Now(), int8(SessionTimeout)).Update("status", int8(SessionTimeout))
	if result.Error == nil && result.RowsAffected > 0 {
		logrus.Infof("GC: set %d sessions' status to be 'expired'", result.RowsAffected)
	}

	return result.Error
}

type WatchedJobStatus struct {
	JobId          string `gorm:"column:session_id"`
	EngineEndpoint string `gorm:"column:engine_url_for_self"`
	// number of engine inaccessible times
	InaccessibleTimes int `gorm:"-"`
}

func (m *MetaManager) GetWatchedJobs() ([]WatchedJobStatus, error) {
	var watchedJobs []WatchedJobStatus
	result := m.db.Model(&SessionInfo{}).
		Select("session_id, engine_url_for_self").
		Where("status = ?", int8(SessionRunning)).
		Find(&watchedJobs)
	if result.Error != nil {
		return nil, result.Error
	}

	for i := range watchedJobs {
		watchedJobs[i].InaccessibleTimes = 0
	}
	return watchedJobs, nil
}

func (m *MetaManager) SetSessionStatus(id string, status SessionStatus) error {
	result := m.db.Model(&SessionInfo{}).Select("status", "updated_at").Where("session_id = ?", id).Updates(&SessionInfo{Status: int8(status), UpdatedAt: time.Now()})
	return result.Error
}

func (m *MetaManager) GetSessionStatus(id string) (SessionStatus, error) {
	var status SessionStatus
	result := m.db.Model(&SessionInfo{}).Select("status").Where("session_id = ?", id).Scan(&status)
	return status, result.Error
}

func (m *MetaManager) SetMultipleSessionStatus(ids []string, status SessionStatus) error {
	if len(ids) == 0 {
		return nil
	}

	quotedIds := make([]string, len(ids))
	for i, id := range ids {
		quotedIds[i] = fmt.Sprintf("'%s'", id)
	}
	updatedAt := time.Now().Format("2006-01-02 15:04:05")

	sql := fmt.Sprintf("UPDATE session_infos SET status = %d, updated_at = '%s' WHERE session_id IN (%s)",
		status,
		updatedAt,
		strings.Join(quotedIds, ","),
	)

	return m.db.Exec(sql).Error
}
