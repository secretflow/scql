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

// This is by design
const (
	SessionRunning SessionStatus = iota
	SessionFinished
	SessionCanceled
	SessionTimeout
	SessionFailed
	SessionSubmitted
)

type DistributeLockGuard struct {
	txn             *MetaTransaction
	postProcessFunc func(error)
}

// Every time a lock operation is performed, a new guard must be used.
func NewDistributeLockGuard(m *MetaManager) *DistributeLockGuard {
	lm := &DistributeLockGuard{
		txn: m.CreateMetaTransaction(),
	}
	lm.postProcessFunc = func(err error) {
		err = lm.txn.Finish(err)
		if err != nil {
			logrus.Error(err)
		}
	}
	return lm
}

func (lm *DistributeLockGuard) InitDistributedLockIfNecessary(id int8) (err error) {
	defer lm.postProcessFunc(err)
	var lk Lock
	// If a lock with id does not exist, create one and set expired_at to now - 24h to ensure expiration so that it can be acquired immediately
	result := lm.txn.db.Where(Lock{ID: id}).Attrs(Lock{ExpiredAt: time.Now().Add(-24 * time.Hour)}).FirstOrCreate(&lk)
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

// NOTE: 1. InitDistributedLockIfNecessary must be called once before call DistributedLockExpired
//  2. lock will expired after expired_at
func (lm *DistributeLockGuard) DistributedLockExpired(id int8, owner string) (expired bool, err error) {
	defer lm.postProcessFunc(err)
	result := lm.txn.db.Model(&Lock{}).
		Where(&Lock{ID: id}).
		Select("expired_at < ?", time.Now()).
		Scan(&expired)
	return expired, result.Error
}

func (lm *DistributeLockGuard) PreemptDistributedLock(id int8, owner string, ttl time.Duration) (ok bool, err error) {
	defer lm.postProcessFunc(err)
	result := lm.txn.db.Model(&Lock{}).
		Where(&Lock{ID: id}).
		Where("expired_at < ?", time.Now()).
		Updates(map[string]interface{}{
			// other instance can get lock after 2 * ttl
			"expired_at": time.Now().Add(2 * ttl),
			"owner":      owner,
		})
	return result.RowsAffected == 1, result.Error
}

func (lm *DistributeLockGuard) RenewDistributedLock(id int8, owner string, ttl time.Duration) (ok bool, err error) {
	defer lm.postProcessFunc(err)
	result := lm.txn.db.Model(&Lock{}).
		Where(&Lock{ID: id, Owner: owner}).
		Updates(map[string]interface{}{
			// other instance can get lock after 2 * ttl
			"expired_at": time.Now().Add(2 * ttl),
		})
	return result.RowsAffected == 1, result.Error
}

func (lm *DistributeLockGuard) GetDistributedLockOwner(id int8) (owner string, err error) {
	defer lm.postProcessFunc(err)
	result := lm.txn.db.Model(&Lock{}).
		Select("owner").
		Where(&Lock{ID: id}).
		Scan(&owner)
	return owner, result.Error
}

func (txn *MetaTransaction) SetSessionInfo(info *SessionInfo) error {
	result := txn.db.Create(info)
	return result.Error
}

func (txn *MetaTransaction) UpdateSessionInfo(info *SessionInfo) error {
	result := txn.db.Model(&SessionInfo{}).Where(&SessionInfo{SessionID: info.SessionID}).Updates(info)
	return result.Error
}

func (txn *MetaTransaction) UpdateSessionInfoStatusWithCondition(sessionID string, conditionStatus, targetStatus SessionStatus) error {
	result := txn.db.Model(&SessionInfo{}).Where(&SessionInfo{SessionID: sessionID, Status: int8(conditionStatus)}).Update("status", targetStatus)
	return result.Error
}

func (txn *MetaTransaction) GetSessionInfo(id string) (*SessionInfo, error) {
	var info SessionInfo
	result := txn.db.Model(&SessionInfo{}).Where(&SessionInfo{SessionID: id}).First(&info)
	return &info, result.Error
}

func (txn *MetaTransaction) CheckIdCanceled(ids []string) ([]string, error) {
	// TODO: limit length of ids
	var canceledIds []string
	if len(ids) == 0 {
		return canceledIds, nil
	}
	result := txn.db.Model(&SessionInfo{}).Select("session_id").Where("session_id in ?", ids).Where("status", int8(SessionCanceled)).Scan(&canceledIds)
	return canceledIds, result.Error
}

func (txn *MetaTransaction) CheckIdExpired(ids []string) ([]string, error) {
	var expiredIds []string
	if len(ids) == 0 {
		return expiredIds, nil
	}

	result := txn.db.Model(&SessionInfo{}).Select("session_id").Where("session_id in ?", ids).Where("expired_at < ?", time.Now()).Scan(&expiredIds)

	return expiredIds, result.Error
}

// NOTE: check SessionInfo exist before calling
func (txn *MetaTransaction) SetSessionResult(sr SessionResult) (err error) {
	// update session status to finished
	result := txn.db.Model(&SessionInfo{}).Where(&SessionInfo{SessionID: sr.SessionID}).Update("status", int8(SessionFinished))
	if result.Error != nil {
		return result.Error
	}

	result = txn.db.Create(&sr)
	if result.Error != nil {
		return fmt.Errorf("%s, result size(%d)", result.Error.Error(), len(sr.Result))
	}
	return nil
}

func (txn *MetaTransaction) GetSessionResult(id string) (SessionResult, error) {
	var sr SessionResult
	result := txn.db.Model(&SessionResult{}).Where(&SessionResult{SessionID: id}).First(&sr)
	return sr, result.Error
}

func (txn *MetaTransaction) ClearSessionResult(id string) (err error) {

	// update session status to canceled
	result := txn.db.Model(&SessionInfo{}).Where(&SessionInfo{SessionID: id}).Update("status", int8(SessionCanceled))
	if result.Error != nil {
		return result.Error
	}

	// clear SessionResult
	result = txn.db.Where("session_id = ?", id).Delete(&SessionResult{})
	return result.Error
}

func (txn *MetaTransaction) ClearSessionInfo(id string) (err error) {
	// clear SessionResult
	result := txn.db.Where(&SessionResult{SessionID: id}).Delete(&SessionResult{})
	if result.Error != nil {
		return result.Error
	}
	// clear SessionInfo
	result = txn.db.Where(&SessionInfo{SessionID: id}).Delete(&SessionInfo{})
	return result.Error
}

func (txn *MetaTransaction) ClearExpiredSessions() (err error) {
	var expiredResults []SessionResult

	result := txn.db.Where("expired_at < ?", time.Now()).Limit(100).Delete(&expiredResults)
	if result.Error != nil {
		return result.Error
	}
	logrus.Infof("ClearExpiredSessions rows:%d", result.RowsAffected)
	if result.RowsAffected > 0 {
		logrus.Infof("GC: clear %d expired results", result.RowsAffected)
	}

	result = txn.db.Model(&SessionInfo{}).Where("expired_at < ? AND status <> ?", time.Now(), int8(SessionTimeout)).Update("status", int8(SessionTimeout))
	if result.Error == nil && result.RowsAffected > 0 {
		logrus.Infof("GC: set %d sessions' status to be 'expired'", result.RowsAffected)
	}

	return result.Error
}

func (txn *MetaTransaction) GetWatchedJobs() ([]*SessionInfo, error) {
	var watchedJobs []*SessionInfo
	result := txn.db.Model(&SessionInfo{}).
		Where("status = ?", int8(SessionRunning)).
		Scan(&watchedJobs)
	if result.Error != nil {
		return nil, result.Error
	}
	return watchedJobs, nil
}

func (txn *MetaTransaction) SetSessionStatus(id string, status SessionStatus) error {
	result := txn.db.Model(&SessionInfo{}).Where("session_id = ?", id).Updates(&SessionInfo{Status: int8(status), UpdatedAt: time.Now()})
	return result.Error
}

func (txn *MetaTransaction) GetSessionStatus(id string) (SessionStatus, error) {
	var status SessionStatus
	result := txn.db.Model(&SessionInfo{}).Select("status").Where("session_id = ?", id).Scan(&status)
	return status, result.Error
}

func (txn *MetaTransaction) SetMultipleSessionStatus(ids []string, status SessionStatus) error {
	if len(ids) == 0 {
		return nil
	}
	return txn.db.Model(&SessionInfo{}).Where("session_id in ?", ids).Updates(&SessionInfo{Status: int8(status), UpdatedAt: time.Now()}).Error
}

func (txn *MetaTransaction) UpdateSessionUpdatedAt(ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return txn.db.Model(&SessionInfo{}).Where("session_id in ?", ids).Updates(&SessionInfo{UpdatedAt: time.Now()}).Error
}
