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
	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/storage"
)

// NOTE: StorageGc will continue GC until program exits
func (app *App) StorageGc() {
	// scan table to get all expired ids
	err := app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
		return txn.ClearExpiredSessions()
	})
	if err != nil {
		logrus.Warnf("GC err: %s", err.Error())
	}
}
