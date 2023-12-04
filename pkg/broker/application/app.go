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
	"fmt"
	"time"

	"github.com/patrickmn/go-cache"

	"github.com/secretflow/scql/pkg/broker/config"
	"github.com/secretflow/scql/pkg/broker/partymgr"
	"github.com/secretflow/scql/pkg/broker/services/auth"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/executor"
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
}

func NewApp(partyMgr partymgr.PartyMgr, metaMgr *storage.MetaManager, cfg *config.Config) (*App, error) {
	auth, err := auth.NewAuth(cfg.PrivatePemPath)
	if err != nil {
		return nil, fmt.Errorf("create auth from pem file %s: %v", cfg.PrivatePemPath, err)
	}

	return &App{
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
	}, nil
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

func (app *App) AddSession(sid string, session *Session) {
	app.Sessions.SetDefault(sid, session)
}
