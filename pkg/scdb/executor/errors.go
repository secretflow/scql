// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/parser/terror"
)

// Error instances.
var (
	ErrUnknownPlan             = terror.ClassExecutor.New(mysql.ErrUnknownPlan, mysql.MySQLErrName[mysql.ErrUnknownPlan])
	ErrPasswordFormat          = terror.ClassExecutor.New(mysql.ErrPasswordFormat, mysql.MySQLErrName[mysql.ErrPasswordFormat])
	ErrBadDB                   = terror.ClassExecutor.New(mysql.ErrBadDB, mysql.MySQLErrName[mysql.ErrBadDB])
	ErrCantCreateUserWithGrant = terror.ClassExecutor.New(mysql.ErrCantCreateUserWithGrant, mysql.MySQLErrName[mysql.ErrCantCreateUserWithGrant])
	ErrDBaccessDenied          = terror.ClassExecutor.New(mysql.ErrDBaccessDenied, mysql.MySQLErrName[mysql.ErrDBaccessDenied])
	ErrTableaccessDenied       = terror.ClassExecutor.New(mysql.ErrTableaccessDenied, mysql.MySQLErrName[mysql.ErrTableaccessDenied])
)
