// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"strconv"
	"time"

	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/parser/auth"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/types"
)

const (
	MaxAllowedPacket = "max_allowed_packet"
)

// SessionVars is to handle user-defined or global variables in the current session.
type SessionVars struct {
	// systems variables, don't modify it directly, use GetSystemVar/SetSystemVar method.
	systems map[string]string
	// SysWarningCount is the system variable "warning_count", because it is on the hot path, so we extract it from the systems
	SysWarningCount int
	// SysErrorCount is the system variable "error_count", because it is on the hot path, so we extract it from the systems
	SysErrorCount uint16

	// StrictSQLMode indicates if the session is in strict mode.
	StrictSQLMode bool

	// ActiveRoles stores active roles for current user
	ActiveRoles []*auth.RoleIdentity

	// Status stands for the session status. e.g. in transaction or not, auto commit is on or off, and so on.
	Status uint16

	// User is the user identity with which the session login.
	User *auth.UserIdentity

	// CurrentDB is the default database of this session.
	CurrentDB string

	// PlanID is the unique id of logical and physical plan.
	PlanID int

	// StmtCtx holds variables for current executing statement.
	StmtCtx *stmtctx.StatementContext

	// PlanColumnID is the unique id for column when building plan.
	PlanColumnID int64

	// SnapshotTS is used for reading history data.
	SnapshotTS uint64

	// SnapshotInfoschema is used with SnapshotTS, when the schema version at snapshotTS less than current schema
	// version, we load an old version schema for query.
	SnapshotInfoschema interface{}

	// StartTime is the start time of the last query.
	StartTime time.Time

	// DurationParse is the duration of parsing SQL string to AST of the last query.
	DurationParse time.Duration

	// DurationPlanning is the duration of compiling AST to logical plan of the last query.
	DurationPlanning time.Duration

	// DurationTranslating is the duration of converting logical plan to execution plan of the last query.
	DurationTranslating time.Duration

	// DurationExecuting is the duration of executing execution plan of the last query.
	DurationExecuting time.Duration

	// Per-connection time zones. Each client that connects has its own time zone setting, given by the session time_zone variable.
	// See https://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html
	TimeZone string

	// Storage
	Storage *gorm.DB

	// PreparedParams params for prepared statements
	PreparedParams PreparedParams

	SQLMode mysql.SQLMode

	// AffectedByGroupThreshold is used to mark whether GroupByThreshold is applied to protect query results
	AffectedByGroupThreshold bool

	// GroupByThreshold applied to protect query results
	GroupByThreshold uint64
}

// PreparedParams contains the parameters of the current prepared statement when executing it.
type PreparedParams []types.Datum

func (pps PreparedParams) String() (string, error) {
	if len(pps) == 0 {
		return "", nil
	}
	if datum_str, err := types.DatumsToString(pps, true); err != nil {
		return " [arguments: " + datum_str + "]", nil
	} else {
		return "", nil
	}
}

// NewSessionVars creates a session vars object.
func NewSessionVars() *SessionVars {
	vars := &SessionVars{}
	return vars
}

// AllocPlanColumnID allocates column id for plan.
func (s *SessionVars) AllocPlanColumnID() int64 {
	s.PlanColumnID++
	return s.PlanColumnID
}

// GetSystemVar gets the string value of a system variable.
func (s *SessionVars) GetSystemVar(name string) (string, bool) {
	if name == WarningCount {
		return strconv.Itoa(s.SysWarningCount), true
	} else if name == ErrorCount {
		return strconv.Itoa(int(s.SysErrorCount)), true
	}
	val, ok := s.systems[name]
	return val, ok
}

func (s *SessionVars) SetTimeZone(TimeZone string) {
	s.TimeZone = TimeZone
}

func (s *SessionVars) GetTimeZone() string {
	return s.TimeZone
}
