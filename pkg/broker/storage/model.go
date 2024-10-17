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
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

type DistLockID int8

const (
	GcLockID         DistLockID = 100
	JobWatcherLockID DistLockID = 101
)

func (lid DistLockID) String() string {
	switch lid {
	case GcLockID:
		return "gc_lock"
	case JobWatcherLockID:
		return "job_watcher_lock"
	default:
		return "unknown_lock"
	}
}

type Project struct {
	// ->;<-:create means read and create
	// id can't be modified
	ID          string `gorm:"column:id;type:varchar(64);primaryKey;uniqueIndex:;comment:'unique id';->;<-:create"`
	Name        string `gorm:"column:name;type:varchar(64);not null;comment:'project name'"`
	Description string `gorm:"column:description;type:varchar(64);comment:'description'"`
	Creator     string `gorm:"column:creator;type:varchar(64);comment:'creator of the project'"`
	Archived    bool   `gorm:"column:archived;comment:'if archived is true, whole project can't be modified'"`
	ProjectConf string `gorm:"column:project_conf;type:text;comment:'project config in json format'"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (p *Project) Equals(other *Project) (bool, error) {
	// compare each field except for time fields
	if p.ID != other.ID ||
		p.Name != other.Name ||
		p.Description != other.Description ||
		p.Creator != other.Creator ||
		p.Archived != other.Archived {
		return false, nil
	}

	var projConf pb.ProjectConfig
	if len(p.ProjectConf) == 0 && len(other.ProjectConf) == 0 {
		return true, nil
	}
	err := message.ProtoUnmarshal([]byte(p.ProjectConf), &projConf)

	if err != nil {
		return false, err
	}

	var otherProjConf pb.ProjectConfig
	err = message.ProtoUnmarshal([]byte(other.ProjectConf), &otherProjConf)

	if err != nil {
		return false, err
	}

	return proto.Equal(&projConf, &otherProjConf), nil
}

type Member struct {
	ProjectID string `gorm:"column:project_id;type:varchar(64);primaryKey;not null"`
	Member    string `gorm:"column:member;type:varchar(64);primaryKey;not null;comment:'member in the project'"`
}

type TableIdentifier struct {
	ProjectID string `gorm:"column:project_id;type:varchar(64);primaryKey;not null"`
	TableName string `gorm:"column:table_name;type:varchar(64);primaryKey;not null"`
}

type Table struct {
	TableIdentifier
	RefTable string `gorm:"column:ref_table;type:varchar(128);comment:'ref table'"`
	DBType   string `gorm:"column:db_type;type:varchar(64);comment:'database type like MYSQL'"`
	Owner    string `gorm:"column:owner;comment:'table owner'"`
	// view
	IsView       bool   `gorm:"column:is_view;comment:'this table is a view'"`
	SelectString string `gorm:"column:select_string;comment:'the internal select query in string format, the field is valid only when IsView is true'"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

type ColumnIdentifier struct {
	ProjectID  string `gorm:"column:project_id;type:varchar(64);primaryKey;not null"`
	TableName  string `gorm:"column:table_name;type:varchar(64);primaryKey;not null"`
	ColumnName string `gorm:"column:column_name;type:varchar(64);primaryKey;not null;"`
}

type Column struct {
	ColumnIdentifier
	DType     string `gorm:"column:data_type;type:varchar(64);comment:'data type like float'"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

type ColumnPrivIdentifier struct {
	ProjectID  string `gorm:"column:project_id;type:varchar(64);primaryKey;not null"`
	TableName  string `gorm:"column:table_name;type:varchar(64);primaryKey;not null"`
	ColumnName string `gorm:"column:column_name;type:varchar(64);primaryKey;not null;"`
	DestParty  string `gorm:"column:dest_party;type:varchar(64);primaryKey;not null;"`
}

type ColumnPriv struct {
	ColumnPrivIdentifier
	Priv      string `gorm:"column:priv;type:varchar(256);comment:'priv of column'"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Invitation struct {
	ID               uint64    `gorm:"column:id;primaryKey;comment:'auto generated increment id'"`
	ProjectID        string    `gorm:"column:project_id;type:varchar(64);not null;index:,composite:identifier;comment:'project id'"`
	Name             string    `gorm:"column:name;type:varchar(64);comment:'name'"`
	Description      string    `gorm:"column:description;type:varchar(64);comment:'description'"`
	Creator          string    `gorm:"column:creator;type:varchar(64);comment:'creator of the project'"`
	ProjectCreatedAt time.Time `gorm:"column:proj_created_at;comment:'the create time of the project'"`
	Member           string    `gorm:"column:member;type:string;not null;comment:'members, flattened string, like: alice;bob'"`
	ProjectConf      string    `gorm:"column:project_conf;type:text;comment:'project config in json format'"`
	Inviter          string    `gorm:"column:inviter;type:varchar(256);index:,composite:identifier;comment:'inviter'"`
	Invitee          string    `gorm:"column:invitee;type:varchar(256);index:,composite:identifier;comment:'invitee'"`
	// 0: default, not decided to accept invitation or not; 1: accepted; 2: rejected; 3: invalid
	Status     int8 `gorm:"column:status;default:0;comment:'accepted'"`
	InviteTime time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// SessionInfo and SessionResult ares used to support broker cluster mode
type SessionInfo struct {
	SessionID string `gorm:"column:session_id;type:varchar(64);primaryKey;uniqueIndex:;comment:'unique session id';->;<-:create"`
	// 0: default, running; 1: finished; 2: canceled
	Status           int8   `gorm:"column:status;default:0;comment:'session status'"`
	TableChecksum    []byte `gorm:"column:table_checksum;type:varbinary(256);comment:'table checksum for self party'"`
	CCLChecksum      []byte `gorm:"column:ccl_checksum;type:varbinary(256);comment:'ccl checksum for self party'"`
	EngineUrl        string `gorm:"column:engine_url;type:varchar(256);comment:'url for engine to communicate with peer engine'"`
	EngineUrlForSelf string `gorm:"column:engine_url_for_self;type:varchar(256);comment:'engine url used for self broker'"`
	JobInfo          []byte `gorm:"column:job_info;type:bytes;comment:'serialized job info to specify task in engine'"`
	WorkParties      string `gorm:"column:work_parties;type:string;not null;comment:'parties involved, flattened string, like: alice;bob'"`
	OutputNames      string `gorm:"column:output_names;type:string;comment:'output column names, flattened string, like: col1,col2'"`
	Warning          []byte `gorm:"column:warning;type:bytes;comment:'warning infos, serialized from pb.Warning'"`
	CreatedAt        time.Time
	UpdatedAt        time.Time
	ExpiredAt        time.Time
}

type SessionResult struct {
	SessionID string `gorm:"column:session_id;type:varchar(64);primaryKey;uniqueIndex:;comment:'unique session id';->;<-:create"`
	Result    []byte `gorm:"column:result;type:bytes;comment:'query result, serialized from protobuf message'"`
	CreatedAt time.Time
	UpdatedAt time.Time
	ExpiredAt time.Time
}

type Lock struct {
	ID        int8   `gorm:"column:id;primaryKey;uniqueIndex;comment:'lock id';->;<-:create"`
	Owner     string `gorm:"column:owner;type:varchar(64);comment:'lock owner'"`
	UpdatedAt time.Time
	ExpiredAt time.Time
}
