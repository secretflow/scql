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

package common

import (
	"errors"
	"fmt"
	"net/http"
	"unicode"

	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/logutil"
	"github.com/secretflow/scql/pkg/util/message"
	prom "github.com/secretflow/scql/pkg/util/prometheus"
)

func VerifyTableMeta(meta storage.TableMeta) error {
	// 1. check DB type
	_, err := core.ParseDBType(meta.Table.DBType)
	if err != nil {
		return fmt.Errorf("VerifyTableMeta: %s", err)
	}
	// 2. check column type
	for _, col := range meta.Columns {
		if ok := constant.SupportTypes[col.DType]; !ok {
			return fmt.Errorf("VerifyTableMeta: unsupported data type %s", col.DType)
		}
	}
	// 3. check table/column name
	if hasSpaceInString(meta.Table.ProjectID) {
		return fmt.Errorf("VerifyTableMeta: illegal project id %s which contains space", meta.Table.ProjectID)
	}
	if hasSpaceInString(meta.Table.TableName) {
		return fmt.Errorf("VerifyTableMeta: illegal table name %s which contains space", meta.Table.TableName)
	}

	createFormat := `
	create table %s.%s (
		%s
	);
	`
	columnStr := ""
	for i := 0; i < len(meta.Columns); i++ {
		if hasSpaceInString(meta.Columns[i].ColumnName) {
			return fmt.Errorf("VerifyTableMeta: illegal column name %s which contains space", meta.Columns[i].ColumnName)
		}
		columnStr += fmt.Sprintf("%s int", meta.Columns[i].ColumnName)
		if i < len(meta.Columns)-1 {
			columnStr += ",\n"
		}
	}
	p := parser.New()
	_, err = p.ParseOneStmt(fmt.Sprintf(createFormat, meta.Table.ProjectID, meta.Table.TableName, columnStr), "", "")
	if err != nil {
		return fmt.Errorf("VerifyTableMeta: %s", err)
	}
	// 4. check ref table
	_, err = core.NewDbTableFromString(meta.Table.RefTable)
	if err != nil {
		return fmt.Errorf("VerifyTableMeta: %s", err)
	}
	return nil
}

func VerifyProjectID(projectID string) error {
	if hasSpaceInString(projectID) {
		return fmt.Errorf("VerifyProjectID: illegal project id %s which contains space", projectID)
	}
	p := parser.New()
	// project id may work as db name
	_, err := p.ParseOneStmt(fmt.Sprintf("create database %s;", projectID), "", "")
	if err != nil {
		return fmt.Errorf("VerifyProjectID: %s", err)
	}
	return nil
}

func hasSpaceInString(s string) bool {
	for _, c := range s {
		if unicode.IsSpace(c) {
			return true
		}
	}
	return false
}

func feedResponseStatus(c *gin.Context, response proto.Message, err error) {
	if err != nil {
		var statusPointer *status.Status
		if !errors.As(err, &statusPointer) {
			statusPointer = status.New(pb.Code_INTERNAL, err.Error())
		}
		statusDesc := response.ProtoReflect().Descriptor().Fields().ByJSONName("status")
		response.ProtoReflect().Set(statusDesc, protoreflect.ValueOf(statusPointer.ToProto().ProtoReflect()))
		c.Set(prom.ResponseStatusKey, statusPointer.Code().String())
	} else {
		c.Set(prom.ResponseStatusKey, pb.Code_OK.String())
	}
}

func FeedResponse(c *gin.Context, response proto.Message, err error, encodingType message.ContentEncodingType) {
	feedResponseStatus(c, response, err)
	body, err := message.SerializeTo(response, encodingType)
	if err != nil {
		c.String(http.StatusInternalServerError, fmt.Sprintf("FeedResponse: unable to serialize response: %+v", response))
		return
	}

	switch encodingType {
	case message.EncodingTypeJson:
		c.Header("Content-Type", "application/json")
	case message.EncodingTypeProtobuf:
		c.Header("Content-Type", "application/x-protobuf")
	default:
		c.Header("Content-Type", "application/x-protobuf")
	}

	c.String(http.StatusOK, body)
}

func LogWithError(logEntry *logutil.BrokerMonitorLogEntry, err error) {
	if err != nil {
		logEntry.ErrorMsg = err.Error()
		logrus.Errorf("%v", logEntry)
	} else {
		logrus.Infof("%v", logEntry)
	}
}

// Check Archived/ProjectConf/Creator
func CheckInvitationCompatibleWithProj(invitation storage.Invitation, proj storage.Project) error {
	if proj.Archived {
		return fmt.Errorf("failed to reply invitation due to project %s archived", proj.ID)
	}

	var currentProj pb.ProjectConfig
	err := message.ProtoUnmarshal([]byte(proj.ProjectConf), &currentProj)
	if err != nil {
		return fmt.Errorf("failed to deserialize project conf of current project")
	}

	var inviteProj pb.ProjectConfig
	err = message.ProtoUnmarshal([]byte(invitation.ProjectConf), &inviteProj)

	if err != nil {
		return fmt.Errorf("failed to deserialize project conf of invitation project")
	}

	if !proto.Equal(&currentProj, &inviteProj) {
		return fmt.Errorf("failed to check project config got %+v from project but expected %+v", proj.ProjectConf, invitation.ProjectConf)
	}
	if proj.Creator != invitation.Creator {
		return fmt.Errorf("failed to check creator got %q from project but expected %q", proj.Creator, invitation.Creator)
	}
	return nil
}

// check project exist and current party is member
func CheckMemberExistInProject(manager *storage.MetaManager, projectID string, member string) error {
	return manager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
		members, err := txn.GetProjectMembers(projectID)
		if err != nil {
			return err
		}
		if len(members) == 0 {
			return fmt.Errorf("CheckMemberExistInProject: project %s has no members or project doesn't exist", projectID)
		}
		if !slices.Contains(members, member) {
			return fmt.Errorf("CheckMemberExistInProject: issuer code %s is not member of project %s", member, projectID)
		}
		return nil
	})
}
