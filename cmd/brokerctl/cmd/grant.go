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

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

var (
	tableName string
	colName   string

	// TODO: support grant multiple ccls
	grantCmd = &cobra.Command{
		Use:   "grant <party> {PLAINTEXT|PLAINTEXT_AFTER_JOIN|ENCRYPTED_ONLY|...}",
		Short: "Grant column constraint to party",
		Args:  cobra.MatchAll(cobra.ExactArgs(2)),
		RunE: func(cmd *cobra.Command, args []string) error {
			return grantCCL(args[0], args[1])
		},
	}
)

func init() {
	grantCmd.Flags().StringVar(&tableName, "table-name", "", "table name for the constraint")
	grantCmd.Flags().StringVar(&colName, "column-name", "", "column name for the constraint")
}

func grantCCL(party, constraint string) error {
	req := &pb.GrantCCLRequest{}
	if projectID == "" {
		return fmt.Errorf("flags project-id must not be empty")
	} else {
		req.ProjectId = projectID
	}
	if tableName == "" {
		return fmt.Errorf("flags table-name must not be empty")
	}
	if colName == "" {
		return fmt.Errorf("flags column-name must not be empty")
	}
	value, ok := pb.Constraint_value[constraint]
	if !ok {
		return fmt.Errorf("not support constraint %v", constraint)
	}
	var ccls []*pb.ColumnControl
	ccls = append(ccls, &pb.ColumnControl{
		Col: &pb.ColumnDef{
			ColumnName: colName,
			TableName:  tableName,
		},
		PartyCode:  party,
		Constraint: pb.Constraint(value),
	})

	err := brokerCommand.GrantCCL(projectID, ccls)
	if err != nil {
		return err
	}
	fmt.Println("grant ccl succeeded")
	return nil
}
