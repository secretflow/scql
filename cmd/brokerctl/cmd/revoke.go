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
	revokeCmd = &cobra.Command{
		Use:   "revoke <party>",
		Short: "Revoke column constraint from party",
		Args:  cobra.MatchAll(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			return revokeCCL(args[0])
		},
	}
)

func init() {
	revokeCmd.Flags().StringVar(&tableName, "table-name", "", "table name for the constraint")
	revokeCmd.Flags().StringVar(&colName, "column-name", "", "column name for the constraint")
}

func revokeCCL(party string) error {
	if projectID == "" {
		return fmt.Errorf("flags project-id must not be empty")
	}
	if tableName == "" {
		return fmt.Errorf("flags table-name must not be empty")
	}
	if colName == "" {
		return fmt.Errorf("flags column-name must not be empty")
	}
	var ccls []*pb.ColumnControl
	ccls = append(ccls, &pb.ColumnControl{
		Col: &pb.ColumnDef{
			ColumnName: colName,
			TableName:  tableName,
		},
		PartyCode: party,
	})

	err := brokerCommand.RevokeCCL(projectID, party, ccls)
	if err != nil {
		return err
	}
	fmt.Println("revoke ccl succeeded")
	return nil

}
