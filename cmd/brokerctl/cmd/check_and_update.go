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
	checkAndUpdateCmd = &cobra.Command{
		Use:   "check_and_update",
		Short: "Check and update status for specific or all projects",
		Args:  cobra.MaximumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if projectID != "" {
				return checkAndpdateStatus([]string{projectID})
			}
			return checkAndpdateStatus(nil)
		},
	}
)

func checkAndpdateStatus(ids []string) error {
	response, err := brokerCommand.CheckAndUpdateStatus(ids)
	if err != nil {
		return fmt.Errorf("updateStatus: %w", err)
	}
	switch response.GetStatus().GetCode() {
	case int32(pb.Code_OK):
		fmt.Println("check and update status succeeded")
		return nil
	case int32(pb.Code_PROJECT_CONFLICT):
		fmt.Println("skip update projcts, existing conflicts:")
		for id, conflict := range response.GetConflicts() {
			fmt.Println("project id: ", id)
			for _, item := range conflict.Items {
				fmt.Println("\t", item.Message)
			}
		}
		return nil
	default:
		return fmt.Errorf("updateStatus: %v", response.GetStatus())
	}
}
