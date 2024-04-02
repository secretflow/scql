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
)

var (
	cancelCmd = &cobra.Command{
		Use:   "cancel job",
		Short: "Cancel async job, currently only valid in the KUSCIA",
		Args:  cobra.MatchAll(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case "job":
				return cancelJob()
			default:
				return fmt.Errorf("not support cancel %v", args[0])
			}
		},
	}
)

func init() {
	cancelCmd.Flags().StringVar(&jobId, "job-id", "", "when cancel job, you must specify the job-id obtained after successfully creating the job")
	cancelCmd.MarkFlagRequired("job-id")
}

func cancelJob() error {
	err := brokerCommand.CancelJob(jobId)
	if err != nil {
		return err
	}
	fmt.Println("cancel job succeeded")
	return nil
}
