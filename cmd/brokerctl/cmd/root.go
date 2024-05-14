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
	"github.com/spf13/cobra"

	"github.com/secretflow/scql/pkg/util/brokerutil"
)

var (
	host          string
	projectID     string
	timeoutS      int
	brokerCommand *brokerutil.Command

	rootCmd = &cobra.Command{
		Use:   "brokerctl",
		Short: "A tool helps manage project in broker more easily",
		Long:  `Brokerctl is a tool that can help users quickly complete project configuration and initiate SCQL analysis tasks.`,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			brokerCommand = brokerutil.NewCommand(host, timeoutS)
		},
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func SetVersion(version string) {
	rootCmd.Version = version
}

func init() {
	rootCmd.PersistentFlags().StringVar(&host, "host", "", "host to access broker, e.g: http://localhost:8080")
	rootCmd.PersistentFlags().StringVar(&projectID, "project-id", "", "unique identifier for project")
	rootCmd.PersistentFlags().IntVar(&timeoutS, "timeout", 1, "timeout seconds for http requests to broker")
	rootCmd.MarkPersistentFlagRequired("host")

	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(inviteCmd)
	rootCmd.AddCommand(processCmd)
	rootCmd.AddCommand(grantCmd)
	rootCmd.AddCommand(revokeCmd)
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(cancelCmd)
	rootCmd.AddCommand(checkAndUpdateCmd)
}
