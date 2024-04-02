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
	response string

	processCmd = &cobra.Command{
		Use:   "process invitation <invitation_id>",
		Short: "accept/decline invitation corresponding to invitation_id",
		Args:  cobra.MatchAll(cobra.ExactArgs(2)),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case "invitation":
				return processInvitation(args[1])
			default:
				return fmt.Errorf("not support process %v", args[0])
			}
		},
	}
)

func init() {
	processCmd.Flags().StringVar(&response, "response", "", "accept or decline the invitation")
	processCmd.MarkFlagRequired("response")
}

func processInvitation(ids string) error {
	var accept bool
	switch response {
	case "accept":
		accept = true
	case "decline":
		accept = false
	default:
		return fmt.Errorf("flags response %v not belong to {accept|decline}", response)
	}
	err := brokerCommand.ProcessInvitation(ids, accept)
	if err != nil {
		return err
	}
	fmt.Printf("process invitation %v succeeded\n", ids)
	return nil

}
