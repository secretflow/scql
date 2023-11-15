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
	inviteCmd = &cobra.Command{
		Use:   "invite <party>",
		Short: "Invite party to project",
		Args:  cobra.MatchAll(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			if projectID == "" {
				return fmt.Errorf("flags project-id must not be empty")
			}
			return inviteMember(args[0])
		},
	}
)

func inviteMember(member string) error {
	err := brokerCommand.InviteMember(projectID, member)
	if err != nil {
		return err
	}
	fmt.Printf("invite %v succeeded\n", member)
	return nil
}
