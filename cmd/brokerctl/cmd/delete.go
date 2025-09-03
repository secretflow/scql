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
	deleteCmd = &cobra.Command{
		Use:   "delete {table|view|project} [name]",
		Short: "Delete table, view, or project. Tables/views will be deleted from project, projects must be archived first",
		Args:  cobra.MatchAll(cobra.MinimumNArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case Table:
				if len(args) != 2 {
					return fmt.Errorf("missing <name> for deleting table")
				}
				return deleteTable(args[1])
			case View:
				if len(args) != 2 {
					return fmt.Errorf("missing <name> for deleting view")
				}
				return deleteView(args[1])
			case Project:
				if len(args) != 1 {
					return fmt.Errorf("delete project does not require additional arguments")
				}
				return deleteProject()
			default:
				return fmt.Errorf("not support delete %v", args[0])
			}
		},
	}
)

func deleteView(name string) error {
	if projectID == "" {
		return fmt.Errorf("flags project-id must not be empty")
	}
	err := brokerCommand.DeleteView(projectID, name)
	if err != nil {
		return err
	}
	fmt.Printf("delete view '%s' in '%s' succeeded\n", name, projectID)
	return nil
}

func deleteTable(name string) error {
	if projectID == "" {
		return fmt.Errorf("flags project-id must not be empty")
	}
	err := brokerCommand.DeleteTable(projectID, name)
	if err != nil {
		return err
	}
	fmt.Printf("delete table '%s' in '%s' succeeded\n", name, projectID)
	return nil
}

func deleteProject() error {
	if projectID == "" {
		return fmt.Errorf("flags project-id must not be empty")
	}
	err := brokerCommand.DeleteProject(projectID)
	if err != nil {
		return err
	}
	fmt.Printf("delete project '%s' succeeded\n", projectID)
	return nil
}
