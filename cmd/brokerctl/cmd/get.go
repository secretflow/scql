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
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

var (
	tables  string
	parties string
	jobId   string

	getCmd = &cobra.Command{
		Use:   "get {project|table [NAME1 [,NAME2]]|ccl|invitation|result}",
		Short: "Show existing project|table|ccl|invitation or get job result",
		Args:  cobra.MatchAll(cobra.MinimumNArgs(1), cobra.MaximumNArgs(2)),
		RunE: func(cmd *cobra.Command, args []string) error {
			var names []string
			if len(args) == 2 {
				names = splitNames(args[1])
			}
			switch args[0] {
			case "project", "projects":
				return getProject()
			case "table", "tables":
				return getTable(names)
			case "ccl", "ccls":
				return getCCL()
			case "invitation", "invitations":
				return getInvitation()
			case "result", "results":
				return getResult()
			default:
				return fmt.Errorf("not support get %v", args[0])
			}
		},
	}
)

func init() {
	getCmd.Flags().StringVar(&tables, "tables", "", "when get ccl, you can specify tables in format 'NAME [, NAME]', e.g: 'ta, tb'")
	getCmd.Flags().StringVar(&parties, "parties", "", "when get ccl, you can specify parties in format 'NAME [, NAME]', e.g: 'alice, bob'")
	getCmd.Flags().StringVar(&jobId, "job-id", "", "when get result, you must specify the job-id obtained after successfully creating the job")
}

func getProject() error {
	response, err := brokerCommand.GetProject(projectID)
	if err != nil {
		return err
	}
	fmt.Println("get project succeeded")
	printProjects(response.GetProjects())
	return nil
}

func getTable(names []string) error {
	if projectID == "" {
		fmt.Printf("flags project-id must not be empty")
	}
	response, err := brokerCommand.GetTable(projectID, names)
	if err != nil {
		return err
	}
	fmt.Println("get table succeeded")
	printTables(response.GetTables())
	return nil
}

func getCCL() error {
	if projectID == "" {
		fmt.Printf("flags project-id must not be empty")
	}
	response, err := brokerCommand.GetCCL(projectID, splitNames(tables), splitNames(parties))
	if err != nil {
		return err
	}
	fmt.Println("get ccl succeeded")
	printCCLs(response.GetColumnControlList())
	return nil
}

func getInvitation() error {
	response, err := brokerCommand.GetInvitation()
	if err != nil {
		return err
	}
	fmt.Println("get invitation succeeded:")
	printInvitations(response.GetInvitations())
	return nil
}

func getResult() error {
	if jobId == "" {
		return fmt.Errorf("flags job-id must not be empty")
	}
	response, notReady, err := brokerCommand.GetResult(jobId)
	if err != nil {
		return err
	}
	if notReady {
		fmt.Println("result is not ready, try again later")
		return nil
	}
	if response.GetStatus().GetCode() == 0 {
		fmt.Println("get result succeeded")
		printQueryResult(response)
		return nil
	}
	return nil
}

func splitNames(str string) []string {
	items := strings.Split(str, ",")
	var ss []string
	for _, item := range items {
		tmp := strings.Trim(item, " ")
		if len(tmp) > 0 {
			ss = append(ss, tmp)
		}
	}
	return ss
}

func printProjects(projects []*pb.ProjectDesc) {
	fmt.Fprintf(os.Stdout, "[fetch]\n")
	if len(projects) == 0 {
		fmt.Println("No existing projects")
		return
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(false)
	table.SetHeader([]string{"ProjectId", "Creator", "Members", "Conf"})

	for _, proj := range projects {
		var curRow []string
		curRow = append(curRow, proj.GetProjectId())
		curRow = append(curRow, proj.GetCreator())
		curRow = append(curRow, fmt.Sprint(proj.GetMembers()))
		curRow = append(curRow, protojson.Format(proj.GetConf().GetSpuRuntimeCfg()))

		table.Append(curRow)
	}

	table.Render()
}

func printInvitations(invitations []*pb.ProjectInvitation) {
	fmt.Fprintf(os.Stdout, "[fetch]\n")
	if len(invitations) == 0 {
		fmt.Println("No existing invitations")
		return
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(false)
	table.SetHeader([]string{"InvitationId", "Status", "Inviter", "ProjectId", "Creator", "Members", "Conf"})

	for _, invite := range invitations {
		var curRow []string
		curRow = append(curRow, fmt.Sprint(invite.GetInvitationId()))
		switch invite.GetStatus() {
		case pb.InvitationStatus_UNDECIDED:
			curRow = append(curRow, "Pending")
		case pb.InvitationStatus_ACCEPTED:
			curRow = append(curRow, "Accepted")
		case pb.InvitationStatus_DECLINED:
			curRow = append(curRow, "Declined")
		case pb.InvitationStatus_INVALID:
			curRow = append(curRow, "Invalid")
		default:
			curRow = append(curRow, "Error")
		}
		curRow = append(curRow, invite.GetInviter())
		curRow = append(curRow, invite.GetProject().GetProjectId())
		curRow = append(curRow, invite.GetProject().GetCreator())
		curRow = append(curRow, fmt.Sprint(invite.GetProject().GetMembers()))
		curRow = append(curRow, protojson.Format(invite.GetProject().GetConf().GetSpuRuntimeCfg()))

		table.Append(curRow)
	}

	table.Render()
}

func printTables(tables []*pb.TableMeta) {
	fmt.Fprintf(os.Stdout, "[fetch]\n")
	if len(tables) == 0 {
		fmt.Println("No existing tables")
		return
	}

	for _, tbl := range tables {
		fmt.Printf("TableName: %s, Owner: %s, RefTable: %s, DBType: %s\nColumns:\n", tbl.GetTableName(), tbl.GetTableOwner(), tbl.GetRefTable(), tbl.GetDbType())

		table := tablewriter.NewWriter(os.Stdout)
		table.SetAutoWrapText(false)
		table.SetAutoFormatHeaders(false)
		table.SetHeader([]string{"ColumnName", "DataType"})

		for _, col := range tbl.GetColumns() {
			table.Append([]string{col.GetName(), col.GetDtype()})
		}

		table.Render()
	}
}

func printCCLs(ccls []*pb.ColumnControl) {
	fmt.Fprintf(os.Stdout, "[fetch]\n")
	if len(ccls) == 0 {
		fmt.Println("No existing ccls")
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(false)
	table.SetHeader([]string{"PartyCode", "TableName", "ColumnName", "Constraint"})

	for _, ccl := range ccls {
		var curRow []string
		curRow = append(curRow, ccl.GetPartyCode())
		curRow = append(curRow, ccl.GetCol().GetTableName())
		curRow = append(curRow, ccl.GetCol().GetColumnName())
		curRow = append(curRow, fmt.Sprint(ccl.GetConstraint()))

		table.Append(curRow)
	}
	table.Render()
}
