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

package main

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"

	"github.com/secretflow/scql/pkg/util/sqlbuilder"
)

var (
	userName    string
	partyCode   string
	passwd      string
	pemFile     string
	ifNotExists bool
)

var rootCmd = &cobra.Command{
	Use:       "scqltool",
	Short:     "tool to generate scql query",
	ValidArgs: []string{"genCreateUserStmt", "help"},
	Args:      cobra.OnlyValidArgs,
}

var genCreateUserStmtCmd = &cobra.Command{
	Use:   "genCreateUserStmt",
	Short: "generate create user statement",
	RunE: func(cmd *cobra.Command, args []string) error {
		builder := sqlbuilder.NewCreateUserStmtBuilder()
		if ifNotExists {
			builder = builder.IfNotExists()
		}
		sql, err := builder.SetUser(userName).SetParty(partyCode).SetPassword(passwd).AuthByPubkeyWithPemFile(pemFile).ToSQL()
		if err != nil {
			return err
		}
		fmt.Println("Create User Statement:")
		fmt.Println(sql)
		return nil
	},
}

func initCmd() {
	flagSet := genCreateUserStmtCmd.PersistentFlags()
	flagSet.StringVar(&userName, "user", "", "new user name tobe created")
	flagSet.StringVar(&partyCode, "party", "", "which party the new user belongs to")
	flagSet.StringVar(&passwd, "passwd", "", "user password")
	flagSet.StringVar(&pemFile, "pem", "", "private key pem file path")
	flagSet.BoolVar(&ifNotExists, "ifNotExists", false, "whether if not exists")

	rootCmd.AddCommand(genCreateUserStmtCmd)
}

func main() {
	initCmd()
	err := rootCmd.Execute()
	if err != nil {
		log.Fatalf("Failed to execute cobra.Command: %v", err)
	}
}
