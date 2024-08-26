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

package server_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/executor"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/client"
	"github.com/secretflow/scql/pkg/scdb/config"
	"github.com/secretflow/scql/pkg/scdb/server"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/util/message"
	"github.com/secretflow/scql/pkg/util/mock"
)

const (
	userNameRoot  = "root"
	userNameAlice = "alice"
	userNameBob   = "bob"
)
const (
	userRootPassword  = "root"
	userAlicePassword = "Alice_password_123"
	userBobPassword   = "Bob_password_123"
)

var (
	userRoot  = newUserCredential(userNameRoot, userRootPassword)
	userAlice = newUserCredential(userNameAlice, userAlicePassword)
	userBob   = newUserCredential(userNameBob, userBobPassword)
)

func TestSCDBServer(t *testing.T) {
	r := require.New(t)

	// pick an available port to avoid port conflicts
	listener, err := net.Listen("tcp", ":0")
	r.NoError(err)

	port := listener.Addr().(*net.TCPAddr).Port
	log.Printf("SCDB is listening on :%v\n", port)

	config := &config.Config{
		Port:          port,
		SCDBHost:      fmt.Sprintf("http://localhost:%v", port),
		PasswordCheck: true,
		Storage: config.StorageConf{
			Type:            config.StorageTypeSQLite,
			ConnStr:         ":memory:",
			MaxIdleConns:    1,
			MaxOpenConns:    1,
			ConnMaxIdleTime: -1,
			ConnMaxLifetime: -1,
		},
	}
	os.Setenv("SCQL_ROOT_PASSWORD", userRootPassword)
	storage.InitPasswordValidation(config.PasswordCheck)
	db, err := server.NewDbConnWithBootstrap(&config.Storage)
	r.NoError(err)
	r.NoError(mock.MockStorage(db)) // user already create here

	// mock engine client
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEngineClient := executor.NewMockEngineClient(ctrl)

	s, err := server.NewServer(config, db, mockEngineClient)
	r.NoError(err)

	done := make(chan bool)
	go func() {
		if err := s.Serve(listener); err != nil {
			log.Printf("SCDB HTTP server %v", err)
		}
		done <- true

	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := s.Shutdown(ctx)
		r.Nil(err)
		<-done
	}()

	// Wait one second for HTTP server to start
	time.Sleep(time.Second)
	serverHealthURL := "http://localhost:" + fmt.Sprint(port) + "/public/health_check"
	caseHealthCheck(r, serverHealthURL)

	serverSubmitURL := "http://localhost:" + fmt.Sprint(port) + client.SubmitPath
	serverFetchURL := "http://localhost:" + fmt.Sprint(port) + client.FetchPath
	caseInvalidRequest(r, serverSubmitURL, serverFetchURL)

	stub := client.NewClient("http://localhost:"+fmt.Sprint(port), &http.Client{Timeout: 3 * time.Second}, 50, 200*time.Millisecond)
	caseSubmit(r, stub)
	caseSubmitAndGet(r, stub)
	caseFetch(r, stub)
	caseQuery(r, stub)
	caseQueryWithQueryResultCallback(r, stub)
}

func newUserCredential(user, pwd string) *scql.SCDBCredential {
	return &scql.SCDBCredential{
		User: &scql.User{
			AccountSystemType: scql.User_NATIVE_USER,
			User: &scql.User_NativeUser_{
				NativeUser: &scql.User_NativeUser{
					Name:     user,
					Password: pwd,
				},
			},
		},
	}
}

func caseHealthCheck(a *require.Assertions, url string) {
	resp, err := http.Get(url)
	a.Nil(err)
	body, err := io.ReadAll(resp.Body)
	a.Nil(err)
	a.Equal("ok", string(body))
}

func caseInvalidRequest(a *require.Assertions, submitUrl, fetchUrl string) {
	// invalid request for submit
	{
		resp, err := http.Post(submitUrl, "application/json", strings.NewReader("invalid_proto_text"))
		a.Nil(err)
		a.Equal(http.StatusOK, resp.StatusCode)
		response := scql.SCDBSubmitResponse{}
		_, err = message.DeserializeFrom(resp.Body, &response, resp.Header.Get("Content-Type"))
		a.Nil(err)
		a.NotEqual(int32(scql.Code_OK), response.Status.Code)
	}

	// invalid request for fetch
	{
		resp, err := http.Post(fetchUrl, "application/json", strings.NewReader("invalid_proto_text"))
		a.Nil(err)
		a.Equal(http.StatusOK, resp.StatusCode)
		response := scql.SCDBQueryResultResponse{}
		_, err = message.DeserializeFrom(resp.Body, &response, resp.Header.Get("Content-Type"))
		a.Nil(err)
		a.NotEqual(int32(scql.Code_OK), response.Status.Code)
	}
}

func caseSubmit(a *require.Assertions, stub *client.Client) {
	// invalid user
	for _, pair := range [][2]string{{`root`, `wrong_password`}, {`i_dont_exists`, `password`}} {
		user, pwd := pair[0], pair[1]
		response, err := stub.Submit(newUserCredential(user, pwd), "")
		a.Nil(err)
		a.NotEqual(int32(scql.Code_OK), response.Status.Code)
		a.Equal(fmt.Sprintf("user %s authentication failed", user), response.Status.Message)
	}
}

func caseSubmitAndGet(a *require.Assertions, stub *client.Client) {
	// invalid sql statement
	{
		response, err := stub.SubmitAndGet(userRoot, `i am not a sql string`)
		a.Nil(err)
		a.NotEqual(int32(scql.Code_OK), response.Status.Code)
	}

	// invalid password
	{
		response, err := stub.SubmitAndGet(userRoot, `CREATE USER temp_user1 PARTY_CODE "party_temp" IDENTIFIED BY "alice123"`)
		a.Nil(err)
		a.NotEqual(int32(scql.Code_OK), response.Status.Code)
	}

	// success statement
	{
		response, err := stub.SubmitAndGet(userRoot, `CREATE USER temp_user1 PARTY_CODE "party_temp" IDENTIFIED BY "Alice_password_123"`)
		a.Nil(err)
		a.Equal(int32(scql.Code_OK), response.Status.Code)
	}
}

func caseFetch(a *require.Assertions, stub *client.Client) {

	// invalid session id
	{
		resp, err := stub.Fetch(userRoot, "invalid_session_id")
		a.Nil(err)
		a.NotEqual(int32(scql.Code_OK), resp.Status.Code)
	}

	// invalid user
	{
		response, err := stub.Submit(userRoot, `CREATE USER if not exists alice PARTY_CODE "party_alice" IDENTIFIED BY "Alice_password_123"`)
		a.Nil(err)

		// invalid user&pwd
		resp, err := stub.Fetch(newUserCredential("i_dont_exists", "password"), response.ScdbSessionId)
		a.Nil(err)
		a.NotEqual(int32(scql.Code_OK), resp.Status.Code)

		// submit/fetch should be the same user
		resp, err = stub.Fetch(userAlice, response.ScdbSessionId)
		a.Nil(err)
		a.NotEqual(int32(scql.Code_OK), resp.Status.Code)
	}
}

func caseQuery(a *require.Assertions, stub *client.Client) {
	// invalid sql statement
	{
		response, err := stub.Submit(userRoot, `i am not a sql string`)
		a.Nil(err)
		a.NotEqual(int32(scql.Code_OK), response.Status.Code)
	}

	// success statement
	{
		response, err := stub.Submit(userRoot, `CREATE USER temp_user PARTY_CODE "another_party" IDENTIFIED BY "Password_123456789"`)
		a.Nil(err)
		a.Equal(int32(scql.Code_OK), response.Status.Code)

		sessionId := response.ScdbSessionId
		fetchResp, err := stub.Fetch(userRoot, sessionId)
		a.Nil(err)
		a.Equal(int32(scql.Code_OK), fetchResp.Status.Code)
	}
}

func caseQueryWithQueryResultCallback(a *require.Assertions, stub *client.Client) {
	cb := &cbMockServer{}
	ts := httptest.NewServer(setupQueryResultCbServer(cb))
	defer ts.Close()
	cbURL := fmt.Sprintf("%s/fetch_result", ts.URL)

	stub.SetCallbackURL(cbURL)
	defer func() {
		stub.SetCallbackURL("")
	}()
	var sessionID string
	{
		sql := "DESCRIBE test.table_1"
		resp, err := stub.Submit(userRoot, sql)
		a.NoError(err)
		a.Equal(int32(scql.Code_OK), resp.Status.Code)

		sessionID = resp.ScdbSessionId
	}

	{
		resp, err := stub.Fetch(userRoot, sessionID)
		a.NoError(err)

		a.Equal(int32(scql.Code_OK), resp.Status.Code)
	}

}

type cbMockServer struct {
	result          *scql.SCDBQueryResultResponse
	reqReceiveCount int
	err             error
}

func (cb *cbMockServer) ReportQueryResult(c *gin.Context) {
	cb.reqReceiveCount++

	var result scql.SCDBQueryResultResponse
	_, err := message.DeserializeFrom(c.Request.Body, &result, "")
	if err != nil {
		cb.err = err
		return
	}

	cb.result = &result

}

func setupQueryResultCbServer(cb *cbMockServer) *gin.Engine {
	r := gin.Default()
	r.POST("/fetch_result", cb.ReportQueryResult)

	return r
}

func TestSCDBServerForPipeline(t *testing.T) {
	r := require.New(t)

	config := &config.Config{
		Port:          0,
		PasswordCheck: true,
		Storage: config.StorageConf{
			Type:            config.StorageTypeSQLite,
			ConnStr:         ":memory:",
			MaxIdleConns:    1,
			MaxOpenConns:    1,
			ConnMaxIdleTime: -1,
			ConnMaxLifetime: -1,
		},
	}
	os.Setenv("SCQL_ROOT_PASSWORD", userRootPassword)
	storage.InitPasswordValidation(config.PasswordCheck)
	db, err := server.NewDbConnWithBootstrap(&config.Storage)
	r.NoError(err)

	// mock engine client
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEngineClient := executor.NewMockEngineClient(ctrl)

	s, err := server.NewServer(config, db, mockEngineClient)
	r.NoError(err)

	listener, err := net.Listen("tcp", s.Addr)
	r.NoError(err)

	port := fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
	log.Printf("SCDB4Pipeline is listening on :%s\n", port)

	done := make(chan bool)
	go func() {
		if err := s.Serve(listener); err != nil {
			log.Printf("SCDB4Pipeline HTTP server %v", err)
		}
		done <- true

	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := s.Shutdown(ctx)
		r.Nil(err)
		<-done
	}()

	// Wait one second for HTTP server to start
	time.Sleep(time.Second)

	stub := client.NewClient("http://localhost:"+port, &http.Client{Timeout: 3 * time.Second}, 50, 200*time.Millisecond)
	mustRun := func(user *scql.SCDBCredential, sql string) []*scql.Tensor {
		response, err := stub.SubmitAndGet(user, sql)
		r.NoError(err)
		r.Equal(int32(scql.Code_OK), response.Status.Code, response.String())
		r.Equal(int32(0), response.Status.Code)

		// table, err := result.NewTableFromTensors(response.OutColumns)
		r.NoError(err)
		return response.OutColumns
	}

	casePipeline(r, mustRun)
}

func casePipeline(r *require.Assertions, mustRun func(user *scql.SCDBCredential, sql string) []*scql.Tensor) {
	// 1.1 root> create user bob
	mustRun(userRoot, `CREATE USER alice PARTY_CODE "ALICE" IDENTIFIED BY "Alice_password_123"`)
	// 1.2 root> create user alice
	mustRun(userRoot, `CREATE USER bob PARTY_CODE "BOB" IDENTIFIED BY "Bob_password_123"`)
	// 1.3 root> create database
	mustRun(userRoot, `CREATE DATABASE dbtest`)
	// 1.4 root> grant to bob, alice
	mustRun(userRoot, `GRANT CREATE,GRANT OPTION ON dbtest.* TO alice`)
	mustRun(userRoot, `GRANT CREATE,GRANT OPTION ON dbtest.* TO bob`)

	result := mustRun(userRoot, `SHOW DATABASES`)
	if len(result) == 0 {
		return
	}

	// 2.1 alice> create table
	mustRun(userAlice, `CREATE TABLE dbtest.t1 (column1_1 int, column1_2 int, column1_3 int) REF_TABLE=test.table_1 DB_TYPE='mysql'`)
	mustRun(userBob, `CREATE TABLE dbtest.t2 (column2_1 int, column2_2 int) REF_TABLE=test.table_2 DB_TYPE='mysql'`)
	mustRun(userAlice, `CREATE TABLE dbtest.t3 (column3_1 int, column3_2 int) REF_TABLE=test.table_3 DB_TYPE='mysql'`)

	// 2.2 alice > grant ccl to alice&bob
	mustRun(userAlice, `GRANT SELECT PLAINTEXT(column1_1), SELECT ENCRYPTED_ONLY(column1_2) ON dbtest.t1 TO bob`)
	mustRun(userAlice, `GRANT SELECT PLAINTEXT(column3_1), SELECT PLAINTEXT_AFTER_JOIN(column3_2) ON dbtest.t3 TO bob`)
	mustRun(userBob, `GRANT SELECT PLAINTEXT(column2_1), SELECT PLAINTEXT_AFTER_JOIN(column2_2) ON dbtest.t2 TO alice`)

	// 2.3 alice > show
	mustRun(userAlice, `SHOW GRANTS ON dbtest FOR alice;`)
	mustRun(userAlice, `SHOW GRANTS ON dbtest FOR bob;`)
	mustRun(userAlice, `SHOW TABLES FROM dbtest;`)

}
