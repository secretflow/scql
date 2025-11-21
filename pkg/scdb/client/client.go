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

package client

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
	urlutil "github.com/secretflow/scql/pkg/util/url"
)

const (
	SubmitPath       = `/public/submit_query`
	SubmitAndGetPath = `/public/submit_and_get`
	FetchPath        = `/public/fetch_result`
)

//go:generate mockgen -source client.go -destination client_mock.go -package client
type httpClientI interface {
	Post(url, contentType string, body io.Reader) (resp *http.Response, err error)
}

type Client struct {
	serverHost    string
	dbName        string
	callbackURL   string
	c             httpClientI
	maxFetchNum   int
	fetchInterval time.Duration
}

func NewDefaultClient(host string, c httpClientI) *Client {
	return &Client{
		serverHost:    host,
		c:             c,
		maxFetchNum:   300,
		fetchInterval: time.Second,
	}
}

func NewClient(host string, c httpClientI, maxFetchNum int, fetchInterval time.Duration) *Client {
	return &Client{
		serverHost:    host,
		c:             c,
		maxFetchNum:   maxFetchNum,
		fetchInterval: fetchInterval,
	}
}

func (c *Client) SetDBName(dbName string) {
	c.dbName = dbName
}

func (c *Client) SetCallbackURL(callbackURL string) {
	c.callbackURL = callbackURL
}

func (c *Client) Submit(user *scql.SCDBCredential, sql string) (*scql.SCDBSubmitResponse, error) {
	req := &scql.SCDBQueryRequest{
		User:                   user,
		Query:                  sql,
		QueryResultCallbackUrl: c.callbackURL,
		BizRequestId:           "",
		DbName:                 c.dbName,
	}
	requestStr, err := message.SerializeTo(req, message.EncodingTypeJson)
	if err != nil {
		return nil, err
	}
	resp, err := c.c.Post(urlutil.JoinHostPath(c.serverHost, SubmitPath),
		"application/json", strings.NewReader(requestStr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	response := &scql.SCDBSubmitResponse{}
	_, err = message.DeserializeFrom(resp.Body, response, resp.Header.Get("Content-Type"))
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Client) SubmitAndGet(user *scql.SCDBCredential, sql string) (*scql.SCDBQueryResultResponse, error) {
	req := &scql.SCDBQueryRequest{
		User:         user,
		Query:        sql,
		BizRequestId: "",
		DbName:       c.dbName,
	}
	requestStr, err := message.SerializeTo(req, message.EncodingTypeJson)
	if err != nil {
		return nil, err
	}
	resp, err := c.c.Post(urlutil.JoinHostPath(c.serverHost, SubmitAndGetPath),
		"application/json", strings.NewReader(requestStr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	response := &scql.SCDBQueryResultResponse{}
	_, err = message.DeserializeFrom(resp.Body, response, resp.Header.Get("Content-Type"))
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Client) Fetch(user *scql.SCDBCredential, sessionId string) (*scql.SCDBQueryResultResponse, error) {
	count := 0
	for {
		if c.maxFetchNum > 0 && count > c.maxFetchNum {
			return nil, fmt.Errorf("exceed max number of fetch number %v", c.maxFetchNum)
		}
		count += 1

		<-time.After(c.fetchInterval)

		response, err := c.FetchOnce(user, sessionId)
		if err != nil {
			return nil, err
		}
		if response.Status.Code == int32(scql.Code_NOT_READY) {
			continue
		}
		return response, nil
	}
}

func (c *Client) FetchOnce(user *scql.SCDBCredential, sessionId string) (*scql.SCDBQueryResultResponse, error) {
	req := &scql.SCDBFetchRequest{
		User:          user,
		ScdbSessionId: sessionId,
	}
	requestStr, err := message.SerializeTo(req, message.EncodingTypeJson)
	if err != nil {
		return nil, err
	}
	resp, err := c.c.Post(urlutil.JoinHostPath(c.serverHost, FetchPath),
		"application/json", strings.NewReader(requestStr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	response := &scql.SCDBQueryResultResponse{}
	if _, err = message.DeserializeFrom(resp.Body, response, resp.Header.Get("Content-Type")); err != nil {
		return nil, err
	}
	return response, nil
}
