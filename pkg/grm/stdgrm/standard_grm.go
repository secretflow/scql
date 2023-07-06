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

package stdgrm

import (
	"bytes"
	"fmt"
	"net/http"

	"google.golang.org/protobuf/proto"

	"github.com/secretflow/scql/pkg/executor"
	"github.com/secretflow/scql/pkg/grm"
	grmproto "github.com/secretflow/scql/pkg/proto-gen/grm"
	"github.com/secretflow/scql/pkg/util/message"
	urlutil "github.com/secretflow/scql/pkg/util/url"
)

var (
	_ grm.Grm = &GrmInfoStd{}
)

type GrmInfoStd struct {
	client executor.SimpleHttpClient
	host   string
}

func (c *GrmInfoStd) SetClient(client executor.SimpleHttpClient) {
	c.client = client
}

func New(client executor.SimpleHttpClient, host string) *GrmInfoStd {
	return &GrmInfoStd{
		client: client,
		host:   host,
	}
}

func (c *GrmInfoStd) postInternal(url, requestBody string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte(requestBody)))
	if err != nil {
		return nil, err
	}
	return c.client.Do(req)
}

func (c *GrmInfoStd) post(url string, pbRequest proto.Message, pbResponse proto.Message) error {
	requestBody, err := message.SerializeTo(pbRequest, message.EncodingTypeJson)
	if err != nil {
		return fmt.Errorf("failed to serialize url:%s, request:%v, error:%v", url, requestBody, err)
	}
	response, err := c.postInternal(url, requestBody)
	if err != nil {
		return fmt.Errorf("failed to post url:%s, request:%v, error:%v", url, requestBody, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to post url:%s, request:%v, statusCode: %v", url, requestBody, response.StatusCode)
	}
	_, err = message.DeserializeFrom(response.Body, pbResponse)
	if err != nil {
		return fmt.Errorf("failed to deserialize url:%s, request:%v, error:%v", url, requestBody, err)
	}
	return nil
}

func (c *GrmInfoStd) GetTableMeta(tid, requestParty, token string) (*grm.TableSchema, error) {
	request := &grmproto.GetTableMetaRequest{
		Tid:          tid,
		RequestParty: requestParty,
		Token:        token,
	}
	var response grmproto.GetTableMetaResponse
	err := c.post(urlutil.JoinHostPath(c.host, "/GetTableMeta"), request, &response)
	if err != nil {
		return nil, err
	}
	if response.GetSchema() == nil {
		return nil, fmt.Errorf("fail to get schema for tid(%s) due to nil schema", tid)
	}
	var columnDesc []*grm.ColumnDesc
	for _, column := range response.GetSchema().Columns {
		columnDesc = append(columnDesc, &grm.ColumnDesc{
			Name:        column.Name,
			Type:        column.Type,
			Description: column.Description,
		})
	}
	return &grm.TableSchema{
		DbName:    response.GetSchema().DbName,
		TableName: response.GetSchema().TableName,
		Columns:   columnDesc,
		DBType:    response.GetDbType(),
	}, nil
}

func (c *GrmInfoStd) GetEngines(partyCodes []string, token string) ([]*grm.EngineInfo, error) {
	request := &grmproto.GetEnginesRequest{
		PartyCodes: partyCodes,
		Token:      token,
	}
	var response grmproto.GetEnginesResponse
	err := c.post(urlutil.JoinHostPath(c.host, "/GetEngines"), request, &response)
	if err != nil {
		return nil, err
	}
	engineInfo := []*grm.EngineInfo{}
	for _, engine := range response.GetEngineInfos() {
		engineInfo = append(engineInfo, &grm.EngineInfo{
			Endpoints:  engine.GetEndpoints(),
			Credential: engine.GetCredential(),
		})
	}
	return engineInfo, nil
}

func (c *GrmInfoStd) VerifyTableOwnership(tid, token string) (bool, error) {
	request := &grmproto.VerifyTableOwnershipRequest{
		Tid:   tid,
		Token: token,
	}
	var response grmproto.VerifyTableOwnershipResponse
	err := c.post(urlutil.JoinHostPath(c.host, "/VerifyTableOwnership"), request, &response)
	if err != nil {
		return false, err
	}
	return response.GetIsOwner(), nil
}
