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

package message

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestSerializeResponseToProtobuf(t *testing.T) {
	// GIVEN
	a := require.New(t)
	var resp scql.SCDBSubmitResponse
	resp.ScdbSessionId = "abc123"
	expectResponseContent := "\x12\x06abc123"
	// WHEN
	actualContent, err := SerializeTo(&resp, EncodingTypeProtobuf)
	// THEN
	a.Nil(err)
	a.Equal(expectResponseContent, actualContent)
}

func TestSerializeResponseToJson(t *testing.T) {
	// GIVEN
	a := require.New(t)
	var response scql.SCDBSubmitResponse
	response.ScdbSessionId = "abc123"
	expectResponseContent := `{"status":null, "scdb_session_id":"abc123"}`
	// WHEN
	actualContent, err := SerializeTo(&response, EncodingTypeJson)
	// THEN
	a.Nil(err)
	a.JSONEq(expectResponseContent, actualContent, "expected equal")
}

func TestSerializeResponseToJsonEmpty(t *testing.T) {
	// GIVEN
	a := require.New(t)
	var response scql.SCDBSubmitResponse
	expectResponseContent := `{"scdb_session_id":"", "status":null}`
	// WHEN
	actualContent, err := SerializeTo(&response, EncodingTypeJson)
	// THEN
	a.Nil(err)
	a.JSONEq(expectResponseContent, actualContent, "expected equal")
}

func TestDeserializeRequestFromJsonEmpty(t *testing.T) {
	// GIVEN
	a := require.New(t)
	// lower case with under score style field
	requestBody := `{"scdb_session_id": ""}`
	in := io.NopCloser(strings.NewReader(requestBody))
	// WHEN
	var request scql.SCDBFetchRequest
	encodingType, err := DeserializeFrom(in, &request, "")
	// THEN
	a.Nil(err)
	a.Equal(EncodingTypeJson, encodingType)
	a.Equal("", request.ScdbSessionId)
}

func TestDeserializeRequestFromJson1(t *testing.T) {
	// GIVEN
	a := require.New(t)
	// lower case with under score style field
	requestBody := `{"scdb_session_id": "abc123"}`
	in := io.NopCloser(strings.NewReader(requestBody))
	// WHEN
	var request scql.SCDBFetchRequest
	encodingType, err := DeserializeFrom(in, &request, "")
	// THEN
	a.Nil(err)
	a.Equal(EncodingTypeJson, encodingType)
	a.Equal("abc123", request.ScdbSessionId)
}

func TestDeserializeRequestFromJson2(t *testing.T) {
	// GIVEN
	a := require.New(t)
	// camel style
	requestBody := `{"scdbSessionId":"abc123"}`
	in := io.NopCloser(strings.NewReader(requestBody))
	// WHEN
	var request scql.SCDBFetchRequest
	encodingType, err := DeserializeFrom(in, &request, "")
	// THEN
	a.Nil(err)
	a.Equal(EncodingTypeJson, encodingType)
	a.Equal("abc123", request.ScdbSessionId)
}

func TestSerializeRequestToProtobufBinary(t *testing.T) {
	// GIVEN
	a := require.New(t)
	request := scql.SCDBFetchRequest{
		ScdbSessionId: "abc123",
	}
	// WHEN
	requestBody, err := SerializeTo(&request, EncodingTypeProtobuf)
	// THEN
	a.Nil(err)
	expectedRequestBody, _ := proto.Marshal(&request)
	a.Equal(string(expectedRequestBody), requestBody)
}

func TestDeserializeRequestFromProtobufBinary(t *testing.T) {
	// GIVEN
	a := require.New(t)
	request := scql.SCDBFetchRequest{
		ScdbSessionId: "abc123",
	}
	// WHEN 1
	requestBody, err := SerializeTo(&request, EncodingTypeProtobuf)
	// THEN 1
	a.Nil(err)
	expectedRequestBody, _ := proto.Marshal(&request)
	a.Equal(string(expectedRequestBody), requestBody)
	// WHEN 2
	in := io.NopCloser(strings.NewReader(requestBody))
	actualRequest := scql.SCDBFetchRequest{}
	encodingType, err := DeserializeFrom(in, &actualRequest, "")
	// THEN 2
	a.Nil(err)
	a.Equal(EncodingTypeProtobuf, encodingType)
	a.Equal("abc123", request.ScdbSessionId)
}

func TestDeserializeRequestFromInvalidRequest(t *testing.T) {
	// GIVEN
	a := require.New(t)
	requestBody := `invalid_content_here_xxx`
	in := io.NopCloser(strings.NewReader(requestBody))
	// WHEN
	var request scql.SCDBFetchRequest
	encodingType, err := DeserializeFrom(in, &request, "")
	// THEN
	a.NotNil(err)
	a.Equal(EncodingTypeUnknown, encodingType)
}

func TestSerializeResponseToUnknown(t *testing.T) {
	// GIVEN
	a := require.New(t)
	var response scql.SCDBFetchRequest
	response.ScdbSessionId = "abc123"
	// WHEN
	_, err := SerializeTo(&response, EncodingTypeUnknown)
	// THEN
	a.NotNil(err)
}

func TestDeserializeResponseFromEmpty(t *testing.T) {
	a := require.New(t)
	body, err := SerializeTo(nil, EncodingTypeProtobuf)
	a.NotNil(err)
	a.Equal(body, "")

	var response scql.SCDBSubmitResponse

	in := io.NopCloser(strings.NewReader(body))
	encodingType, err := DeserializeFrom(in, &response, "")
	a.Nil(err)
	a.Equal(EncodingTypeProtobuf, encodingType)
}
