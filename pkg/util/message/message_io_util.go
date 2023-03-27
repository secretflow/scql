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
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type ContentEncodingType int

const (
	EncodingTypeUnknown ContentEncodingType = iota + 1
	EncodingTypeJson
	EncodingTypeProtobuf
)

var ContentType2EncodingType = map[string]ContentEncodingType{
	"application/json":       EncodingTypeJson,
	"application/x-protobuf": EncodingTypeProtobuf,
}

var maxPrintContentLength = 1000

func DeserializeFrom(in io.ReadCloser, request proto.Message) (ContentEncodingType, error) {
	if request == nil {
		return EncodingTypeUnknown, fmt.Errorf("unexpected nil request")
	}
	body, err := io.ReadAll(in)
	if err != nil {
		return EncodingTypeUnknown, err
	}
	// try parse order(fast&strict -> slow&loose): 1. protobuf 2. json
	err = proto.Unmarshal(body, request)
	if err == nil {
		return EncodingTypeProtobuf, nil
	}
	err = protojson.Unmarshal(body, request)
	if err == nil {
		return EncodingTypeJson, nil
	}
	highLimit := len(body)
	if highLimit > maxPrintContentLength {
		highLimit = maxPrintContentLength
	}
	return EncodingTypeUnknown, fmt.Errorf("got unexpected error=\"%w\" while unmarshal proto from content=%s", err, string(body[:highLimit]))
}

func SerializeTo(response proto.Message, encodingType ContentEncodingType) (content string, err error) {
	switch encodingType {
	case EncodingTypeProtobuf:
		c, err := proto.Marshal(response)
		return string(c), err
	case EncodingTypeJson:
		options := &protojson.MarshalOptions{
			Multiline:       false,
			Indent:          "",
			UseProtoNames:   true,
			EmitUnpopulated: true,
		}
		c, err := options.Marshal(response)
		return string(c), err
	default:
		return "", fmt.Errorf("not implemented")
	}
}
