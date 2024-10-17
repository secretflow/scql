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

	"github.com/sirupsen/logrus"
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

var EncodingType2ContentType = map[ContentEncodingType]string{
	EncodingTypeJson:     "application/json",
	EncodingTypeProtobuf: "application/x-protobuf",
}

var (
	ProtoMarshal   = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal
	ProtoUnmarshal = protojson.UnmarshalOptions{}.Unmarshal
)

var maxPrintContentLength = 1000

func DeserializeFrom(in io.ReadCloser, request proto.Message, contentType string) (ContentEncodingType, error) {
	if request == nil {
		return EncodingTypeUnknown, fmt.Errorf("unexpected empty message")
	}
	body, err := io.ReadAll(in)
	if err != nil {
		return EncodingTypeUnknown, err
	}
	switch contentType {
	case EncodingType2ContentType[EncodingTypeJson]:
		err = ProtoUnmarshal(body, request)
		if err == nil {
			return EncodingTypeJson, nil
		}
	case EncodingType2ContentType[EncodingTypeProtobuf]:
		err = proto.Unmarshal(body, request)
		if err == nil {
			return EncodingTypeProtobuf, nil
		}
	default:
		if contentType == "" {
			logrus.Warning("empty content-type")
		} else {
			logrus.Warningf("content-type (%s) is not one of application/json and application/x-protobuf", contentType)
		}

		// Try to parse protobuf first, then json
		err = proto.Unmarshal(body, request)
		if err == nil {
			return EncodingTypeProtobuf, nil
		}
		err = ProtoUnmarshal(body, request)
		if err == nil {
			return EncodingTypeJson, nil
		}
	}

	highLimit := len(body)
	if highLimit > maxPrintContentLength {
		highLimit = maxPrintContentLength
	}
	return EncodingTypeUnknown, fmt.Errorf("got unexpected error=\"%w\" while unmarshal proto from content=%s", err, string(body[:highLimit]))
}

func SerializeTo(response proto.Message, encodingType ContentEncodingType) (content string, err error) {
	if response == nil {
		return "", fmt.Errorf("unexpected empty message")
	}
	switch encodingType {
	case EncodingTypeProtobuf:
		c, err := proto.Marshal(response)
		return string(c), err
	case EncodingTypeJson:
		c, err := ProtoMarshal(response)
		return string(c), err
	default:
		return "", fmt.Errorf("not implemented")
	}
}
