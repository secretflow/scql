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

package partymgr

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"

	"github.com/secretflow/kuscia/proto/api/v1alpha1/kusciaapi"
	"google.golang.org/grpc"

	"github.com/secretflow/scql/pkg/interpreter/graph"
)

type kusciaPartyMgr struct {
	client kusciaapi.DomainServiceClient
}

func NewKusciaPartyMgr(conn grpc.ClientConnInterface) (PartyMgr, error) {
	client := kusciaapi.NewDomainServiceClient(conn)
	return &kusciaPartyMgr{
		client: client,
	}, nil
}

func (k *kusciaPartyMgr) GetBrokerUrlByParty(party string) (string, error) {
	return fmt.Sprintf("http://scql-broker-inter.%s.svc", party), nil
}

func (k *kusciaPartyMgr) GetPubKeyByParty(party string) (string, error) {
	// TODO: add cache to avoid querying kusciaapi too frequently
	req := &kusciaapi.BatchQueryDomainRequest{
		DomainIds: []string{party},
	}
	resp, err := k.client.BatchQueryDomain(context.TODO(), req)
	if err != nil {
		return "", err
	}
	if resp.GetStatus().GetCode() != 0 {
		return "", fmt.Errorf("failed to query kuscia domain %s: code=%d message=%s", party, resp.GetStatus().GetCode(), resp.GetStatus().GetMessage())
	}
	if len(resp.GetData().GetDomains()) != 1 {
		return "", fmt.Errorf("expect only one domain in BatchQueryDomainResponse but got %d", len(resp.GetData().GetDomains()))
	}
	domain := resp.GetData().GetDomains()[0]
	if domain.GetDomainId() != party {
		return "", fmt.Errorf("domain_id=%s in BatchQueryDomainResponse mismatch with requested party %s", domain.GetDomainId(), party)
	}
	return extractPublicKeyFromCertificate(domain.GetCert())
}

// TODO: refactor(DRY) it with filePartyMgr.GetPartyInfoByParties
func (k *kusciaPartyMgr) GetPartyInfoByParties(parties []string) (*graph.PartyInfo, error) {
	var participants []*graph.Participant
	for _, party := range parties {
		participant := &graph.Participant{PartyCode: party, Endpoints: []string{}}
		pubkey, err := k.GetPubKeyByParty(party)
		if err != nil {
			return nil, err
		}
		participant.PubKey = pubkey
		participants = append(participants, participant)
	}
	return graph.NewPartyInfo(participants), nil
}

// extractPublicKeyFromCertificate extract public key from base64 encoded certificate & return its base64 encoded value
func extractPublicKeyFromCertificate(certStr string) (string, error) {
	certPemData, err := base64.StdEncoding.DecodeString(certStr)
	if err != nil {
		return "", err
	}
	for block, rest := pem.Decode(certPemData); block != nil; block, rest = pem.Decode(rest) {
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return "", fmt.Errorf("failed to parse certificate: %v", err)
		}
		pubkey, err := x509.MarshalPKIXPublicKey(cert.PublicKey)
		if err != nil {
			return "", err
		}

		return base64.StdEncoding.EncodeToString(pubkey), nil
	}
	return "", fmt.Errorf("no certificate found in PEM data")
}
