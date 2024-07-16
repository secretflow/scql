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
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/config"
	"github.com/secretflow/scql/pkg/interpreter/graph"
)

var (
	_ PartyMgr = &filePartyMgr{}
)

// type Discoverer interface {
// 	FindPeers()
// }

type PartyMgr interface {
	GetBrokerUrlByParty(party string) (string, error)
	GetPubKeyByParty(party string) (string, error)
	GetPartyInfoByParties(parties []string) (*graph.PartyInfo, error)
}

// TODO: renamed to avoid confusion with engine partyInfo structure
type Participant struct {
	PartyCode string `json:"party_code"`
	Endpoint  string `json:"endpoint"`
	Token     string `json:"token"`
	PubKey    string `json:"pubkey"`
}

type BrokerInfo struct {
	Participants []*Participant `json:"participants"`
}

type filePartyMgr struct {
	urlMap    map[string]string
	pubKeyMap map[string]string
}

type ConsulPartyMgr struct {
	urlMap    map[string]string
	pubKeyMap map[string]string
	client    *api.Client
	mu        sync.RWMutex
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

const (
	NoTLS     string = "notls"
	TLS       string = "tls"
	MutualTLS string = "mtls"
)

const consulServiceName string = "broker-scql-secretflow"

func newConsulClient(cfg *config.ConsulApiConf) (*api.Client, error) {
	consulConfig := api.DefaultConfig()

	consulConfig.Address = cfg.Address
	if consulConfig.Address == "" {
		return nil, fmt.Errorf("consul address can't be empty")
	}

	if cfg.Token != "" {
		consulConfig.Token = cfg.Token
	}

	switch strings.ToLower(string(cfg.TLS.Mode)) {
	case NoTLS:
		consulConfig.Scheme = "http"
	case TLS, MutualTLS:
		consulConfig.Scheme = "https"
		tlsConfig, err := config.LoadTLSConfig(cfg.TLS.Mode, cfg.TLS.CACertPath, cfg.TLS.CertPath, cfg.TLS.KeyPath)
		if err != nil {
			return nil, err
		}
		consulConfig.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	default:
		return nil, fmt.Errorf("unknown TLS mode: %s", cfg.TLS.Mode)
	}

	return api.NewClient(consulConfig)
}

func NewConsulPartyMgr(cfg *config.Config, pubKey string) (*ConsulPartyMgr, error) {
	// Create conusl client
	client, err := newConsulClient(cfg.Discovery.Consul)
	if err != nil {
		return nil, fmt.Errorf("failed to create Consul client: %v", err)
	}

	consulPartyMgr := &ConsulPartyMgr{
		urlMap:    make(map[string]string),
		pubKeyMap: make(map[string]string),
		client:    client,
		stopCh:    make(chan struct{}),
	}

	err = consulPartyMgr.registerService(cfg.PartyCode, cfg.InterHost, pubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to register service: %v", err)
	}

	err = consulPartyMgr.discoverServices()
	if err != nil {
		return nil, fmt.Errorf("failed to discover service: %v", err)
	}

	// Starting background service discovery
	go consulPartyMgr.startServiceDiscovery(30 * time.Second)

	return consulPartyMgr, nil
}

func (m *ConsulPartyMgr) registerService(partyCode, interHost, pubKey string) error {
	// Parse interHost to get the host address and port
	parsedURL, err := url.Parse(interHost)
	if err != nil {
		return fmt.Errorf("failed to parse interHost: %v", err)
	}
	host := parsedURL.Hostname()
	portStr := parsedURL.Port()
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("failed to convert port to integer: %v", err)
	}

	registration := &api.AgentServiceRegistration{
		ID:      partyCode,
		Name:    consulServiceName,
		Address: host,
		Port:    port,
		Meta: map[string]string{
			"pubkey":     pubKey,
			"party_code": partyCode,
			"inter_host": interHost,
		},
		// ignore it temporarily
		// Check: &api.AgentServiceCheck{
		// 	HTTP:     fmt.Sprintf("http://%s/health", interHost),
		// 	Interval: "10s",
		// 	Timeout:  "1s",
		// },
	}

	err = m.client.Agent().ServiceRegister(registration)
	if err != nil {
		return fmt.Errorf("failed to register service with Consul: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.urlMap[partyCode] = interHost
	m.pubKeyMap[partyCode] = pubKey

	return nil
}

func (m *ConsulPartyMgr) discoverServices() error {
	// Get all services named "broker.scql.secretflow"
	services, _, err := m.client.Health().Service(consulServiceName, "", true, nil)
	if err != nil {
		return fmt.Errorf("failed to discover services: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, service := range services {
		oldURL, urlExists := m.urlMap[service.Service.ID]
		oldPubKey, pubKeyExists := m.pubKeyMap[service.Service.ID]

		m.urlMap[service.Service.ID] = service.Service.Meta["inter_host"]
		m.pubKeyMap[service.Service.ID] = service.Service.Meta["pubkey"]

		if !urlExists || !pubKeyExists || oldURL != service.Service.Meta["inter_host"] || oldPubKey != service.Service.Meta["pubkey"] {
			logrus.Infof("Service register/change detected: ServiceID: %v, URL: %v, PubKey: %v", service.Service.ID, service.Service.Meta["inter_host"], service.Service.Meta["pubkey"])
		}
	}

	return nil
}

func (m *ConsulPartyMgr) startServiceDiscovery(interval time.Duration) {
	m.wg.Add(1)
	defer m.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := m.discoverServices()
			if err != nil {
				fmt.Printf("Failed to discover services: %v\n", err)
			}
		case <-m.stopCh:
			return
		}
	}
}

func (m *ConsulPartyMgr) StopServiceDiscovery() {
	close(m.stopCh)
	m.wg.Wait()
}

func (m *ConsulPartyMgr) GetBrokerUrlByParty(party string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	url, ok := m.urlMap[party]
	if !ok {
		return "", fmt.Errorf("GetBrokerUrlByParty: no url for party: %v", party)
	}
	return url, nil
}

func (m *ConsulPartyMgr) GetPubKeyByParty(party string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pubKey, ok := m.pubKeyMap[party]
	if !ok {
		return "", fmt.Errorf("GetPubKeyByParty: no pubKey for party: %v", party)
	}
	return pubKey, nil
}

func (m *ConsulPartyMgr) GetPartyInfoByParties(parties []string) (*graph.PartyInfo, error) {
	var participants []*graph.Participant
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, party := range parties {
		participant := &graph.Participant{PartyCode: party, Endpoints: []string{}}
		// find pub key
		pubKey, exist := m.pubKeyMap[party]
		if !exist {
			return nil, fmt.Errorf("GetPartyInfoByParties: failed to find pub key for %s", party)
		}
		participant.PubKey = pubKey
		participants = append(participants, participant)
	}
	return graph.NewPartyInfo(participants), nil
}

func NewFilePartyMgr(partyPath string) (PartyMgr, error) {
	content, err := os.ReadFile(partyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %+v", partyPath, err)
	}

	var partyInfo BrokerInfo
	err = json.Unmarshal(content, &partyInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal from json: %v", err)
	}
	filePartyMgr := filePartyMgr{
		urlMap:    map[string]string{},
		pubKeyMap: map[string]string{},
	}
	for _, party := range partyInfo.Participants {
		filePartyMgr.urlMap[party.PartyCode] = party.Endpoint
		filePartyMgr.pubKeyMap[party.PartyCode] = party.PubKey
	}

	return &filePartyMgr, nil

}

func (m *filePartyMgr) GetBrokerUrlByParty(party string) (string, error) {
	url, ok := m.urlMap[party]
	if !ok {
		return "", fmt.Errorf("GetBrokerUrlByParty: no url for party: %v", party)
	}
	return url, nil
}

func (m *filePartyMgr) GetPubKeyByParty(party string) (string, error) {
	pubKey, ok := m.pubKeyMap[party]
	if !ok {
		return "", fmt.Errorf("GetPubKeyByParty: no pubKey for party: %v", party)
	}
	return pubKey, nil
}

func (m *filePartyMgr) GetPartyInfoByParties(parties []string) (*graph.PartyInfo, error) {
	var participants []*graph.Participant
	for _, party := range parties {
		participant := &graph.Participant{PartyCode: party, Endpoints: []string{}}
		// find pub key
		pubKey, exist := m.pubKeyMap[party]
		if !exist {
			return nil, fmt.Errorf("GetPartyInfoByParties: failed to find pub key for %s", party)
		}
		participant.PubKey = pubKey
		participants = append(participants, participant)
	}
	return graph.NewPartyInfo(participants), nil
}
