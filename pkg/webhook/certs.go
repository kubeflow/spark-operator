/*
Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

import (
	"crypto/tls"
	"io/ioutil"
	"sync"
	"time"

	"github.com/golang/glog"
)

// certProvider is a container of a X509 certificate file and a corresponding key file for the
// webhook server, and a CA certificate file for the API server to verify the server certificate.
type certProvider struct {
	serverCertFile   string
	serverKeyFile    string
	caCertFile       string
	reloadInterval   time.Duration
	ticker           *time.Ticker
	stopChannel      chan interface{}
	currentCert      *tls.Certificate
	certPointerMutex *sync.RWMutex
}

func NewCertProvider(serverCertFile, serverKeyFile, caCertFile string, reloadInterval time.Duration) (*certProvider, error) {
	cert, err := tls.LoadX509KeyPair(serverCertFile, serverKeyFile)
	if err != nil {
		return nil, err
	}
	return &certProvider{
		serverCertFile:   serverCertFile,
		serverKeyFile:    serverKeyFile,
		caCertFile:       caCertFile,
		reloadInterval:   reloadInterval,
		currentCert:      &cert,
		stopChannel:      make(chan interface{}),
		ticker:           time.NewTicker(reloadInterval),
		certPointerMutex: &sync.RWMutex{},
	}, nil
}

func (c *certProvider) Start() {
	go func() {
		for {
			select {
			case <-c.stopChannel:
				return
			case <-c.ticker.C:
				c.updateCert()
			}
		}
	}()
}

func (c *certProvider) tlsConfig() *tls.Config {
	return &tls.Config{
		GetCertificate: func(ch *tls.ClientHelloInfo) (*tls.Certificate, error) {
			c.certPointerMutex.RLock()
			defer c.certPointerMutex.RUnlock()
			return c.currentCert, nil
		},
	}
}

func (c *certProvider) Stop() {
	close(c.stopChannel)
	c.ticker.Stop()
}

func (c *certProvider) updateCert() {
	cert, err := tls.LoadX509KeyPair(c.serverCertFile, c.serverKeyFile)
	if err != nil {
		glog.Errorf("could not reload certificate %s (key %s): %v", c.serverCertFile, c.serverKeyFile, err)
		return
	}
	c.certPointerMutex.Lock()
	c.currentCert = &cert
	c.certPointerMutex.Unlock()
}

func readCertFile(certFile string) ([]byte, error) {
	return ioutil.ReadFile(certFile)
}
