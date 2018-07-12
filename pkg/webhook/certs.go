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
)

// certBundle is a container of a X509 certificate file and a corresponding key file for the
// webhook server, and a CA certificate file for the API server to verify the server certificate.
type certBundle struct {
	serverCertFile string
	serverKeyFile  string
	caCertFile     string
}

// configServerTLS configures TLS for the admission webhook server.
func configServerTLS(certBundle *certBundle) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certBundle.serverCertFile, certBundle.serverKeyFile)
	if err != nil {
		return nil, err
	}

	return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
}

func readCertFile(certFile string) ([]byte, error) {
	return ioutil.ReadFile(certFile)
}
