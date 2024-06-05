package webhook

import "testing"

// TestNewCertProvider tests the NewCertProvider function.
func TestNewCertProvider(t *testing.T) {
	name := "test-name"
	namespace := "test-namespace"

	cp, err := NewCertProvider(name, namespace)
	if err != nil {
		t.Errorf("failed to create CertProvider: %v", err)
	}

	// Check if the returned CertProvider has non-nil fields.
	if cp.caKey == nil {
		t.Error("CA key is nil")
	}
	if cp.caCert == nil {
		t.Error("CA certificate is nil")
	}
	if cp.serverKey == nil {
		t.Error("server key is nil")
	}
	if cp.serverCert == nil {
		t.Error("server certificate is nil")
	}
}

// TestCAKey tests the CAKey method of certProvider.
func TestCAKey(t *testing.T) {
	cp, err := NewCertProvider("test-name", "test-namespace")
	if err != nil {
		t.Errorf("failed to create CertProvider: %v", err)
	}

	key, err := cp.CAKey()
	if err != nil {
		t.Errorf("failed to get CA key: %v", err)
	}

	// Check if the returned key is not nil.
	if key == nil {
		t.Error("CA key is nil")
	}
}

// TestCACert tests the CACert method of certProvider.
func TestCACert(t *testing.T) {
	cp, err := NewCertProvider("test-name", "test-namespace")
	if err != nil {
		t.Errorf("failed to create CertProvider: %v", err)
	}

	cert, err := cp.CACert()
	if err != nil {
		t.Errorf("failed to get CA certificate: %v", err)
	}

	// Check if the returned certificate is not nil.
	if cert == nil {
		t.Error("CA certificate is nil")
	}
}

// TestServerKey tests the ServerKey method of certProvider.
func TestServerKey(t *testing.T) {
	cp, err := NewCertProvider("test-name", "test-namespace")
	if err != nil {
		t.Errorf("failed to create CertProvider: %v", err)
	}

	key, err := cp.ServerKey()
	if err != nil {
		t.Errorf("failed to get server key: %v", err)
	}

	// Check if the returned key is not nil.
	if key == nil {
		t.Error("server key is nil")
	}
}

// TestServerCert tests the ServerCert method of certProvider.
func TestServerCert(t *testing.T) {
	cp, err := NewCertProvider("test-name", "test-namespace")
	if err != nil {
		t.Errorf("failed to create CertProvider: %v", err)
	}

	cert, err := cp.ServerCert()
	if err != nil {
		t.Errorf("failed to get server certificate: %v", err)
	}

	// Check if the returned certificate is not nil.
	if cert == nil {
		t.Error("server certificate is nil")
	}
}

// TestTLSConfig tests the TLSConfig method of certProvider.
func TestTLSConfig(t *testing.T) {
	cp, err := NewCertProvider("test-name", "test-namespace")
	if err != nil {
		t.Errorf("failed to create CertProvider: %v", err)
	}

	cfg, err := cp.TLSConfig()
	if err != nil {
		t.Errorf("failed to get TLS configuration: %v", err)
	}

	// Check if the returned configuration is not nil.
	if cfg == nil {
		t.Error("TLS configuration is nil")
	}
}
