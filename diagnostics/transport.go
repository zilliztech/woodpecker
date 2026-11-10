package diagnostics

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

// TLSConfig contains private, operator-selected certificate paths. It is never
// accepted from a browser request or included in diagnostic evidence.
type TLSConfig struct {
	CAFile     string `yaml:"ca_file,omitempty"`
	CertFile   string `yaml:"cert_file,omitempty"`
	KeyFile    string `yaml:"key_file,omitempty"`
	ServerName string `yaml:"server_name,omitempty"`
}

func HTTPTransport(s TLSConfig, baseDir string) (*http.Transport, error) {
	t := http.DefaultTransport.(*http.Transport).Clone()
	if s.CAFile == "" && s.CertFile == "" && s.KeyFile == "" && s.ServerName == "" {
		return t, nil
	}
	if (s.CertFile == "") != (s.KeyFile == "") {
		return nil, fmt.Errorf("TLS requires both cert_file and key_file")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: s.ServerName}
	resolve := func(p string) string {
		if filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(baseDir, p)
	}
	if s.CAFile != "" {
		data, err := os.ReadFile(resolve(s.CAFile))
		if err != nil {
			return nil, fmt.Errorf("configured TLS CA cannot be read")
		}
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("configured TLS CA is invalid")
		}
		config.RootCAs = pool
	}
	if s.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(resolve(s.CertFile), resolve(s.KeyFile))
		if err != nil {
			return nil, fmt.Errorf("configured TLS client certificate is unavailable")
		}
		config.Certificates = []tls.Certificate{cert}
	}
	t.TLSClientConfig = config
	return t, nil
}
