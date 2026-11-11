package diagnostics

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPTransportUsesExplicitCAButKeepsSystemDefaults(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ready")) }))
	defer server.Close()
	dir := t.TempDir()
	data := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ca.pem"), data, 0o600))
	transport, err := HTTPTransport(TLSConfig{CAFile: "ca.pem"}, dir)
	require.NoError(t, err)
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	response, err := client.Get(server.URL)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, 200, response.StatusCode)
	require.False(t, transport.TLSClientConfig.InsecureSkipVerify)
	_, err = HTTPTransport(TLSConfig{CertFile: "cert.pem"}, dir)
	require.Error(t, err)
}
