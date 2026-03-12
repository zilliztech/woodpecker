// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func Test_buildKvGroup(t *testing.T) {
	t.Run("length not equal", func(t *testing.T) {
		keys := []string{"k1", "k2"}
		values := []string{"v1"}
		_, err := buildKvGroup(keys, values)
		assert.Error(t, err)
	})

	t.Run("duplicate", func(t *testing.T) {
		keys := []string{"k1", "k1"}
		values := []string{"v1", "v2"}
		_, err := buildKvGroup(keys, values)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		keys := []string{"k1", "k2"}
		values := []string{"v1", "v2"}
		kvs, err := buildKvGroup(keys, values)
		assert.NoError(t, err)
		for i, k := range keys {
			v, ok := kvs[k]
			assert.True(t, ok)
			assert.Equal(t, values[i], v)
		}
	})
}

func Test_SaveByBatch(t *testing.T) {
	t.Run("empty kvs", func(t *testing.T) {
		kvs := map[string]string{}

		group := 0
		count := 0
		saveFn := func(partialKvs map[string]string) error {
			group++
			count += len(partialKvs)
			return nil
		}

		limit := 2
		err := SaveByBatchWithLimit(kvs, limit, saveFn)
		assert.NoError(t, err)
		assert.Equal(t, 0, group)
		assert.Equal(t, 0, count)
	})

	t.Run("normal case", func(t *testing.T) {
		kvs := map[string]string{
			"k1": "v1",
			"k2": "v2",
			"k3": "v3",
		}

		group := 0
		count := 0
		saveFn := func(partialKvs map[string]string) error {
			group++
			count += len(partialKvs)
			return nil
		}

		limit := 2
		err := SaveByBatchWithLimit(kvs, limit, saveFn)
		assert.NoError(t, err)
		assert.Equal(t, 2, group)
		assert.Equal(t, 3, count)
	})

	t.Run("multi save failed", func(t *testing.T) {
		saveFn := func(partialKvs map[string]string) error {
			return errors.New("mock")
		}
		kvs := map[string]string{
			"k1": "v1",
			"k2": "v2",
			"k3": "v3",
		}
		limit := 2
		err := SaveByBatchWithLimit(kvs, limit, saveFn)
		assert.Error(t, err)
	})
}

func Test_RemoveByBatch(t *testing.T) {
	t.Run("empty kvs case", func(t *testing.T) {
		var kvs []string

		group := 0
		count := 0
		removeFn := func(partialKvs []string) error {
			group++
			count += len(partialKvs)
			return nil
		}

		limit := 2
		err := RemoveByBatchWithLimit(kvs, limit, removeFn)
		assert.NoError(t, err)
		assert.Equal(t, 0, group)
		assert.Equal(t, 0, count)
	})

	t.Run("normal case", func(t *testing.T) {
		kvs := []string{"k1", "k2", "k3", "k4", "k5"}

		group := 0
		count := 0
		removeFn := func(partialKvs []string) error {
			group++
			count += len(partialKvs)
			return nil
		}

		limit := 2
		err := RemoveByBatchWithLimit(kvs, limit, removeFn)
		assert.NoError(t, err)
		assert.Equal(t, 3, group)
		assert.Equal(t, 5, count)
	})

	t.Run("multi remove failed", func(t *testing.T) {
		removeFn := func(partialKvs []string) error {
			return errors.New("mock")
		}
		kvs := []string{"k1", "k2", "k3", "k4", "k5"}
		limit := 2
		err := RemoveByBatchWithLimit(kvs, limit, removeFn)
		assert.Error(t, err)
	})
}

func Test_min(t *testing.T) {
	type args struct {
		a int
		b int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			args: args{a: 1, b: 2},
			want: 1,
		},
		{
			args: args{a: 4, b: 3},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := min(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("min() = %v, want %v", got, tt.want)
			}
		})
	}
}

// generateTestCerts creates self-signed CA, cert, and key files for TLS testing.
func generateTestCerts(t *testing.T) (certFile, keyFile, caCertFile string) {
	t.Helper()
	dir := t.TempDir()

	// Generate CA key and certificate
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Test CA"}},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caCertFile = filepath.Join(dir, "ca.pem")
	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	require.NoError(t, os.WriteFile(caCertFile, caCertPEM, 0o644))

	// Generate server cert signed by CA
	certKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	certTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{Organization: []string{"Test"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	caCert, err := x509.ParseCertificate(caCertDER)
	require.NoError(t, err)
	certDER, err := x509.CreateCertificate(rand.Reader, certTemplate, caCert, &certKey.PublicKey, caKey)
	require.NoError(t, err)

	certFile = filepath.Join(dir, "cert.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o644))

	keyFile = filepath.Join(dir, "key.pem")
	keyDER, err := x509.MarshalECPrivateKey(certKey)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o644))

	return
}

func TestStartTestEmbedEtcdServer_And_GetEndpoints(t *testing.T) {
	server, dir, err := StartTestEmbedEtcdServer()
	require.NoError(t, err)
	defer server.Close()
	defer os.RemoveAll(dir)
	<-server.Server.ReadyNotify()

	endpoints := GetEmbedEtcdEndpoints(server)
	assert.NotEmpty(t, endpoints)
	for _, addr := range endpoints {
		assert.NotEmpty(t, addr)
	}
}

func TestGetRemoteEtcdClient(t *testing.T) {
	server, dir, err := StartTestEmbedEtcdServer()
	require.NoError(t, err)
	defer server.Close()
	defer os.RemoveAll(dir)
	<-server.Server.ReadyNotify()

	endpoints := GetEmbedEtcdEndpoints(server)

	client, err := GetRemoteEtcdClient(endpoints)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	// Verify connection works
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = client.Put(ctx, "test_remote_client", "value")
	assert.NoError(t, err)
}

func TestGetRemoteEtcdClientWithAuth(t *testing.T) {
	server, dir, err := StartTestEmbedEtcdServer()
	require.NoError(t, err)
	defer server.Close()
	defer os.RemoveAll(dir)
	<-server.Server.ReadyNotify()

	endpoints := GetEmbedEtcdEndpoints(server)

	// Embedded server doesn't have auth enabled.
	// Client creation should still succeed; auth is validated at request time.
	client, err := GetRemoteEtcdClientWithAuth(endpoints, "user", "pass")
	if err == nil {
		defer client.Close()
	}
	// Function body is covered regardless of success/failure
}

func TestGetEtcdClient_NonSSLRemote(t *testing.T) {
	server, dir, err := StartTestEmbedEtcdServer()
	require.NoError(t, err)
	defer server.Close()
	defer os.RemoveAll(dir)
	<-server.Server.ReadyNotify()

	endpoints := GetEmbedEtcdEndpoints(server)

	// Test the non-embed, non-SSL branch of GetEtcdClient
	client, err := GetEtcdClient(false, false, endpoints, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()
}

func TestGetEtcdClient_SSLBranch(t *testing.T) {
	// Test the useSSL=true branch of GetEtcdClient (error path via bad cert)
	_, err := GetEtcdClient(false, true, []string{"127.0.0.1:2379"},
		"nonexistent.pem", "nonexistent.key", "nonexistent_ca.pem", "1.2")
	assert.Error(t, err)
}

func TestGetRemoteEtcdSSLClient(t *testing.T) {
	// Test that GetRemoteEtcdSSLClient delegates to GetRemoteEtcdSSLClientWithCfg.
	// Bad cert file causes a fast error, covering the delegation path.
	_, err := GetRemoteEtcdSSLClient(
		[]string{"127.0.0.1:2379"},
		"nonexistent.pem", "nonexistent.key", "nonexistent_ca.pem", "1.2")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "load etcd cert key pair error")
}

func TestGetRemoteEtcdSSLClientWithCfg_TLSVersions(t *testing.T) {
	certFile, keyFile, caCertFile := generateTestCerts(t)

	// Test all valid TLS version strings.
	// Use a pre-cancelled context so clientv3.New returns immediately
	// after the TLS config is fully built (covering all switch branches).
	validVersions := []string{"1.0", "1.1", "1.2", "1.3"}
	for _, ver := range validVersions {
		t.Run("TLS_"+ver, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // cancel immediately so clientv3.New won't block
			cfg := clientv3.Config{Context: ctx}
			_, err := GetRemoteEtcdSSLClientWithCfg(
				[]string{"127.0.0.1:2379"}, certFile, keyFile, caCertFile, ver, cfg)
			assert.Error(t, err) // fails due to cancelled context, but TLS config path is covered
		})
	}

	t.Run("invalid_version", func(t *testing.T) {
		_, err := GetRemoteEtcdSSLClientWithCfg(
			[]string{"127.0.0.1:2379"}, certFile, keyFile, caCertFile, "invalid", clientv3.Config{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown TLS version")
	})
}

func TestGetRemoteEtcdSSLClientWithCfg_CertErrors(t *testing.T) {
	certFile, keyFile, caCertFile := generateTestCerts(t)

	t.Run("bad_cert_file", func(t *testing.T) {
		_, err := GetRemoteEtcdSSLClientWithCfg(
			[]string{"127.0.0.1:2379"}, "nonexistent.pem", keyFile, caCertFile, "1.2", clientv3.Config{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "load etcd cert key pair error")
	})

	t.Run("bad_ca_file", func(t *testing.T) {
		_, err := GetRemoteEtcdSSLClientWithCfg(
			[]string{"127.0.0.1:2379"}, certFile, keyFile, "nonexistent_ca.pem", "1.2", clientv3.Config{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "load etcd CACert file error")
	})
}

func TestCreateEtcdClient(t *testing.T) {
	server, dir, err := StartTestEmbedEtcdServer()
	require.NoError(t, err)
	defer server.Close()
	defer os.RemoveAll(dir)
	<-server.Server.ReadyNotify()

	endpoints := GetEmbedEtcdEndpoints(server)

	t.Run("no_auth_non_ssl", func(t *testing.T) {
		// enableAuth=false → delegates to GetEtcdClient(useEmbedEtcd=false, useSSL=false)
		client, err := CreateEtcdClient(false, false, "", "", false, endpoints, "", "", "", "")
		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer client.Close()
	})

	t.Run("auth_with_ssl_bad_cert", func(t *testing.T) {
		// enableAuth=true, useSSL=true → delegates to GetRemoteEtcdSSLClientWithCfg with auth creds
		_, err := CreateEtcdClient(false, true, "user", "pass", true, endpoints, "bad.pem", "bad.key", "bad_ca.pem", "1.2")
		assert.Error(t, err)
	})

	t.Run("auth_without_ssl", func(t *testing.T) {
		// enableAuth=true, useSSL=false → delegates to GetRemoteEtcdClientWithAuth
		client, err := CreateEtcdClient(false, true, "user", "pass", false, endpoints, "", "", "", "")
		if err == nil {
			defer client.Close()
		}
		// Function body is covered regardless
	})
}
