package server

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cache-custom/internal/command"
	"cache-custom/internal/protocol"
	"cache-custom/internal/tenant"
)

func startServerOpts(t *testing.T, reg *command.Registry, opts Options) (*Server, string) {
	t.Helper()
	s := NewWithOptions("127.0.0.1:0", reg, opts)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if opts.TLSConfig != nil {
		ln = tls.NewListener(ln, opts.TLSConfig)
	}
	addr := ln.Addr().String()
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(func() { _ = s.Close() })

	// Wait until accept works (plain dial for TCP; for TLS just sleep briefly).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return s, addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	// TLS listener may reject plain dial; still return after wait.
	time.Sleep(50 * time.Millisecond)
	return s, addr
}

func authReg(t *testing.T) *command.Registry {
	t.Helper()
	r, err := tenant.NewRegistry([]tenant.Config{
		{Name: "App1", Password: "p1", MaxMemory: 1 << 20, ShardCount: 2, MaxClients: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	reg := command.NewRegistry()
	reg.SetPolicy(command.NewPolicy(true, nil, nil))
	command.RegisterDefaults(reg, r)
	command.RegisterAuth(reg, r)
	command.RegisterStringCommands(reg)
	return reg
}

func TestMaxClientsGlobal(t *testing.T) {
	reg := authReg(t)
	s, addr := startServerOpts(t, reg, Options{MaxClients: 1})

	c1 := dial(t, addr)
	// Occupy the only slot with a live connection.
	writeRaw(t, c1, "*1\r\n$4\r\nPING\r\n")
	_ = readValue(t, c1)

	c2, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	_ = c2.SetReadDeadline(time.Now().Add(2 * time.Second))
	v, err := protocol.Read(bufio.NewReader(c2))
	if err != nil {
		// Connection may be closed after error write.
		return
	}
	if v.Type != protocol.Error || v.Str != "ERR max number of clients reached" {
		t.Fatalf("expected max clients error, got %+v", v)
	}
	_ = c1.Close()
	// After close, new client should work.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.ClientCount() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	c3 := dial(t, addr)
	writeRaw(t, c3, "*1\r\n$4\r\nPING\r\n")
	v = readValue(t, c3)
	if v.Str != "PONG" {
		t.Fatalf("got %+v", v)
	}
}

func TestIdleTimeout(t *testing.T) {
	reg := authReg(t)
	_, addr := startServerOpts(t, reg, Options{IdleTimeout: 100 * time.Millisecond})
	c := dial(t, addr)
	writeRaw(t, c, "*1\r\n$4\r\nPING\r\n")
	_ = readValue(t, c)
	time.Sleep(250 * time.Millisecond)
	// Next write/read should fail (connection closed by server).
	_ = c.SetWriteDeadline(time.Now().Add(time.Second))
	_, err := io.WriteString(c, "*1\r\n$4\r\nPING\r\n")
	if err == nil {
		_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, err = protocol.Read(bufio.NewReader(c))
	}
	if err == nil {
		t.Fatal("expected connection to be closed after idle timeout")
	}
}

func genTestTLS(t *testing.T) (tls.Certificate, *x509.CertPool, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	tlsCert := tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)

	dir := t.TempDir()
	certPath := filepath.Join(dir, "server.pem")
	keyPath := filepath.Join(dir, "server.key")
	certOut, _ := os.Create(certPath)
	_ = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	_ = certOut.Close()
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyOut, _ := os.Create(keyPath)
	_ = pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	_ = keyOut.Close()
	return tlsCert, pool, dir
}

func TestTLSPing(t *testing.T) {
	tlsCert, pool, _ := genTestTLS(t)
	reg := authReg(t)
	cfg := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS12,
	}
	_, addr := startServerOpts(t, reg, Options{TLSConfig: cfg})

	conn, err := tls.DialWithDialer(
		&net.Dialer{Timeout: 2 * time.Second},
		"tcp",
		addr,
		&tls.Config{RootCAs: pool, ServerName: "localhost"},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	writeRaw(t, conn, "*1\r\n$4\r\nPING\r\n")
	v := readValue(t, conn)
	if v.Type != protocol.SimpleString || v.Str != "PONG" {
		t.Fatalf("TLS PING: %+v", v)
	}
	// AUTH + SET over TLS
	writeRaw(t, conn, "*3\r\n$4\r\nAUTH\r\n$4\r\nApp1\r\n$2\r\np1\r\n")
	v = readValue(t, conn)
	if v.Str != "OK" {
		t.Fatalf("AUTH: %+v", v)
	}
	writeRaw(t, conn, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	v = readValue(t, conn)
	if v.Str != "OK" {
		t.Fatalf("SET: %+v", v)
	}
}

func TestProtectedUnauthCannotWrite(t *testing.T) {
	reg := authReg(t)
	_, addr := startServerOpts(t, reg, Options{})
	c := dial(t, addr)
	writeRaw(t, c, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
	v := readValue(t, c)
	if v.Type != protocol.Error || v.Str != "NOAUTH Authentication required." {
		t.Fatalf("got %+v", v)
	}
}
