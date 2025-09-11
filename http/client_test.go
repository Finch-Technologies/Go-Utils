package http

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClient_DoDirectRequest(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST method, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Expected application/json content type, got %s", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	}))
	defer server.Close()

	client := NewClient(5*time.Second, nil)
	ctx := context.Background()

	opts := RequestOptions{
		Method: "POST",
		URL:    server.URL,
		Body:   strings.NewReader(`{"test": "data"}`),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}

	resp, err := client.Do(ctx, opts)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != 200 {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	if string(resp.Body) != "success" {
		t.Errorf("Expected 'success', got %s", string(resp.Body))
	}

	if resp.ProxyIP != "" {
		t.Errorf("Expected empty ProxyIP for direct request, got %s", resp.ProxyIP)
	}
}

func TestClient_DoDirectRequestTimeout(t *testing.T) {
	// Create slow test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	}))
	defer server.Close()

	client := NewClient(50*time.Millisecond, nil)
	ctx := context.Background()

	opts := RequestOptions{
		Method: "GET",
		URL:    server.URL,
	}

	_, err := client.Do(ctx, opts)
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}
}

func TestClient_NewClientDefaults(t *testing.T) {
	client := NewClient(5*time.Second, nil)

	if client.timeout != 5*time.Second {
		t.Errorf("Expected timeout 5s, got %v", client.timeout)
	}

	if client.tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("Expected TLS 1.2 minimum, got %x", client.tlsConfig.MinVersion)
	}
}

func TestClient_CustomTLSConfig(t *testing.T) {
	customTLS := &tls.Config{MinVersion: tls.VersionTLS13}
	client := NewClient(5*time.Second, customTLS)

	if client.tlsConfig.MinVersion != tls.VersionTLS13 {
		t.Errorf("Expected TLS 1.3 minimum, got %x", client.tlsConfig.MinVersion)
	}
}

func TestBasicAuth(t *testing.T) {
	result := basicAuth("user:pass")
	expected := "dXNlcjpwYXNz" // base64 encoding of "user:pass"

	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}
