package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPostRequest(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST method, got %s", r.Method)
		}
		w.Header().Set("X-Test", "test-value")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("created"))
	}))
	defer server.Close()

	ctx := context.Background()
	body := []byte(`{"key": "value"}`)
	headers := map[string]string{
		"Content-Type": "application/json",
	}

	resp, err := PostRequest(
		ctx, server.URL, body, headers, "", 5*time.Second,
	)

	if err != nil {
		t.Fatalf("PostRequest failed: %v", err)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", resp.StatusCode)
	}

	if string(resp.Body) != "created" {
		t.Errorf("Expected 'created', got %s", string(resp.Body))
	}

	if resp.Headers.Get("X-Test") != "test-value" {
		t.Errorf("Expected test-value header, got %s", resp.Headers.Get("X-Test"))
	}

	if resp.ProxyIP != "" {
		t.Errorf("Expected empty ProxyIP, got %s", resp.ProxyIP)
	}
}

func TestGetRequest(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("Expected GET method, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("get response"))
	}))
	defer server.Close()

	ctx := context.Background()
	headers := map[string]string{
		"User-Agent": "test-agent",
	}

	resp, err := GetRequest(
		ctx, server.URL, headers, "", 5*time.Second,
	)

	if err != nil {
		t.Fatalf("GetRequest failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	if string(resp.Body) != "get response" {
		t.Errorf("Expected 'get response', got %s", string(resp.Body))
	}

	if resp.ProxyIP != "" {
		t.Errorf("Expected empty ProxyIP, got %s", resp.ProxyIP)
	}
}

func TestRequest(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("Expected PUT method, got %s", r.Method)
		}
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("put response"))
	}))
	defer server.Close()

	ctx := context.Background()
	body := []byte("put data")
	headers := map[string]string{
		"Content-Type": "text/plain",
	}

	resp, err := Request(
		ctx, "PUT", server.URL, body, headers, "", 5*time.Second,
	)

	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("Expected status 202, got %d", resp.StatusCode)
	}

	if string(resp.Body) != "put response" {
		t.Errorf("Expected 'put response', got %s", string(resp.Body))
	}

	if resp.ProxyIP != "" {
		t.Errorf("Expected empty ProxyIP, got %s", resp.ProxyIP)
	}
}

func TestRequestWithNilBody(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	ctx := context.Background()

	resp, err := Request(
		ctx, "GET", server.URL, nil, nil, "", 5*time.Second,
	)

	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	if string(resp.Body) != "ok" {
		t.Errorf("Expected 'ok', got %s", string(resp.Body))
	}

	if resp.ProxyIP != "" {
		t.Errorf("Expected empty ProxyIP, got %s", resp.ProxyIP)
	}
}
