package http

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"
)

// Response represents the response from an HTTP request with optional proxy information
type Response struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
	ProxyIP    string // X-Proxy-IP header from CONNECT response, empty if no proxy used
}

// RequestOptions contains options for making HTTP requests
type RequestOptions struct {
	Method    string
	URL       string
	Body      io.Reader
	Headers   map[string]string
	ProxyURL  string
	Timeout   time.Duration
	TLSConfig *tls.Config
	CookieJar *cookiejar.Jar
}

// Client is a custom HTTP client that can extract proxy information
type Client struct {
	timeout   time.Duration
	tlsConfig *tls.Config
	cookieJar *cookiejar.Jar
}

// NewClient creates a new custom HTTP client
func NewClient(timeout time.Duration, tlsConfig *tls.Config) *Client {
	if tlsConfig == nil {
		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	return &Client{
		timeout:   timeout,
		tlsConfig: tlsConfig,
	}
}

// NewClientWithCookieJar creates a new custom HTTP client with cookie jar
func NewClientWithCookieJar(timeout time.Duration, tlsConfig *tls.Config, cookieJar *cookiejar.Jar) *Client {
	if tlsConfig == nil {
		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	return &Client{
		timeout:   timeout,
		tlsConfig: tlsConfig,
		cookieJar: cookieJar,
	}
}

// Do performs an HTTP request and returns the response with optional proxy IP
func (c *Client) Do(ctx context.Context, opts RequestOptions) (*Response, error) {
	if opts.ProxyURL == "" {
		// No proxy - use standard HTTP client
		return c.doDirectRequest(ctx, opts)
	}

	// Use proxy with custom CONNECT handling
	return c.doProxyRequest(ctx, opts)
}

// doDirectRequest performs a request without proxy
func (c *Client) doDirectRequest(ctx context.Context, opts RequestOptions) (*Response, error) {
	// Use cookie jar from opts first, then from client
	var cookieJar *cookiejar.Jar
	if opts.CookieJar != nil {
		cookieJar = opts.CookieJar
	} else {
		cookieJar = c.cookieJar
	}

	client := &http.Client{
		Timeout: c.timeout,
		Transport: &http.Transport{
			TLSClientConfig: c.tlsConfig,
		},
	}

	// Only set the cookie jar if it's not nil
	if cookieJar != nil {
		client.Jar = cookieJar
	}

	var bodyReader io.Reader
	if opts.Body != nil {
		bodyReader = opts.Body
	}

	req, err := http.NewRequestWithContext(ctx, opts.Method, opts.URL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	for key, value := range opts.Headers {
		req.Header.Set(key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return &Response{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header,
		Body:       body,
		ProxyIP:    "", // No proxy used
	}, nil
}

// doProxyRequest performs a request through a proxy with custom CONNECT handling
func (c *Client) doProxyRequest(ctx context.Context, opts RequestOptions) (*Response, error) {
	proxyURL, err := url.Parse(opts.ProxyURL)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy URL: %w", err)
	}

	targetURL, err := url.Parse(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid target URL: %w", err)
	}

	// For HTTP requests through proxy, we can use regular proxy
	if targetURL.Scheme == "http" {
		return c.doHTTPProxy(ctx, opts, proxyURL)
	}

	// For HTTPS requests, we need to handle CONNECT manually
	return c.doHTTPSProxy(ctx, opts, proxyURL, targetURL)
}

// doHTTPProxy handles HTTP requests through proxy (no CONNECT needed)
func (c *Client) doHTTPProxy(ctx context.Context, opts RequestOptions, proxyURL *url.URL) (*Response, error) {
	// Use cookie jar from opts first, then from client
	var cookieJar *cookiejar.Jar
	if opts.CookieJar != nil {
		cookieJar = opts.CookieJar
	} else {
		cookieJar = c.cookieJar
	}

	transport := &http.Transport{
		Proxy:           http.ProxyURL(proxyURL),
		TLSClientConfig: c.tlsConfig,
	}

	client := &http.Client{
		Timeout:   c.timeout,
		Transport: transport,
	}

	// Only set the cookie jar if it's not nil
	if cookieJar != nil {
		client.Jar = cookieJar
	}

	var bodyReader io.Reader
	if opts.Body != nil {
		bodyReader = opts.Body
	}

	req, err := http.NewRequestWithContext(ctx, opts.Method, opts.URL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	for key, value := range opts.Headers {
		req.Header.Set(key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Note: For HTTP proxy, we don't get CONNECT response headers
	// The proxy IP would need to be extracted differently if needed
	return &Response{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header,
		Body:       body,
		ProxyIP:    "", // HTTP proxy doesn't expose CONNECT headers
	}, nil
}

// doHTTPSProxy handles HTTPS requests through proxy with manual CONNECT
func (c *Client) doHTTPSProxy(ctx context.Context, opts RequestOptions, proxyURL *url.URL, targetURL *url.URL) (*Response, error) {
	// Connect to proxy
	proxyAddr := proxyURL.Host
	if !strings.Contains(proxyAddr, ":") {
		proxyAddr += ":8080" // Default proxy port
	}

	dialer := &net.Dialer{
		Timeout: c.timeout,
	}

	conn, err := dialer.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to proxy: %w", err)
	}
	defer conn.Close()

	// Send CONNECT request
	targetAddr := targetURL.Host
	if !strings.Contains(targetAddr, ":") {
		targetAddr += ":443" // Default HTTPS port
	}

	connectReq := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n", targetAddr, targetAddr)

	// Add proxy authentication if present in URL
	if proxyURL.User != nil {
		if password, ok := proxyURL.User.Password(); ok {
			auth := proxyURL.User.Username() + ":" + password
			connectReq += fmt.Sprintf("Proxy-Authorization: Basic %s\r\n", basicAuth(auth))
		}
	}

	connectReq += "\r\n"

	// Send CONNECT request
	_, err = conn.Write([]byte(connectReq))
	if err != nil {
		return nil, fmt.Errorf("failed to send CONNECT request: %w", err)
	}

	// Read CONNECT response
	reader := bufio.NewReader(conn)
	connectResp, err := http.ReadResponse(reader, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read CONNECT response: %w", err)
	}
	defer connectResp.Body.Close()

	if connectResp.StatusCode != 200 {
		return nil, fmt.Errorf("CONNECT failed with status: %d %s", connectResp.StatusCode, connectResp.Status)
	}

	// Extract X-Proxy-IP header from CONNECT response
	proxyIP := connectResp.Header.Get("X-Proxy-IP")

	// Establish TLS connection over the tunnel
	// Create a copy of the TLS config with the correct ServerName
	tlsConfig := c.tlsConfig.Clone()
	tlsConfig.ServerName = targetURL.Hostname()

	tlsConn := tls.Client(conn, tlsConfig)
	err = tlsConn.HandshakeContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("TLS handshake failed: %w", err)
	}

	// Create HTTP request
	var bodyReader io.Reader
	if opts.Body != nil {
		bodyReader = opts.Body
	}

	req, err := http.NewRequestWithContext(ctx, opts.Method, opts.URL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	for key, value := range opts.Headers {
		req.Header.Set(key, value)
	}

	// Send HTTP request over TLS connection
	err = req.Write(tlsConn)
	if err != nil {
		return nil, fmt.Errorf("failed to send HTTP request: %w", err)
	}

	// Read HTTP response
	httpReader := bufio.NewReader(tlsConn)
	resp, err := http.ReadResponse(httpReader, req)
	if err != nil {
		return nil, fmt.Errorf("failed to read HTTP response: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return &Response{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header,
		Body:       body,
		ProxyIP:    proxyIP,
	}, nil
}

// basicAuth encodes username:password for basic authentication
func basicAuth(auth string) string {
	return base64.StdEncoding.EncodeToString([]byte(auth))
}
