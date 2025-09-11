package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/cookiejar"
	"time"
)

// HttpxResponse represents the response from an HTTP request
type HttpxResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
	ProxyIP    string // X-Proxy-IP header from CONNECT response, empty if no proxy used
}

// PostRequest performs a POST request and returns an HttpxResponse
func PostRequest(ctx context.Context, url string, body []byte, headers map[string]string, proxyURL string, timeout time.Duration) (*HttpxResponse, error) {
	client := NewClient(timeout, &tls.Config{
		MinVersion: tls.VersionTLS12,
	})

	opts := RequestOptions{
		Method:   "POST",
		URL:      url,
		Body:     bytes.NewReader(body),
		Headers:  headers,
		ProxyURL: proxyURL,
		Timeout:  timeout,
	}

	resp, err := client.Do(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &HttpxResponse{
		StatusCode: resp.StatusCode,
		Headers:    resp.Headers,
		Body:       resp.Body,
		ProxyIP:    resp.ProxyIP,
	}, nil
}

// PostRequestWithCookieJar performs a POST request with cookie jar support and returns an HttpxResponse
func PostRequestWithCookieJar(ctx context.Context, url string, body []byte, headers map[string]string, proxyURL string, timeout time.Duration, cookieJar *cookiejar.Jar) (*HttpxResponse, error) {
	client := NewClientWithCookieJar(timeout, &tls.Config{
		MinVersion: tls.VersionTLS12,
	}, cookieJar)

	opts := RequestOptions{
		Method:    "POST",
		URL:       url,
		Body:      bytes.NewReader(body),
		Headers:   headers,
		ProxyURL:  proxyURL,
		Timeout:   timeout,
		CookieJar: cookieJar,
	}

	resp, err := client.Do(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &HttpxResponse{
		StatusCode: resp.StatusCode,
		Headers:    resp.Headers,
		Body:       resp.Body,
		ProxyIP:    resp.ProxyIP,
	}, nil
}

// GetRequest performs a GET request and returns an HttpxResponse
func GetRequest(ctx context.Context, url string, headers map[string]string, proxyURL string, timeout time.Duration) (*HttpxResponse, error) {
	client := NewClient(timeout, &tls.Config{
		MinVersion: tls.VersionTLS12,
	})

	opts := RequestOptions{
		Method:   "GET",
		URL:      url,
		Headers:  headers,
		ProxyURL: proxyURL,
		Timeout:  timeout,
	}

	resp, err := client.Do(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &HttpxResponse{
		StatusCode: resp.StatusCode,
		Headers:    resp.Headers,
		Body:       resp.Body,
		ProxyIP:    resp.ProxyIP,
	}, nil
}

// GetRequestWithCookieJar performs a GET request with cookie jar support and returns an HttpxResponse
func GetRequestWithCookieJar(ctx context.Context, url string, headers map[string]string, proxyURL string, timeout time.Duration, cookieJar *cookiejar.Jar) (*HttpxResponse, error) {
	client := NewClientWithCookieJar(timeout, &tls.Config{
		MinVersion: tls.VersionTLS12,
	}, cookieJar)

	opts := RequestOptions{
		Method:    "GET",
		URL:       url,
		Headers:   headers,
		ProxyURL:  proxyURL,
		Timeout:   timeout,
		CookieJar: cookieJar,
	}

	resp, err := client.Do(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &HttpxResponse{
		StatusCode: resp.StatusCode,
		Headers:    resp.Headers,
		Body:       resp.Body,
		ProxyIP:    resp.ProxyIP,
	}, nil
}

// Request performs an HTTP request and returns an HttpxResponse
func Request(ctx context.Context, method, url string, body []byte, headers map[string]string, proxyURL string, timeout time.Duration) (*HttpxResponse, error) {
	client := NewClient(timeout, &tls.Config{
		MinVersion: tls.VersionTLS12,
	})

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	opts := RequestOptions{
		Method:   method,
		URL:      url,
		Body:     bodyReader,
		Headers:  headers,
		ProxyURL: proxyURL,
		Timeout:  timeout,
	}

	resp, err := client.Do(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &HttpxResponse{
		StatusCode: resp.StatusCode,
		Headers:    resp.Headers,
		Body:       resp.Body,
		ProxyIP:    resp.ProxyIP,
	}, nil
}

// RequestWithCookieJar performs an HTTP request with cookie jar support and returns an HttpxResponse
func RequestWithCookieJar(ctx context.Context, method, url string, body []byte, headers map[string]string, proxyURL string, timeout time.Duration, cookieJar *cookiejar.Jar) (*HttpxResponse, error) {
	client := NewClientWithCookieJar(timeout, &tls.Config{
		MinVersion: tls.VersionTLS12,
	}, cookieJar)

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	opts := RequestOptions{
		Method:    method,
		URL:       url,
		Body:      bodyReader,
		Headers:   headers,
		ProxyURL:  proxyURL,
		Timeout:   timeout,
		CookieJar: cookieJar,
	}

	resp, err := client.Do(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &HttpxResponse{
		StatusCode: resp.StatusCode,
		Headers:    resp.Headers,
		Body:       resp.Body,
		ProxyIP:    resp.ProxyIP,
	}, nil
}
