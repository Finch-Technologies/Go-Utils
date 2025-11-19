package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/finch-technologies/go-utils/log"
	"github.com/finch-technologies/go-utils/utils"
	"github.com/google/go-querystring/query"
)

type FetchOptions struct {
	Headers        *http.Header
	Cookies        *[]http.Cookie
	Proxy          *Proxy
	ShowCurl       bool
	RawBody        bool
	CookieJar      *cookiejar.Jar
	ReturnResponse bool
	Timeout        time.Duration
}

type Proxy struct {
	Host         string
	Port         string
	Username     string
	Password     string
	AssignedIp   string
	AssignedPort string
}

func getOpts(options []FetchOptions) FetchOptions {
	var opts FetchOptions

	if len(options) > 0 {
		opts = options[0]
	}

	return opts
}

func Fetch[T interface{}](ctx context.Context, url, method string, payload interface{}, options ...FetchOptions) (T, error) {
	var jsonResp T

	resp, err := FetchRaw(ctx, url, method, payload, options...)

	if err != nil {
		return jsonResp, err
	}

	return JsonBody[T](ctx, resp)
}

func BodyBytes(ctx context.Context, response *http.Response) ([]byte, error) {

	bodyBytes, err := io.ReadAll(response.Body)

	//contentType := http.DetectContentType(bodyBytes)
	//log.Debugf("Content Type: %s", contentType)

	if err != nil {
		return nil, fmt.Errorf("failed to read body from response. Error: %s, Body: %s", err, string(bodyBytes))
	}

	return bodyBytes, nil
}

func TextBody(ctx context.Context, response *http.Response) (string, error) {

	bodyBytes, err := io.ReadAll(response.Body)

	//contentType := http.DetectContentType(bodyBytes)
	//log.Debugf("Content Type: %s", contentType)

	if err != nil {
		return "", fmt.Errorf("failed to read body from response. Error: %s, Body: %s", err, string(bodyBytes))
	}

	return string(bodyBytes), nil
}

func JsonBody[T interface{}](ctx context.Context, response *http.Response) (T, error) {
	var jsonResp T

	bodyBytes, err := io.ReadAll(response.Body)

	if err != nil {
		return jsonResp, fmt.Errorf("failed to read body from response. Error: %s, Body: %s", err, string(bodyBytes))
	}

	err = json.Unmarshal(bodyBytes, &jsonResp)

	//log.Debugf("Response body: %s", string(bodyBytes))

	if err != nil {
		return jsonResp, fmt.Errorf("failed to unmarshal response body into json: %s", err)
	}

	return jsonResp, nil
}

func FetchRaw(ctx context.Context, uri, method string, payload interface{}, options ...FetchOptions) (*http.Response, error) {
	method = strings.ToUpper(method)
	opts := getOpts(options)

	// Build proxy URL if proxy options exist
	proxyURL := getProxyUrl(opts.Proxy)

	// Convert headers to map[string]string
	var headers map[string]string
	if opts.Headers != nil {
		headers = make(map[string]string)
		for key, values := range *opts.Headers {
			if len(values) > 0 {
				headers[key] = values[0] // Use the first value if multiple exist
			}
		}
	}

	// Handle cookies by converting them to header
	if opts.Cookies != nil {
		if headers == nil {
			headers = make(map[string]string)
		}
		var cookieStrings []string
		for _, cookie := range *opts.Cookies {
			cookieStrings = append(cookieStrings, fmt.Sprintf("%s=%s", cookie.Name, cookie.Value))
		}
		if len(cookieStrings) > 0 {
			headers["Cookie"] = strings.Join(cookieStrings, "; ")
		}
	}

	var body []byte
	if payload != nil {
		if method == "GET" {
			var qs string
			//if payload is a map of strings, convert it to a query string
			if reflect.TypeOf(payload).Kind() == reflect.Map && reflect.TypeOf(payload).Elem().Kind() == reflect.String {
				payloadMap := payload.(map[string]string)
				values := url.Values{}
				for key, value := range payloadMap {
					values.Add(key, fmt.Sprintf("%v", value))
				}
				qs = values.Encode()
			} else {
				v, err := query.Values(payload)
				if err != nil {
					return nil, fmt.Errorf("failed to create query string: %s", err)
				}
				qs = v.Encode()
			}
			if qs != "" {
				uri += fmt.Sprint("?", qs)
			}
		} else if opts.RawBody {
			if reflect.TypeOf(payload).Kind() == reflect.String {
				body = []byte(payload.(string))
			}
		} else {
			jsonBytes, err := json.Marshal(payload)
			if err != nil {
				return nil, fmt.Errorf("failed to create request body: %s", err)
			}
			body = jsonBytes
		}
	}

	var resp *HttpxResponse
	var err error
	timeout := utils.DurationOrDefault(opts.Timeout, 30*time.Second)

	// Use our custom Request function with CookieJar support
	if opts.CookieJar != nil {
		resp, err = RequestWithCookieJar(ctx, method, uri, body, headers, proxyURL, timeout, opts.CookieJar)
	} else {
		resp, err = Request(ctx, method, uri, body, headers, proxyURL, timeout)
	}

	if err != nil {
		return nil, fmt.Errorf("http request failed with error: %s", err)
	}

	// Create a mock HTTP response for backward compatibility
	httpResp := &http.Response{
		Status:        fmt.Sprintf("%d %s", resp.StatusCode, http.StatusText(resp.StatusCode)),
		StatusCode:    resp.StatusCode,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        resp.Headers,
		Body:          io.NopCloser(bytes.NewReader(resp.Body)),
		ContentLength: int64(len(resp.Body)),
	}

	if opts.ReturnResponse && resp.StatusCode >= 300 {
		return httpResp, fmt.Errorf("http request was unsuccessful with status code: %d. request url: %s", resp.StatusCode, uri)
	} else if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http request was unsuccessful with status code: %d. request url: %s", resp.StatusCode, uri)
	}

	if opts.ShowCurl {
		log.Debugf("Request: %s %s", method, uri)
		if headers != nil {
			for key, value := range headers {
				log.Debugf("Header: %s: %s", key, value)
			}
		}
	}

	return httpResp, nil
}

func FetchData(ctx context.Context, apiURL, method, stage string, headers *http.Header, responseType string) (string, error) {
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
			DisableKeepAlives:   false,
		},
	}

	// Create a new HTTP request
	req, err := http.NewRequest(method, apiURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set headers
	//for key, value := range headers {
	//	req.Header.Set(key, value)
	//}
	req.Header = *headers

	// Execute the request
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to execute HTTP request: %w", err)
	}
	defer resp.Body.Close()

	// Check the response status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("unsuccessful HTTP request: status code %d", resp.StatusCode)
	}

	// Read the response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	// Optionally handle response type (e.g., JSON, plain text)
	if responseType == "json" {
		var jsonBuffer bytes.Buffer
		if err := json.Indent(&jsonBuffer, bodyBytes, "", "  "); err != nil {
			return "", fmt.Errorf("failed to parse JSON response: %w", err)
		}
		return jsonBuffer.String(), nil
	}

	return string(bodyBytes), nil
}

// GetPublicIP returns the service's public-facing IP address by querying external IP detection services
// This is useful when the service is behind NAT and you need the internet-visible IP address
func GetPublicIP(proxy *Proxy) (string, error) {

	// List of IP detection services to try (in order)
	services := []string{
		"https://api.ipify.org",
		"https://icanhazip.com",
		"https://ifconfig.me/ip",
	}

	// Create HTTP client with timeout and proxy
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	proxyURL, err := url.Parse(getProxyUrl(proxy))

	if proxy != nil && err == nil {
		client.Transport = &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		}
	}

	var lastErr error
	for _, service := range services {
		ip, err := queryIPService(client, service)
		if err == nil && ip != "" {
			return ip, nil
		}
		lastErr = err
		log.Warningf("Failed to get public IP from %s: %v", service, err)
	}

	return "", lastErr
}

// queryIPService queries a single IP detection service
func queryIPService(client *http.Client, url string) (string, error) {
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// Trim whitespace and newlines
	ip := strings.TrimSpace(string(body))
	return ip, nil
}

func getProxyUrl(proxy *Proxy) string {
	if proxy == nil {
		return ""
	}
	proxyHost := regexp.MustCompile(`^(http|https)://`).ReplaceAllString(proxy.Host, "")
	if proxy.Username != "" && proxy.Password != "" {
		return fmt.Sprintf("http://%s:%s@%s:%s", proxy.Username, proxy.Password, proxyHost, proxy.Port)
	}
	return fmt.Sprintf("http://%s:%s", proxyHost, proxy.Port)
}
