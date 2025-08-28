package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/aoliveti/curling"
	"github.com/finch-technologies/go-utils/log"
	"github.com/google/go-querystring/query"
)

type FetchOptions struct {
	Headers   *http.Header
	Cookies   *[]http.Cookie
	Proxy     *Proxy
	ShowCurl  bool
	RawBody   bool
	CookieJar *cookiejar.Jar
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
	client := &http.Client{}

	opts := getOpts(options)

	if opts.CookieJar != nil && opts.Headers.Get("Cookie") == "" && opts.Cookies == nil {
		client.Jar = opts.CookieJar
	}

	var body io.Reader = nil

	var err error
	if opts.Proxy != nil {
		err = addProxy(client, opts.Proxy)
		if err != nil {
			return nil, fmt.Errorf("failed to create client with proxy: %s", err)
		}
	}
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
				body = strings.NewReader(payload.(string))
			}
		} else {
			jsonBytes, err := json.Marshal(payload)
			if err != nil {
				return nil, fmt.Errorf("failed to create request body: %s", err)
			}
			//log.Debug("Request body: %s", string(jsonBytes))
			body = bytes.NewReader(jsonBytes)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, uri, body)

	if err != nil {
		return nil, fmt.Errorf("failed to create new http request: %s", err)
	}

	if opts.Headers != nil {
		for key, values := range *opts.Headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
	}

	if opts.Cookies != nil {
		for _, cookie := range *opts.Cookies {
			req.AddCookie(&cookie)
		}
	}

	if opts.ShowCurl {
		curlCmd, _ := curling.NewFromRequest(req)
		log.Debugf("Request: %s", curlCmd.String())
	}

	resp, err := client.Do(req)

	if err != nil {
		return resp, fmt.Errorf("http request failed with error: %s", err)
	}

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http request was unsuccessful with status code: %d. request url: %s", resp.StatusCode, uri)
	}

	return resp, nil
}

func addProxy(client *http.Client, proxy *Proxy) error {
	//remove the http:// or https:// from the host using regex
	proxyHost := regexp.MustCompile(`^(http|https)://`).ReplaceAllString(proxy.Host, "")

	proxyUrl, err := url.Parse("http://" + proxy.Username + ":" + proxy.Password + "@" + proxyHost + ":" + proxy.Port)
	if err != nil {
		return err
	}
	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyUrl),
	}
	client.Transport = transport
	client.Timeout = 30 * time.Second
	return nil
}

func FetchData(ctx context.Context, apiURL, method, stage string, headers *http.Header, responseType string) (string, error) {
	client := &http.Client{}

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
	bodyBytes, err := ioutil.ReadAll(resp.Body)
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
