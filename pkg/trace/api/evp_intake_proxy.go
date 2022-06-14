// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package api

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	stdlog "log"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/DataDog/datadog-agent/pkg/trace/api/apiutil"
	"github.com/DataDog/datadog-agent/pkg/trace/config"
	"github.com/DataDog/datadog-agent/pkg/trace/info"
	"github.com/DataDog/datadog-agent/pkg/trace/log"
	"github.com/DataDog/datadog-agent/pkg/trace/metrics"
)

const (
	validSubdomainSymbols       = "_-."
	validPathSymbols            = "/_-+"
	validPathQueryStringSymbols = "/_-+@?&=.:\""
)

func isValidSubdomain(s string) bool {
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && !strings.ContainsRune(validSubdomainSymbols, c) {
			return false
		}
	}
	return true
}

func isValidPath(s string) bool {
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && !strings.ContainsRune(validPathSymbols, c) {
			return false
		}
	}
	return true
}

func isValidQueryString(s string) bool {
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && !strings.ContainsRune(validPathQueryStringSymbols, c) {
			return false
		}
	}
	return true
}

// evpProxyEndpointsFromConfig returns the configured list of endpoints to forward payloads to.
func evpProxyEndpointsFromConfig(conf *config.AgentConfig) []config.Endpoint {
	apiKey := conf.EVPProxy.APIKey
	if apiKey == "" {
		apiKey = conf.APIKey()
	}
	endpoint := conf.EVPProxy.DDURL
	if endpoint == "" {
		endpoint = conf.Site
	}
	mainEndpoint := config.Endpoint{Host: endpoint, APIKey: apiKey}
	endpoints := []config.Endpoint{mainEndpoint}
	for host, keys := range conf.EVPProxy.AdditionalEndpoints {
		for _, key := range keys {
			endpoints = append(endpoints, config.Endpoint{
				Host:   host,
				APIKey: key,
			})
		}
	}
	return endpoints
}

// evpProxyHandler returns an HTTP handler for the /evp_proxy API.
// Depending on the config, this is a proxying handler or a noop handler.
func (r *HTTPReceiver) evpProxyHandler() http.Handler {
	// r.conf is populated by cmd/trace-agent/config/config.go
	if !r.conf.EVPProxy.Enabled {
		return evpProxyErrorHandler("Has been disabled in config")
	}
	endpoints := evpProxyEndpointsFromConfig(r.conf)
	transport := r.conf.NewHTTPTransport()
	logger := stdlog.New(log.NewThrottled(5, 10*time.Second), "EVPProxy: ", 0) // limit to 5 messages every 10 seconds
	handler := evpProxyForwarder(r.conf, endpoints, transport, logger)
	return http.StripPrefix("/evp_proxy/v1", handler)
}

// evpProxyErrorHandler returns an HTTP handler that will always return
// http.StatusMethodNotAllowed along with a clarification.
func evpProxyErrorHandler(message string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		msg := fmt.Sprintf("EVPProxy is disabled: %v", message)
		http.Error(w, msg, http.StatusMethodNotAllowed)
	})
}

// evpProxyForwarder creates an http.ReverseProxy which can forward payloads to
// one or more endpoints, based on the request received and the Agent configuration.
// Headers are not proxied, instead we add our own known set of headers.
// See also evpProxyTransport below.
func evpProxyForwarder(conf *config.AgentConfig, endpoints []config.Endpoint, transport http.RoundTripper, logger *stdlog.Logger) http.Handler {
	return &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.Header["X-Forwarded-For"] = nil // Prevent setting X-Forwarded-For
		},
		ErrorLog:  logger,
		Transport: &evpProxyTransport{transport, endpoints, conf},
	}
}

// evpProxyTransport sends HTTPS requests to multiple targets using an
// underlying http.RoundTripper. API keys are set separately for each target.
// When multiple endpoints are in use the response from the first endpoint
// is proxied back to the client, while for all aditional endpoints the
// response is discarded.
type evpProxyTransport struct {
	transport http.RoundTripper
	endpoints []config.Endpoint
	conf      *config.AgentConfig
}

func (t *evpProxyTransport) RoundTrip(req *http.Request) (rresp *http.Response, rerr error) {
	if req.Body != nil && t.conf.EVPProxy.MaxPayloadSize > 0 {
		req.Body = apiutil.NewLimitedReader(req.Body, t.conf.EVPProxy.MaxPayloadSize)
	}

	beginTime := time.Now()
	metricTags := []string{}
	if ct := req.Header.Get("Content-Type"); ct != "" {
		metricTags = append(metricTags, "content_type:"+ct)
	}
	defer func() {
		metrics.Count("datadog.trace_agent.evp_proxy.request", 1, metricTags, 1)
		metrics.Count("datadog.trace_agent.evp_proxy.request_bytes", req.ContentLength, metricTags, 1)
		metrics.Timing("datadog.trace_agent.evp_proxy.request_duration_ms", time.Since(beginTime), metricTags, 1)
		if rerr != nil {
			metrics.Count("datadog.trace_agent.evp_proxy.request_error", 1, metricTags, 1)
		}
	}()

	subdomain := req.Header.Get("X-Datadog-EVP-Subdomain")
	containerID := req.Header.Get("Datadog-Container-ID")
	contentType := req.Header.Get("Content-Type")
	userAgent := req.Header.Get("User-Agent")

	// Sanitize the input, don't accept any valid URL but just some limited subset
	if len(subdomain) == 0 {
		return nil, fmt.Errorf("EVPProxy: no subdomain specified")
	}
	if !isValidSubdomain(subdomain) {
		return nil, fmt.Errorf("EVPProxy: invalid subdomain: %s", subdomain)
	}
	metricTags = append(metricTags, "subdomain:"+subdomain)
	if !isValidPath(req.URL.Path) {
		return nil, fmt.Errorf("EVPProxy: invalid target path: %s", req.URL.Path)
	}
	if !isValidQueryString(req.URL.RawQuery) {
		return nil, fmt.Errorf("EVPProxy: invalid query string: %s", req.URL.RawQuery)
	}

	// We don't want to forward arbitrary headers, clear them
	req.Header = http.Header{}

	// Set standard headers
	req.Header.Set("Via", fmt.Sprintf("trace-agent %s", info.Version))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("User-Agent", userAgent) // Set even if an empty string so Go doesn't set its default

	// Set Datadog headers, except API key which is set per-endpoint
	if ctags := getContainerTags(t.conf.ContainerTags, containerID); ctags != "" {
		req.Header.Set("X-Datadog-Container-Tags", ctags)
	}
	req.Header.Set("X-Datadog-Hostname", t.conf.Hostname)
	req.Header.Set("X-Datadog-AgentDefaultEnv", t.conf.DefaultEnv)

	// Set target URL and API key header (per domain)
	req.URL.Scheme = "https"
	setTarget := func(r *http.Request, host, apiKey string) {
		targetHost := subdomain + "." + host
		r.Host = targetHost
		r.URL.Host = targetHost
		r.Header.Set("DD-API-KEY", apiKey)
	}

	// Shortcut if we only have one endpoint
	if len(t.endpoints) == 1 {
		setTarget(req, t.endpoints[0].Host, t.endpoints[0].APIKey)
		return t.transport.RoundTrip(req)
	}

	// There's more than one destination endpoint
	var slurp []byte
	if req.Body != nil {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		slurp = body
	}
	for i, endpointDomain := range t.endpoints {
		newreq := req.Clone(req.Context())
		if slurp != nil {
			newreq.Body = ioutil.NopCloser(bytes.NewReader(slurp))
		}
		setTarget(newreq, endpointDomain.Host, endpointDomain.APIKey)
		if i == 0 {
			// given the way we construct the list of targets the main endpoint
			// will be the first one called, we return its response and error
			rresp, rerr = t.transport.RoundTrip(newreq)
			continue
		}

		if resp, err := t.transport.RoundTrip(newreq); err == nil {
			// we discard responses for all subsequent requests
			io.Copy(ioutil.Discard, resp.Body) //nolint:errcheck
			resp.Body.Close()
		} else {
			log.Error(err)
		}
	}
	return rresp, rerr
}
