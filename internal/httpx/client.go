// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright © 2025 Ronmi Ren <ronmi.ren@gmail.com>

package httpx

import (
	"net"
	"net/http"
	"time"
)

// NewResilientClient builds the *http.Client used to reach Forgejo. It
// combines three fixes over the previous http.DefaultClient:
//
//  1. Retry on transient failures (429 from the nginx rate limiter, edge
//     rejections, network errors) via RetryTransport.
//  2. An overall 60s timeout: without one, a hung request pinned an MCP
//     tool slot forever.
//  3. A connection pool sized for bursts of parallel tool calls. Go's
//     default MaxIdleConnsPerHost is 2, so anything beyond two concurrent
//     requests caused constant dial churn against the TLS endpoint.
//
// The SDK version check and every wrapper endpoint share this client: both
// request paths ultimately call client.Do on the injected *http.Client.
func NewResilientClient() *http.Client {
	inner := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
	}
	return &http.Client{
		Timeout:   60 * time.Second,
		Transport: NewRetryTransport(inner),
	}
}
