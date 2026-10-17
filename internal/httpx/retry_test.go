// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright © 2025 Ronmi Ren <ronmi.ren@gmail.com>

package httpx_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raohwork/forgejo-mcp/internal/httpx"
)

func TestRetryTransport_Retries429ThenSucceeds(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&requests, 1) <= 2 {
			// nginx rate limit: reject at the edge.
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	// Near-zero base delay keeps the test fast.
	tr := &httpx.RetryTransport{BaseDelay: time.Millisecond}
	client := &http.Client{Transport: tr}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", body, "ok")
	}
	if got := atomic.LoadInt32(&requests); got != 3 {
		t.Errorf("server saw %d requests, want exactly 3", got)
	}
}

func TestRetryTransport_Retries503(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&requests, 1) == 1 {
			// 503 is also rejected at the edge before reaching the app.
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	tr := &httpx.RetryTransport{BaseDelay: time.Millisecond}
	client := &http.Client{Transport: tr}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&requests); got != 2 {
		t.Errorf("server saw %d requests, want exactly 2", got)
	}
}

// doOnce issues one request with the given method against a transport/client
// pair built for tests and returns the response.
func doOnce(t *testing.T, client *http.Client, method, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("NewRequest(%s): %v", method, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: unexpected error: %v", method, url, err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestRetryTransport_502RetriedOnlyForIdempotentMethods(t *testing.T) {
	cases := []struct {
		method      string
		wantRequest int32
	}{
		// 502/504 mean the gateway gave up; the app may have partially
		// processed the request. Retrying a non-idempotent method could
		// duplicate its effect, so only idempotent methods are retried.
		{http.MethodGet, 2},
		{http.MethodPost, 1},
	}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			var requests int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if atomic.AddInt32(&requests, 1) == 1 {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				fmt.Fprint(w, "ok")
			}))
			defer srv.Close()

			tr := &httpx.RetryTransport{BaseDelay: time.Millisecond}
			client := &http.Client{Transport: tr}

			resp := doOnce(t, client, tc.method, srv.URL)
			if got := atomic.LoadInt32(&requests); got != tc.wantRequest {
				t.Errorf("%s: server saw %d requests, want %d", tc.method, got, tc.wantRequest)
			}
			want := http.StatusOK
			if tc.wantRequest == 1 {
				want = http.StatusBadGateway // not retried: passed through as-is
			}
			if resp.StatusCode != want {
				t.Errorf("%s: status = %d, want %d", tc.method, resp.StatusCode, want)
			}
		})
	}
}

func TestRetryTransport_Retries504ForGet(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&requests, 1) == 1 {
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	tr := &httpx.RetryTransport{BaseDelay: time.Millisecond}
	client := &http.Client{Transport: tr}

	resp := doOnce(t, client, http.MethodGet, srv.URL)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&requests); got != 2 {
		t.Errorf("server saw %d requests, want exactly 2", got)
	}
}

func TestRetryTransport_NeverRetriesOther4xx(t *testing.T) {
	// 401/403/404/422 are semantic answers from the application, not
	// transient faults; a retry would be wrong regardless of method.
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, 422} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var requests int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&requests, 1)
				w.WriteHeader(status)
			}))
			defer srv.Close()

			tr := &httpx.RetryTransport{BaseDelay: time.Millisecond}
			client := &http.Client{Transport: tr}

			resp := doOnce(t, client, http.MethodGet, srv.URL)
			if resp.StatusCode != status {
				t.Errorf("status = %d, want %d", resp.StatusCode, status)
			}
			if got := atomic.LoadInt32(&requests); got != 1 {
				t.Errorf("server saw %d requests, want exactly 1", got)
			}
		})
	}
}
