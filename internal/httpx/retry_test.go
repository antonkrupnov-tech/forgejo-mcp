// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright © 2025 Ronmi Ren <ronmi.ren@gmail.com>

package httpx_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

// nonSeekReader makes http.NewRequest leave GetBody nil, mimicking a
// streaming upload (like a multipart release-asset body).
type nonSeekReader struct{ io.Reader }

func TestRetryTransport_DoesNotRetryNonReplayableBody(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	// Body consumed on the first attempt; without GetBody we cannot replay
	// it. Retrying would send a truncated (or empty) upload and silently
	// corrupt the file, so the 503 must be passed through untouched.
	req, err := http.NewRequest(http.MethodPost, srv.URL, nonSeekReader{strings.NewReader("payload")})
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	tr := &httpx.RetryTransport{BaseDelay: time.Millisecond}
	client := &http.Client{Transport: tr}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: unexpected error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Errorf("server saw %d requests, want exactly 1", got)
	}
}

func TestRetryTransport_ReplaysBodyOnRetry(t *testing.T) {
	const payload = "hello payload"
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		if len(bodies) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, string(b))
	}))
	defer srv.Close()

	// bytes.NewReader gives the request a GetBody, so the body is
	// replayable and the retry must carry the full payload again.
	req, err := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	tr := &httpx.RetryTransport{BaseDelay: time.Millisecond}
	client := &http.Client{Transport: tr}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: unexpected error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if len(bodies) != 2 {
		t.Fatalf("server saw %d requests, want exactly 2", len(bodies))
	}
	if bodies[1] != payload {
		t.Errorf("retried request body = %q, want %q", bodies[1], payload)
	}
	respBody, _ := io.ReadAll(resp.Body)
	if string(respBody) != payload {
		t.Errorf("response body = %q, want %q", respBody, payload)
	}
}

// TestRetryTransport_HonoursRetryAfterDeltaSeconds verifies that a
// server-supplied Retry-After (delta-seconds form) overrides our own backoff.
func TestRetryTransport_HonoursRetryAfterDeltaSeconds(t *testing.T) {
	var slept []time.Duration
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&requests, 1) == 1 {
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	tr := &httpx.RetryTransport{
		// BaseDelay tiny; if Retry-After is ignored the recorded sleep
		// would be ~1ms instead of 7s.
		BaseDelay: time.Millisecond,
		Sleep: func(_ context.Context, d time.Duration) error {
			slept = append(slept, d)
			return nil
		},
	}
	client := &http.Client{Transport: tr}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if len(slept) != 1 {
		t.Fatalf("recorded %d sleeps, want 1", len(slept))
	}
	if slept[0] != 7*time.Second {
		t.Errorf("sleep = %v, want exactly 7s from Retry-After", slept[0])
	}
}

// Retry-After: 0 is valid ("retry now") and must not be treated as absent.
func TestRetryTransport_HonoursRetryAfterZero(t *testing.T) {
	var slept []time.Duration
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&requests, 1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	tr := &httpx.RetryTransport{
		BaseDelay: 5 * time.Second, // would show up in slept if Retry-After were ignored
		Sleep: func(_ context.Context, d time.Duration) error {
			slept = append(slept, d)
			return nil
		},
	}
	client := &http.Client{Transport: tr}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if len(slept) != 1 || slept[0] != 0 {
		t.Errorf("sleeps = %v, want exactly [0s] from Retry-After: 0", slept)
	}
}

func TestRetryTransport_HonoursRetryAfterHTTPDate(t *testing.T) {
	var slept []time.Duration
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&requests, 1) == 1 {
			w.Header().Set("Retry-After", time.Now().UTC().Add(3*time.Second).Format(http.TimeFormat))
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	tr := &httpx.RetryTransport{
		BaseDelay: time.Millisecond,
		Sleep: func(_ context.Context, d time.Duration) error {
			slept = append(slept, d)
			return nil
		},
	}
	client := &http.Client{Transport: tr}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if len(slept) != 1 {
		t.Fatalf("recorded %d sleeps, want 1", len(slept))
	}
	// HTTP-date has one-second resolution; accept anything in (2s, 3s].
	if slept[0] <= 2*time.Second || slept[0] > 3*time.Second {
		t.Errorf("sleep = %v, want a duration derived from the HTTP-date (~3s)", slept[0])
	}
}

// A hostile or buggy Retry-After must be clamped so it cannot stall the client.
func TestRetryTransport_ClampsHugeRetryAfter(t *testing.T) {
	var slept []time.Duration
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&requests, 1) == 1 {
			w.Header().Set("Retry-After", "999999") // ~11.5 days
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	tr := &httpx.RetryTransport{
		RetryAfterLimit: 2 * time.Second,
		Sleep: func(_ context.Context, d time.Duration) error {
			slept = append(slept, d)
			return nil
		},
	}
	client := &http.Client{Transport: tr}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if len(slept) != 1 || slept[0] > 2*time.Second {
		t.Errorf("sleeps = %v, want clamped to <= 2s", slept)
	}
}

// Backoff grows exponentially with attempt count and stays within jittered bounds.
func TestRetryTransport_ExponentialBackoff(t *testing.T) {
	var slept []time.Duration
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests) // never succeeds
	}))
	defer srv.Close()

	tr := &httpx.RetryTransport{
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    300 * time.Millisecond,
		MaxAttempts: 4,
		Sleep: func(_ context.Context, d time.Duration) error {
			slept = append(slept, d)
			return nil
		},
	}
	client := &http.Client{Transport: tr}

	_, err := client.Get(srv.URL)
	// Exhaustion behaviour is covered by its own test; here we only inspect delays.
	_ = err
	if len(slept) != 3 {
		t.Fatalf("recorded %d sleeps, want 3 (one per retry)", len(slept))
	}
	// Exponential: 100ms, 200ms, capped at 300ms; jitter must keep each delay
	// in [cap/2, cap] of the un-jittered window for that attempt.
	windows := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond}
	for i, d := range slept {
		hi := windows[i]
		lo := hi / 2
		if d < lo || d > hi {
			t.Errorf("sleep %d = %v, want in [%v, %v]", i, d, lo, hi)
		}
	}
}

// Context cancellation must abandon the retry loop promptly instead of
// sleeping through the remaining attempts.
func TestRetryTransport_ContextCancelAbortsRetries(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	// Long base delay: without cancellation-aware sleeping this test would
	// take minutes.
	tr := &httpx.RetryTransport{BaseDelay: time.Hour}
	client := &http.Client{Transport: tr}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Cancel while the transport is in its first backoff sleep.
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	start := time.Now()
	_, err = client.Do(req)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Do: want error from cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Do error = %v, want it to wrap context.Canceled", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("cancellation took %v to take effect, want prompt abort", elapsed)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Errorf("server saw %d requests, want 1 (cancelled during first sleep)", got)
	}
}

// When every attempt fails with a retryable status, the caller gets a clear
// error naming the final status instead of a silently-swallowed response.
func TestRetryTransport_ExhaustedRetriesReportFinalStatus(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	tr := &httpx.RetryTransport{
		MaxAttempts: 2,
		BaseDelay:   time.Millisecond,
		Sleep:       func(context.Context, time.Duration) error { return nil },
	}
	client := &http.Client{Transport: tr}

	_, err := client.Get(srv.URL)
	if err == nil {
		t.Fatal("want error after exhausting retries, got nil response")
	}
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("error %q does not mention the final status 429", err)
	}
	if !strings.Contains(err.Error(), "2") {
		t.Errorf("error %q does not mention the attempt count", err)
	}
	if got := atomic.LoadInt32(&requests); got != 2 {
		t.Errorf("server saw %d requests, want exactly 2", got)
	}
}

// A dead endpoint must not loop forever: transport errors are retried up to
// MaxAttempts and then reported.
func TestRetryTransport_ExhaustedTransportErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := srv.URL
	srv.Close() // nothing is listening now

	tr := &httpx.RetryTransport{
		MaxAttempts: 2,
		BaseDelay:   time.Millisecond,
		Sleep:       func(context.Context, time.Duration) error { return nil },
	}
	client := &http.Client{Transport: tr}

	_, err := client.Get(deadURL)
	if err == nil {
		t.Fatal("want error when the endpoint is dead")
	}
	if !strings.Contains(err.Error(), "2 attempts") {
		t.Errorf("error %q should mention the attempt count", err)
	}
}

// The resilient client must fix the three operational gaps: no overall
// timeout, no retry, and Go's MaxIdleConnsPerHost default of 2.
func TestNewResilientClient(t *testing.T) {
	client := httpx.NewResilientClient()

	if client.Timeout <= 0 {
		t.Errorf("client.Timeout = %v, want a positive overall timeout", client.Timeout)
	}

	rt, ok := client.Transport.(*httpx.RetryTransport)
	if !ok {
		t.Fatalf("Transport = %T, want *httpx.RetryTransport", client.Transport)
	}
	inner, ok := rt.Inner.(*http.Transport)
	if !ok {
		t.Fatalf("RetryTransport.Inner = %T, want *http.Transport", rt.Inner)
	}

	if inner.MaxIdleConnsPerHost <= 2 {
		t.Errorf("MaxIdleConnsPerHost = %d, want well above Go's default of 2", inner.MaxIdleConnsPerHost)
	}
	if inner.MaxIdleConns <= 0 {
		t.Errorf("MaxIdleConns = %d, want a raised pool limit", inner.MaxIdleConns)
	}
	if inner.IdleConnTimeout <= 0 {
		t.Errorf("IdleConnTimeout = %v, want a finite idle timeout", inner.IdleConnTimeout)
	}
	if inner.TLSHandshakeTimeout <= 0 {
		t.Errorf("TLSHandshakeTimeout = %v, want a finite handshake timeout", inner.TLSHandshakeTimeout)
	}
	if inner.DialContext == nil {
		t.Error("DialContext = nil, want a dialer with an explicit connect timeout")
	}
}
