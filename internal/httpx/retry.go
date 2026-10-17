// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright © 2025 Ronmi Ren <ronmi.ren@gmail.com>

// Package httpx provides a resilient HTTP client for talking to a Forgejo
// instance that sits behind an nginx rate limiter. The limiter answers
// over-budget requests with HTTP 429; without retries every burst of parallel
// MCP tool calls would surface those rejections to the MCP client, and a
// single 429 during the SDK's startup version check would abort the server.
package httpx

import (
	"context"
	"io"
	"net/http"
	"time"
)

// Defaults for RetryTransport.
const (
	defaultMaxAttempts     = 4
	defaultBaseDelay       = 250 * time.Millisecond
	defaultMaxDelay        = 8 * time.Second
	defaultRetryAfterLimit = 30 * time.Second

	// maxDrain bounds how much of a rejected response body we consume
	// before discarding it, so draining cannot itself hang on a body of
	// unbounded size.
	maxDrain = 64 << 10
)

// RetryTransport is an http.RoundTripper that retries transient failures.
// The zero value is usable; unset fields fall back to the defaults above.
type RetryTransport struct {
	// Inner is the transport actually used to reach the server.
	// Defaults to http.DefaultTransport so the standard library's proxy
	// and TLS behaviour is preserved.
	Inner http.RoundTripper

	// MaxAttempts is the total number of attempts per request (the first
	// try plus retries). Defaults to 4.
	MaxAttempts int

	// BaseDelay is the pause before the second attempt; each subsequent
	// retry doubles it. Defaults to 250ms.
	BaseDelay time.Duration

	// MaxDelay caps the exponential backoff. Defaults to 8s.
	MaxDelay time.Duration

	// RetryAfterLimit clamps a server-supplied Retry-After so a hostile or
	// buggy value cannot stall us for hours. Defaults to 30s.
	RetryAfterLimit time.Duration

	// Sleep waits for d or returns early when ctx is done. It is
	// injectable so tests need not spend real wall-clock time. When nil,
	// a timer bound to ctx is used.
	Sleep func(ctx context.Context, d time.Duration) error
}

// NewRetryTransport returns a RetryTransport wrapping inner, or
// http.DefaultTransport when inner is nil.
func NewRetryTransport(inner http.RoundTripper) *RetryTransport {
	if inner == nil {
		inner = http.DefaultTransport
	}
	return &RetryTransport{Inner: inner}
}

func (t *RetryTransport) inner() http.RoundTripper {
	if t.Inner != nil {
		return t.Inner
	}
	return http.DefaultTransport
}

func (t *RetryTransport) maxAttempts() int {
	if t.MaxAttempts > 0 {
		return t.MaxAttempts
	}
	return defaultMaxAttempts
}

func (t *RetryTransport) baseDelay() time.Duration {
	if t.BaseDelay > 0 {
		return t.BaseDelay
	}
	return defaultBaseDelay
}

func (t *RetryTransport) maxDelay() time.Duration {
	if t.MaxDelay > 0 {
		return t.MaxDelay
	}
	return defaultMaxDelay
}

func (t *RetryTransport) retryAfterLimit() time.Duration {
	if t.RetryAfterLimit > 0 {
		return t.RetryAfterLimit
	}
	return defaultRetryAfterLimit
}

func (t *RetryTransport) sleep(ctx context.Context, d time.Duration) error {
	if t.Sleep != nil {
		return t.Sleep(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// RoundTrip implements http.RoundTripper.
func (t *RetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// A request may only be retried if its body can be replayed: either
	// there is no body, or GetBody gives us a fresh one per attempt.
	// Streaming bodies (multipart uploads) have GetBody == nil; re-sending
	// them after the first attempt consumed the reader would upload a
	// silently truncated file, so we must not retry those.
	replayable := req.Body == nil || req.GetBody != nil

	attempts := t.maxAttempts()
	for i := 1; ; i++ {
		r := req
		if i > 1 {
			var err error
			r, err = t.replayRequest(req)
			if err != nil {
				return nil, err
			}
		}
		res, err := t.inner().RoundTrip(r)
		if !t.retryable(req, replayable, err, res) || i == attempts {
			return res, err
		}
		// The rejected response is discarded; its body must be drained
		// and closed or the underlying connection is never returned to
		// the pool and leaks.
		drain(res)
		if serr := t.sleep(req.Context(), t.baseDelay()); serr != nil {
			return nil, serr
		}
	}
}

// replayRequest builds the attempt request for a retry: a clone of the
// original carrying a fresh copy of the body. RoundTrippers must not modify
// the request handed to them, so every attempt after the first uses a clone.
func (t *RetryTransport) replayRequest(req *http.Request) (*http.Request, error) {
	r := req.Clone(req.Context())
	if req.Body != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		r.Body = body
	}
	return r, nil
}

// retryable reports whether this outcome deserves another attempt.
func (t *RetryTransport) retryable(req *http.Request, replayable bool, err error, res *http.Response) bool {
	if !replayable {
		return false
	}
	if err != nil {
		// Transport-level failure: the request never completed.
		return true
	}
	switch res.StatusCode {
	case http.StatusTooManyRequests, // 429: the nginx rate limiter; explicitly invited to retry
		http.StatusServiceUnavailable: // 503: rejected at the edge, the app never saw the request
		return true
	case http.StatusBadGateway, http.StatusGatewayTimeout:
		// 502/504 mean the gateway gave up; the application may have
		// partially processed the request. Only retry when re-sending
		// cannot duplicate an effect, i.e. for idempotent methods.
		return isIdempotent(req.Method)
	}
	return false
}

// isIdempotent reports whether repeating the method is safe. It covers the
// methods RFC 9110 defines as idempotent; POST, PATCH and unknown methods are
// not.
func isIdempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete,
		http.MethodOptions, http.MethodTrace:
		return true
	}
	return false
}

// drain consumes and closes res.Body so the connection can be reused.
func drain(res *http.Response) {
	if res == nil || res.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, maxDrain))
	_ = res.Body.Close()
}
