// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright © 2025 Ronmi Ren <ronmi.ren@gmail.com>

package tools

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/raohwork/forgejo-mcp/internal/httpx"
)

// The SDK performs GET /api/v1/version inside forgejo.NewClient, *after*
// applying ClientOptions, so an injected retrying http.Client covers the
// startup check too. This test pins that wiring: a single 429 at startup
// must not fail client creation.
func TestNewClient_SurvivesTransient429AtVersionCheck(t *testing.T) {
	var versionReqs int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/version" {
			http.NotFound(w, r)
			return
		}
		if atomic.AddInt32(&versionReqs, 1) == 1 {
			// nginx rate limiter in front of Forgejo.
			w.Header().Set("Retry-After", "0") // keep the test fast
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"` + forgejo_version_to_test + `"}`))
	}))
	defer srv.Close()

	cl, err := NewClient(srv.URL, "token", "", httpx.NewResilientClient())
	if err != nil {
		t.Fatalf("NewClient failed despite retryable 429 at startup: %v", err)
	}
	if cl == nil {
		t.Fatal("NewClient returned nil client")
	}
	if got := atomic.LoadInt32(&versionReqs); got != 2 {
		t.Errorf("version endpoint hit %d times, want 2 (429 then success)", got)
	}
}

// A persistent 429 exhausts the retries and surfaces as an error rather than
// hanging forever; this is the failure mode cmd packages must tolerate.
func TestNewClient_VersionCheckFailsAfterRetryExhaustion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	cl, err := NewClient(srv.URL, "token", "", httpx.NewResilientClient())
	if err == nil {
		t.Fatal("want error when every attempt is a 429, got nil")
	}
	if cl != nil {
		t.Error("client should be nil when the version check fails hard")
	}
}
