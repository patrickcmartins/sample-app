package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func testApp(cfg config, ready bool, rnd float64) http.Handler {
	var r atomic.Bool
	r.Store(ready)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return newApp(cfg, &r, func() float64 { return rnd }, log).routes()
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestRootReturnsConfig(t *testing.T) {
	rec := get(t, testApp(config{Color: "red", Message: "oi", Cluster: "kind-dev"}, true, 0.9), "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var got response
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Color != "red" || got.Cluster != "kind-dev" || got.Quote != "" {
		t.Fatalf("resposta inesperada: %+v", got)
	}
}

func TestFrontendCallsBackend(t *testing.T) {
	backend := httptest.NewServer(testApp(config{Role: "backend"}, true, 0))
	defer backend.Close()

	rec := get(t, testApp(config{BackendURL: backend.URL}, true, 0.9), "/")
	var got response
	_ = json.NewDecoder(rec.Body).Decode(&got)
	if got.Quote != quotes[0] {
		t.Fatalf("esperava a cotação do backend, veio %q", got.Quote)
	}
}

func TestFrontendDegradesWhenBackendFails(t *testing.T) {
	backend := httptest.NewServer(testApp(config{Role: "backend", FailureRate: 1}, true, 0))
	defer backend.Close()

	rec := get(t, testApp(config{BackendURL: backend.URL}, true, 0.9), "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("falha do backend não deveria derrubar o frontend, veio %d", rec.Code)
	}
}

func TestFailureInjection(t *testing.T) {
	if c := get(t, testApp(config{FailureRate: 0.5}, true, 0.1), "/").Code; c != http.StatusInternalServerError {
		t.Fatalf("esperava 500, veio %d", c)
	}
}

func TestBackendOnlyServesQuote(t *testing.T) {
	h := testApp(config{Role: "backend"}, true, 0)
	if c := get(t, h, "/quote").Code; c != http.StatusOK {
		t.Fatalf("/quote deveria ser 200, veio %d", c)
	}
	if c := get(t, h, "/").Code; c != http.StatusNotFound {
		t.Fatalf("backend não serve /, veio %d", c)
	}
}

func TestReadiness(t *testing.T) {
	if c := get(t, testApp(config{}, false, 0), "/readyz").Code; c != http.StatusServiceUnavailable {
		t.Fatalf("not ready deveria ser 503, veio %d", c)
	}
	if c := get(t, testApp(config{}, true, 0), "/readyz").Code; c != http.StatusOK {
		t.Fatalf("ready deveria ser 200, veio %d", c)
	}
}
