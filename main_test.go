package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func serve(t *testing.T, cfg config, ready bool, rnd float64, path string) *httptest.ResponseRecorder {
	t.Helper()
	var r atomic.Bool
	r.Store(ready)
	rec := httptest.NewRecorder()
	newMux(cfg, &r, func() float64 { return rnd }).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestRootReturnsConfig(t *testing.T) {
	rec := serve(t, config{Color: "red", Message: "oi", Cluster: "kind-dev"}, true, 0.9, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var got response
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Color != "red" || got.Cluster != "kind-dev" {
		t.Fatalf("resposta inesperada: %+v", got)
	}
}

func TestFailureInjection(t *testing.T) {
	rec := serve(t, config{FailureRate: 0.5}, true, 0.1, "/")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("esperava 500, veio %d", rec.Code)
	}
}

func TestReadiness(t *testing.T) {
	if c := serve(t, config{}, false, 0, "/readyz").Code; c != http.StatusServiceUnavailable {
		t.Fatalf("not ready deveria ser 503, veio %d", c)
	}
	if c := serve(t, config{}, true, 0, "/readyz").Code; c != http.StatusOK {
		t.Fatalf("ready deveria ser 200, veio %d", c)
	}
}
