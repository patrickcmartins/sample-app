// sample-app: serviço HTTP mínimo (só stdlib) feito para demonstrar
// canary, retries, timeouts e mTLS do Istio. Tem "botões" de caos por env var.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

// version é injetada no build: -ldflags "-X main.version=..."
var version = "dev"

type config struct {
	Port        string
	Color       string
	Message     string
	Cluster     string
	FailureRate float64       // 0..1: fração de requests que devolvem 500 (testa retries)
	Delay       time.Duration // latência artificial (testa timeouts)
}

func loadConfig() config {
	rate, _ := strconv.ParseFloat(getenv("FAILURE_RATE", "0"), 64)
	delay, _ := time.ParseDuration(getenv("DELAY", "0s"))
	return config{
		Port:        getenv("PORT", "8080"),
		Color:       getenv("COLOR", "#34577c"),
		Message:     getenv("MESSAGE", "olá"),
		Cluster:     getenv("CLUSTER_NAME", "unknown"),
		FailureRate: rate,
		Delay:       delay,
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type response struct {
	Version  string `json:"version"`
	Color    string `json:"color"`
	Message  string `json:"message"`
	Cluster  string `json:"cluster"`
	Hostname string `json:"hostname"`
}

func newMux(cfg config, ready *atomic.Bool, rnd func() float64) *http.ServeMux {
	host, _ := os.Hostname()
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if cfg.Delay > 0 {
			select {
			case <-time.After(cfg.Delay):
			case <-r.Context().Done():
				return
			}
		}
		if cfg.FailureRate > 0 && rnd() < cfg.FailureRate {
			http.Error(w, "falha injetada", http.StatusInternalServerError)
			return
		}
		writeJSON(w, response{version, cfg.Color, cfg.Message, cfg.Cluster, host})
	})
	mux.HandleFunc("/version", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]string{"version": version})
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()

	var ready atomic.Bool
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           newMux(cfg, &ready, rand.Float64),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("starting", "version", version, "port", cfg.Port, "cluster", cfg.Cluster)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server", "err", err)
			os.Exit(1)
		}
	}()
	ready.Store(true)

	// Graceful shutdown: sai do readiness, espera o endpoint sumir do Service, drena conexões.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	ready.Store(false)
	log.Info("shutting down")
	time.Sleep(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
