// sample-app: serviço HTTP feito para demonstrar a plataforma (canary, retries, timeouts,
// mTLS, observabilidade). Uma imagem, dois papéis (ROLE):
//
//	frontend: responde em "/" e chama o backend (BACKEND_URL) para buscar uma cotação
//	backend:  responde em "/quote"
//
// Dois serviços se chamando é o mínimo para um trace distribuído mostrar algo útil.
// Botões de caos por env var: FAILURE_RATE, DELAY, HIGH_CARDINALITY.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// version é injetada no build: -ldflags "-X main.version=..."
var version = "dev"

const instrumentation = "github.com/patrickcmartins/sample-app"

type config struct {
	Port        string
	Role        string // frontend | backend
	BackendURL  string // frontend: base do backend, ex. http://backend.sample:8080
	Color       string
	Message     string
	Cluster     string
	FailureRate float64       // 0..1: fração de requests que devolvem 500 (testa retries)
	Delay       time.Duration // latência artificial (testa timeouts)
	// HighCardinality coloca user_id como atributo da métrica: cada usuário vira uma série nova.
	// É o erro clássico que explode o Prometheus. Liga, observa, corrige no Collector.
	HighCardinality bool
}

func loadConfig() config {
	rate, _ := strconv.ParseFloat(getenv("FAILURE_RATE", "0"), 64)
	delay, _ := time.ParseDuration(getenv("DELAY", "0s"))
	hc, _ := strconv.ParseBool(getenv("HIGH_CARDINALITY", "false"))
	return config{
		Port:            getenv("PORT", "8080"),
		Role:            getenv("ROLE", "frontend"),
		BackendURL:      os.Getenv("BACKEND_URL"),
		Color:           getenv("COLOR", "#34577c"),
		Message:         getenv("MESSAGE", "olá"),
		Cluster:         getenv("CLUSTER_NAME", "unknown"),
		FailureRate:     rate,
		Delay:           delay,
		HighCardinality: hc,
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
	Quote    string `json:"quote,omitempty"`
}

var quotes = []string{
	"Hope is not a strategy.",
	"Everything fails, all the time.",
	"You build it, you run it.",
	"If it hurts, do it more often.",
	"Automate the boring stuff.",
}

type app struct {
	cfg    config
	ready  *atomic.Bool
	rnd    func() float64
	log    *slog.Logger
	client *http.Client
	tracer trace.Tracer
	quotes metric.Int64Counter
}

func newApp(cfg config, ready *atomic.Bool, rnd func() float64, log *slog.Logger) *app {
	meter := otel.Meter(instrumentation)
	counter, _ := meter.Int64Counter("quotes_served", metric.WithDescription("Cotações servidas pelo backend"))
	return &app{
		cfg:   cfg,
		ready: ready,
		rnd:   rnd,
		log:   log,
		// otelhttp.NewTransport injeta o traceparent na chamada ao backend: é o que liga os dois spans.
		client: &http.Client{Timeout: 3 * time.Second, Transport: otelhttp.NewTransport(http.DefaultTransport)},
		tracer: otel.Tracer(instrumentation),
		quotes: counter,
	}
}

// chaos aplica os botões de caos. Devolve true se já respondeu (erro ou cancelamento).
func (a *app) chaos(w http.ResponseWriter, r *http.Request) bool {
	span := trace.SpanFromContext(r.Context())
	if a.cfg.Delay > 0 {
		span.AddEvent("chaos.delay", trace.WithAttributes(attribute.String("delay", a.cfg.Delay.String())))
		select {
		case <-time.After(a.cfg.Delay):
		case <-r.Context().Done():
			return true
		}
	}
	if a.cfg.FailureRate > 0 && a.rnd() < a.cfg.FailureRate {
		span.SetStatus(codes.Error, "falha injetada")
		a.log.ErrorContext(r.Context(), "falha injetada", "path", r.URL.Path)
		http.Error(w, "falha injetada", http.StatusInternalServerError)
		return true
	}
	return false
}

func (a *app) handleRoot(w http.ResponseWriter, r *http.Request) {
	if a.chaos(w, r) {
		return
	}
	host, _ := os.Hostname()
	resp := response{version, a.cfg.Color, a.cfg.Message, a.cfg.Cluster, host, ""}
	if a.cfg.BackendURL != "" {
		q, err := a.fetchQuote(r.Context())
		if err != nil {
			// Degradação graciosa: sem backend, o frontend ainda responde (sem a cotação).
			// Retornar 500 aqui transformaria uma falha parcial em falha total.
			a.log.WarnContext(r.Context(), "backend indisponível", "err", err)
			q = "(backend indisponível)"
		}
		resp.Quote = q
	}
	writeJSON(w, resp)
}

func (a *app) fetchQuote(ctx context.Context) (string, error) {
	ctx, span := a.tracer.Start(ctx, "fetchQuote")
	defer span.End()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.cfg.BackendURL+"/quote", nil)
	if err != nil {
		return "", err
	}
	res, err := a.client.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		err := fmt.Errorf("backend respondeu %d", res.StatusCode)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	var body struct {
		Quote string `json:"quote"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return "", err
	}
	return body.Quote, nil
}

func (a *app) handleQuote(w http.ResponseWriter, r *http.Request) {
	if a.chaos(w, r) {
		return
	}
	q := quotes[int(a.rnd()*float64(len(quotes)))%len(quotes)]
	attrs := []attribute.KeyValue{attribute.String("cluster", a.cfg.Cluster)}
	if a.cfg.HighCardinality {
		// ERRADO de propósito: valor ilimitado como label = uma série por usuário.
		attrs = append(attrs, attribute.String("user_id", strconv.Itoa(rand.Intn(1_000_000))))
	}
	a.quotes.Add(r.Context(), 1, metric.WithAttributes(attrs...))
	a.log.InfoContext(r.Context(), "quote", "quote", q)
	writeJSON(w, map[string]string{"quote": q})
}

func (a *app) routes() http.Handler {
	mux := http.NewServeMux()
	// Só as rotas de negócio passam pelo otelhttp, cada uma com nome fixo de span (a rota,
	// não a URL crua: URL no nome = cardinalidade alta). Probes ficam fora do tracing.
	handle := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, otelhttp.NewHandler(h, "GET "+pattern))
	}
	switch a.cfg.Role {
	case "backend":
		handle("/quote", a.handleQuote)
	default:
		handle("/", a.handleRoot)
	}
	mux.HandleFunc("/version", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]string{"version": version, "role": a.cfg.Role})
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !a.ready.Load() {
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
	log := slog.New(traceHandler{slog.NewJSONHandler(os.Stdout, nil)})
	slog.SetDefault(log)
	cfg := loadConfig()

	ctx := context.Background()
	shutdownTelemetry, err := setupTelemetry(ctx)
	if err != nil {
		log.Error("telemetry", "err", err)
		os.Exit(1)
	}

	var ready atomic.Bool
	a := newApp(cfg, &ready, rand.Float64, log)
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           a.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("starting", "version", version, "role", cfg.Role, "port", cfg.Port, "cluster", cfg.Cluster)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server", "err", err)
			os.Exit(1)
		}
	}()
	ready.Store(true)

	// Graceful shutdown: sai do readiness, espera o endpoint sumir do Service, drena conexões,
	// e só então descarrega os spans/métricas pendentes.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	ready.Store(false)
	log.Info("shutting down")
	time.Sleep(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	_ = shutdownTelemetry(ctx)
}
