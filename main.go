// sample-app: serviço HTTP feito para demonstrar a plataforma (canary, retries, timeouts,
// mTLS, observabilidade, cache, eventos). Uma imagem, três papéis (ROLE):
//
//	frontend: responde em "/", chama o backend (BACKEND_URL) e publica um evento no Kafka
//	backend:  responde em "/quote", com cache-aside no Redis (cache.go)
//	worker:   consome os eventos do Kafka e agrega no Redis; expõe "/stats" (events.go)
//
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
	Role        string // frontend | backend | worker
	BackendURL  string // frontend: base do backend, ex. http://backend.sample:8080
	Color       string
	Message     string
	Cluster     string
	FailureRate float64       // 0..1: fração de requests que devolvem 500 (testa retries)
	Delay       time.Duration // latência artificial (testa timeouts)
	// HighCardinality coloca user_id como atributo da métrica: cada usuário vira uma série nova.
	// É o erro clássico que explode o Prometheus. Liga, observa, corrige no Collector.
	HighCardinality bool

	RedisAddr     string        // backend/worker, ex. redis.sample:6379 (vazio = sem cache)
	RedisTimeout  time.Duration // timeout por comando no caminho quente
	CacheTTL      time.Duration
	SourceLatency time.Duration // latência da "fonte" lenta que o cache protege
	KafkaBrokers  string        // frontend/worker, ex. events-kafka-bootstrap.kafka:9092 (vazio = sem eventos)
	KafkaTopic    string
	KafkaGroup    string // worker: consumer group
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
		RedisAddr:       os.Getenv("REDIS_ADDR"),
		RedisTimeout:    duration("REDIS_TIMEOUT", "100ms"),
		CacheTTL:        duration("CACHE_TTL", "60s"),
		SourceLatency:   duration("SOURCE_LATENCY", "200ms"),
		KafkaBrokers:    os.Getenv("KAFKA_BROKERS"),
		KafkaTopic:      getenv("KAFKA_TOPIC", "quotes.served"),
		KafkaGroup:      getenv("KAFKA_GROUP", "quote-stats"),
	}
}

func duration(k, def string) time.Duration {
	d, err := time.ParseDuration(getenv(k, def))
	if err != nil {
		d, _ = time.ParseDuration(def)
	}
	return d
}

func itoa(i int) string { return strconv.Itoa(i) }

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
	cache  *quoteCache // backend
	pub    *publisher  // frontend
	worker *worker     // worker
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
		// Defaults desligados (sem Redis, sem Kafka): main() troca pelos reais conforme o papel.
		cache: &quoteCache{timeout: 100 * time.Millisecond, sourceMs: 0},
		pub:   &publisher{log: log},
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
	id := int(a.rnd()*float64(len(quotes))) % len(quotes)
	if a.cfg.BackendURL != "" {
		q, err := a.fetchQuote(r.Context(), id)
		if err != nil {
			// Degradação graciosa: sem backend, o frontend ainda responde (sem a cotação).
			// Retornar 500 aqui transformaria uma falha parcial em falha total.
			a.log.WarnContext(r.Context(), "backend indisponível", "err", err)
			q = "(backend indisponível)"
		}
		resp.Quote = q
	}
	writeJSON(w, resp)
	// Depois de responder: publica o evento (assíncrono, não atrasa o usuário).
	a.pub.publish(r.Context(), quoteEvent{QuoteID: id, Version: version, At: time.Now()})
}

func (a *app) fetchQuote(ctx context.Context, id int) (string, error) {
	ctx, span := a.tracer.Start(ctx, "fetchQuote")
	defer span.End()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.cfg.BackendURL+"/quote?id="+itoa(id), nil)
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
	id, _ := strconv.Atoi(r.URL.Query().Get("id"))
	if id < 0 {
		id = 0
	}
	q, source := a.cache.get(r.Context(), id)
	attrs := []attribute.KeyValue{attribute.String("cluster", a.cfg.Cluster)}
	if a.cfg.HighCardinality {
		// ERRADO de propósito: valor ilimitado como label = uma série por usuário.
		attrs = append(attrs, attribute.String("user_id", strconv.Itoa(rand.Intn(1_000_000))))
	}
	a.quotes.Add(r.Context(), 1, metric.WithAttributes(attrs...))
	a.log.InfoContext(r.Context(), "quote", "quote", q, "source", source)
	writeJSON(w, map[string]string{"quote": q, "source": source})
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
	case "worker":
		mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
			if a.worker == nil {
				http.Error(w, "worker sem Kafka/Redis configurado", http.StatusServiceUnavailable)
				return
			}
			st, err := a.worker.stats(r.Context())
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			writeJSON(w, st)
		})
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

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	switch cfg.Role {
	case "backend":
		if a.cache, err = newQuoteCache(cfg.RedisAddr, cfg.CacheTTL, cfg.RedisTimeout, cfg.SourceLatency); err != nil {
			log.Error("redis", "err", err)
			os.Exit(1)
		}
		defer a.cache.close()
	case "worker":
		if cfg.KafkaBrokers == "" || cfg.RedisAddr == "" {
			log.Error("worker precisa de KAFKA_BROKERS e REDIS_ADDR")
			os.Exit(1)
		}
		if a.worker, err = newWorker(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup, cfg.RedisAddr, cfg.Delay, log); err != nil {
			log.Error("kafka", "err", err)
			os.Exit(1)
		}
		go a.worker.run(runCtx)
	default:
		if a.pub, err = newPublisher(cfg.KafkaBrokers, cfg.KafkaTopic, log); err != nil {
			log.Error("kafka", "err", err)
			os.Exit(1)
		}
	}
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
	stopRun()
	a.pub.close() // flush dos eventos pendentes antes de sair
	if a.worker != nil {
		a.worker.close()
	}
	_ = shutdownTelemetry(ctx)
}
