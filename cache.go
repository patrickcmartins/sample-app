package main

// Redis como cache (padrão cache-aside) no backend.
//
//   1. GET no Redis. Achou (hit): responde.
//   2. Não achou (miss): busca na "fonte" (lenta, simula um banco) e grava no Redis com TTL.
//   3. Redis lento ou fora: FAIL-OPEN. Vai direto à fonte. O cache é otimização, não dependência:
//      se a queda do Redis derrubasse o backend, um componente opcional viraria ponto único de falha.
//
// O timeout curto (REDIS_TIMEOUT, default 100ms) é o que impede um Redis lento de segurar
// todas as goroutines do backend (o "threads pile up" do experimento de caos).

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type quoteCache struct {
	rdb      *redis.Client // nil = cache desligado
	ttl      time.Duration
	timeout  time.Duration
	lookups  metric.Int64Counter
	sourceMs time.Duration // latência da "fonte" (o banco simulado)
}

func newQuoteCache(addr string, ttl, timeout, sourceLatency time.Duration) (*quoteCache, error) {
	lookups, _ := otel.Meter(instrumentation).Int64Counter("quote_cache_lookups",
		metric.WithDescription("Consultas ao cache por resultado: hit, miss, error"))
	c := &quoteCache{ttl: ttl, timeout: timeout, lookups: lookups, sourceMs: sourceLatency}
	if addr == "" {
		return c, nil
	}
	c.rdb = redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  timeout,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
		PoolSize:     20,
		MaxRetries:   0, // sem retry: no caminho quente, falhar rápido e ir à fonte é melhor
	})
	// Um span por comando Redis (GET/SET aparecem no trace do request).
	if err := redisotel.InstrumentTracing(c.rdb); err != nil {
		return nil, err
	}
	return c, nil
}

// get devolve a cotação e de onde ela veio ("cache" ou "source").
func (c *quoteCache) get(ctx context.Context, id int) (string, string) {
	key := "quote:" + itoa(id)
	if c.rdb != nil {
		rctx, cancel := context.WithTimeout(ctx, c.timeout)
		v, err := c.rdb.Get(rctx, key).Result()
		cancel()
		switch {
		case err == nil:
			c.count(ctx, "hit")
			return v, "cache"
		case errors.Is(err, redis.Nil):
			c.count(ctx, "miss")
		default:
			c.count(ctx, "error") // fail-open: segue para a fonte
		}
	}

	q := c.fromSource(ctx, id)

	if c.rdb != nil {
		wctx, cancel := context.WithTimeout(ctx, c.timeout)
		_ = c.rdb.Set(wctx, key, q, c.ttl).Err() // erro ao gravar não afeta a resposta
		cancel()
	}
	return q, "source"
}

// fromSource simula a fonte de verdade lenta (um banco ou uma API).
func (c *quoteCache) fromSource(ctx context.Context, id int) string {
	_, span := otel.Tracer(instrumentation).Start(ctx, "quoteSource.load")
	defer span.End()
	select {
	case <-time.After(c.sourceMs):
	case <-ctx.Done():
	}
	return quotes[id%len(quotes)]
}

func (c *quoteCache) count(ctx context.Context, result string) {
	// "result" tem 3 valores possíveis: cardinalidade controlada.
	c.lookups.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}

func (c *quoteCache) close() {
	if c.rdb != nil {
		_ = c.rdb.Close()
	}
}
