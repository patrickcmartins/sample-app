package main

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestCacheAside(t *testing.T) {
	mr := miniredis.RunT(t)
	c, err := newQuoteCache(mr.Addr(), time.Minute, 100*time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	ctx := context.Background()

	if _, src := c.get(ctx, 1); src != "source" {
		t.Fatalf("primeira leitura deveria ser miss (source), veio %s", src)
	}
	if q, src := c.get(ctx, 1); src != "cache" || q != quotes[1] {
		t.Fatalf("segunda leitura deveria ser hit, veio %s %q", src, q)
	}
	if ttl := mr.TTL("quote:1"); ttl <= 0 {
		t.Fatalf("chave deveria ter TTL, veio %v", ttl)
	}
}

// Redis fora do ar: o backend continua respondendo pela fonte (fail-open) e rápido (timeout).
func TestCacheFailOpen(t *testing.T) {
	mr := miniredis.RunT(t)
	c, err := newQuoteCache(mr.Addr(), time.Minute, 50*time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	mr.Close()

	start := time.Now()
	q, src := c.get(context.Background(), 2)
	if src != "source" || q != quotes[2] {
		t.Fatalf("com Redis fora deveria ir à fonte, veio %s %q", src, q)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("fail-open demorou demais (%v): faltou timeout?", took)
	}
}
