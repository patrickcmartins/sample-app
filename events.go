package main

// Kafka: o frontend publica um evento por cotação servida; o worker consome e agrega no Redis.
//
// Por que um evento e não uma chamada HTTP ao worker?
//   - Desacoplamento: o frontend não sabe quem consome, nem quantos consumidores existem.
//   - Absorve picos: se o worker estiver lento ou fora, as mensagens esperam no tópico (lag),
//     nada se perde e o frontend não fica lento.
//
// Garantias e decisões:
//   - Produção ASSÍNCRONA: o request do usuário nunca espera o Kafka. Se o Kafka cair, o evento
//     se perde (contado na métrica) e o request continua 200. Para pedido/pagamento isso seria
//     inaceitável; aí entraria o padrão outbox (grava no banco e um relay publica).
//   - Chave = quote_id: mesma chave cai na mesma partição, então a ordem por cotação é preservada.
//   - Consumo "at least once": o offset só é confirmado DEPOIS de gravar no Redis. Se o worker
//     morrer no meio, a mensagem é reprocessada. Por isso o processamento precisa ser idempotente
//     ou tolerar duplicata (aqui um contador, duplicata = +1 a mais, aceitável).
//   - O contexto do trace vai nos headers da mensagem (kotel): frontend -> Kafka -> worker
//     aparece como um trace só no Tempo.

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type quoteEvent struct {
	QuoteID int       `json:"quote_id"`
	Version string    `json:"version"`
	At      time.Time `json:"at"`
}

func kafkaTracing() *kotel.Kotel {
	return kotel.NewKotel(kotel.WithTracer(kotel.NewTracer(
		kotel.TracerProvider(otel.GetTracerProvider()),
		kotel.TracerPropagator(otel.GetTextMapPropagator()),
	)))
}

// ---------------------------------------------------------------- producer (frontend)

type publisher struct {
	cl     *kgo.Client // nil = Kafka desligado
	topic  string
	log    *slog.Logger
	result metric.Int64Counter
}

func newPublisher(brokers, topic string, log *slog.Logger) (*publisher, error) {
	result, _ := otel.Meter(instrumentation).Int64Counter("events_published",
		metric.WithDescription("Eventos publicados no Kafka por resultado: ok, error"))
	p := &publisher{topic: topic, log: log, result: result}
	if brokers == "" {
		return p, nil
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.DefaultProduceTopic(topic),
		kgo.WithHooks(kafkaTracing().Hooks()...),
		kgo.RequiredAcks(kgo.AllISRAcks()), // durável: espera as réplicas em sincronia
		kgo.ProducerLinger(20*time.Millisecond),
		kgo.RecordDeliveryTimeout(5*time.Second),
		kgo.MaxBufferedRecords(10_000), // backpressure: buffer cheio = descarta, não trava o request
	)
	if err != nil {
		return nil, err
	}
	p.cl = cl
	return p, nil
}

func (p *publisher) publish(ctx context.Context, e quoteEvent) {
	if p.cl == nil {
		return
	}
	value, _ := json.Marshal(e)
	rec := &kgo.Record{Key: []byte(itoa(e.QuoteID)), Value: value}
	// O ctx do request é cancelado assim que o handler responde, e o envio é assíncrono:
	// com ele, o franz-go abortaria o record ("context canceled"). WithoutCancel mantém o
	// trace (o span do producer continua filho do request) mas desliga o cancelamento.
	ctx = context.WithoutCancel(ctx)
	// Assíncrono: o callback roda quando o broker confirma (ou falha). O request não espera.
	p.cl.TryProduce(ctx, rec, func(_ *kgo.Record, err error) {
		status := "ok"
		if err != nil {
			status = "error"
			p.log.WarnContext(ctx, "falha ao publicar evento", "err", err)
		}
		p.result.Add(context.Background(), 1, metric.WithAttributes(attribute.String("result", status)))
	})
}

func (p *publisher) close() {
	if p.cl != nil {
		p.cl.Flush(context.Background())
		p.cl.Close()
	}
}

// ---------------------------------------------------------------- consumer (worker)

const statsKey = "stats:quotes"

type worker struct {
	cl    *kgo.Client
	rdb   *redis.Client
	log   *slog.Logger
	delay time.Duration // caos: worker lento = lag crescendo no consumer group
}

func newWorker(brokers, topic, group, redisAddr string, delay time.Duration, log *slog.Logger) (*worker, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.WithHooks(kafkaTracing().Hooks()...),
		kgo.DisableAutoCommit(), // commit manual, depois de processar = at least once
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, err
	}
	return &worker{cl: cl, rdb: redis.NewClient(&redis.Options{Addr: redisAddr}), log: log, delay: delay}, nil
}

func (w *worker) run(ctx context.Context) {
	tracer := otel.Tracer(instrumentation)
	for {
		fetches := w.cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			w.log.Error("erro de fetch", "topic", t, "partition", p, "err", err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			// r.Context carrega o span do kotel, filho do span do producer (headers da mensagem).
			rctx, span := tracer.Start(r.Context, "worker.process")
			defer span.End()
			if w.delay > 0 {
				time.Sleep(w.delay)
			}
			var e quoteEvent
			if err := json.Unmarshal(r.Value, &e); err != nil {
				// Mensagem inválida: loga e segue (em produção: dead letter topic).
				w.log.ErrorContext(rctx, "evento inválido", "offset", r.Offset, "err", err)
				return
			}
			// Redis fora: tenta de novo com backoff em vez de pular a mensagem. O offset não é
			// confirmado enquanto isso, então nada se perde. O efeito visível é o LAG do consumer
			// group crescendo e depois zerando quando o Redis volta.
			backoff := 200 * time.Millisecond
			for {
				err := w.rdb.HIncrBy(rctx, statsKey, itoa(e.QuoteID), 1).Err()
				if err == nil || ctx.Err() != nil {
					return
				}
				w.log.ErrorContext(rctx, "redis indisponível, tentando de novo", "err", err, "backoff", backoff.String())
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return
				}
				backoff = min(backoff*2, 10*time.Second)
			}
		})
		// Confirma os offsets do lote só depois de processar.
		if err := w.cl.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			w.log.Error("commit de offset", "err", err)
		}
	}
}

func (w *worker) stats(ctx context.Context) (map[string]string, error) {
	return w.rdb.HGetAll(ctx, statsKey).Result()
}

func (w *worker) close() {
	w.cl.Close()
	_ = w.rdb.Close()
}
