# sample-app

Um serviço HTTP em Go feito para demonstrar a plataforma. **Uma imagem, três papéis** (`ROLE`):

- `frontend` (default): responde em `/` e chama o backend (`BACKEND_URL`) para buscar uma cotação. Se o backend falhar, **degrada** (responde sem a cotação) em vez de devolver 500.
- `backend`: responde em `/quote`, com **cache-aside no Redis** (timeout de 100 ms; se o Redis cair, faz *fail-open* e vai à fonte lenta).
- `worker`: consome o tópico Kafka `quotes.served` (consumer group `quote-stats`) e agrega no Redis; `/stats` mostra o resultado.

```
frontend ──HTTP──▶ backend ──cache-aside──▶ Redis
   └─ evento quote.served ──▶ Kafka ──▶ worker ──HINCRBY──▶ Redis
```

**Decisões que valem explicar:**

| Decisão | Por quê |
|---|---|
| Publicação assíncrona, `context.WithoutCancel` | O usuário nunca espera o Kafka. Com o ctx do request, o envio era abortado quando o handler respondia (bug real, pego no teste local). |
| Chave = `quote_id` | Mesma chave, mesma partição: ordem garantida por cotação. |
| Commit manual depois de processar | *At least once*: se o worker morrer no meio, reprocessa. O processamento tolera duplicata. |
| Worker repete o Redis com backoff em vez de pular | Nada se perde: o efeito visível é o **lag** crescendo e zerando quando o Redis volta. |
| Sem auto-criação de tópico | Tópico é infraestrutura: criado por `KafkaTopic` (GitOps), com partições e retenção revisadas em PR. |
| Trace atravessa o Kafka | O `traceparent` vai nos headers da mensagem (kotel): frontend → Kafka → worker é um trace só. |

## Observabilidade (OpenTelemetry)

Traces e métricas saem por **OTLP/HTTP** para o Collector, configurados só pelas env vars padrão do OTel
(`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`). Sem endpoint, a telemetria fica desligada.

- `otelhttp` cria um span por request (nome = rota, nunca a URL crua) e propaga o `traceparent` (W3C) para o backend: frontend → backend aparecem no mesmo trace, junto com os spans dos sidecars do Istio.
- Logs em JSON com `trace_id`/`span_id`: do log no Loki dá para pular para o trace no Tempo.
- Probes (`/healthz`, `/readyz`) ficam fora do tracing.


| Endpoint | |
|---|---|
| `/` | JSON com versão, cor, mensagem, cluster e pod |
| `/version` | versão (injetada no build) |
| `/healthz` / `/readyz` | liveness / readiness (o readiness cai no SIGTERM, para graceful shutdown) |

**Botões de caos** (por env var, no `sample-app-deploy`):

| Env | Efeito | O que demonstra |
|---|---|---|
| `FAILURE_RATE=0.5` | 50% de HTTP 500 | os retries do Istio escondem a falha do cliente |
| `DELAY=10s` | latência artificial | o timeout do VirtualService corta em 5s (504) |
| `REDIS_TIMEOUT`, `SOURCE_LATENCY`, `CACHE_TTL` | timeout do Redis, latência da fonte, TTL do cache | o cache protegendo a fonte; o efeito de um Redis lento |
| `HIGH_CARDINALITY=true` | o backend põe `user_id` como label da métrica `quotes_served` | explosão de séries no Prometheus; a correção é no Collector |

## Pipeline

```
PR   ─► go vet + go test -race
main ─► build (distroless, nonroot) ─► push ghcr.io:sha-xxxxxxx (+SBOM, provenance)
     ─► trivy (falha em HIGH/CRITICAL) ─► cosign keyless (OIDC)
     ─► commit em sample-app-deploy/envs/eks-stg   ─► Argo CD sincroniza stg
     (prod: PR de promoção stg -> prod no sample-app-deploy, com review)
```

Tags são **imutáveis** (`sha-<commit>`). Nunca usamos `latest`. Prod recebe a versão por PR de promoção no `sample-app-deploy`.

### Setup único

1. Crie um fine-grained PAT com **Contents: RW** só no `sample-app-deploy`, salve como secret `DEPLOY_REPO_TOKEN` e crie a variable `AUTO_DEPLOY_DEV=true`.
   Em empresa, isso seria um GitHub App.
2. Depois do primeiro push, deixe o pacote `ghcr.io/patrickcmartins/sample-app` como **public**,
   ou o kind não consegue puxar a imagem sem um `imagePullSecret`.
