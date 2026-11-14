# sample-app

Um serviço HTTP em Go, só com stdlib, feito para demonstrar a plataforma.

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

## Pipeline

```
PR   ─► go vet + go test -race
main ─► build (distroless, nonroot) ─► push ghcr.io:sha-xxxxxxx (+SBOM, provenance)
     ─► trivy (falha em HIGH/CRITICAL) ─► cosign keyless (OIDC)
     ─► commit em sample-app-deploy/envs/kind-dev  ─► Argo CD sincroniza
```

Tags são **imutáveis** (`sha-<commit>`). Nunca usamos `latest`. Prod recebe a versão por PR de promoção no `sample-app-deploy`.

### Setup único

1. Crie um fine-grained PAT com **Contents: RW** só no `sample-app-deploy`, salve como secret `DEPLOY_REPO_TOKEN` e crie a variable `AUTO_DEPLOY_DEV=true`.
   Em empresa, isso seria um GitHub App.
2. Depois do primeiro push, deixe o pacote `ghcr.io/patrickcmartins/sample-app` como **public**,
   ou o kind não consegue puxar a imagem sem um `imagePullSecret`.
