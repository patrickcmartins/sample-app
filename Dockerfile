# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/sample-app .

# distroless: sem shell, sem package manager, usuário não-root (65532).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/sample-app /sample-app
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/sample-app"]
