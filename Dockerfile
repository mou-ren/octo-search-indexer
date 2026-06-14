# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder
WORKDIR /app

# Cache module downloads as a separate layer so code edits don't bust the dep cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /es-indexer ./cmd/es-indexer

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata wget \
  && adduser -D -u 10001 appuser

COPY --from=builder /es-indexer /usr/local/bin/es-indexer

USER appuser

# es-indexer is a Kafka-consumer worker. Phase 4 adds a self-hosted /metrics
# scrape endpoint (reuse octo pkg/metrics NewScrapeServer); EXPOSE/HEALTHCHECK
# will be wired to that port then. Kept minimal in the scaffold.
EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/es-indexer"]
