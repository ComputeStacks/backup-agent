FROM golang:1.21 AS build-stage

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /cs-agent

FROM debian:bookworm-slim AS build-release-stage

RUN set -ex; \
    \
    apt-get update; \
    apt-get install -y --no-install-recommends iptables nftables ca-certificates libssl3 openssl \
    ; \
    apt-get clean \
      && rm -rf /var/lib/apt/lists/*

WORKDIR /

COPY --from=build-stage /cs-agent /cs-agent

ENTRYPOINT ["/cs-agent"]
