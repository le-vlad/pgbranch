ARG GO_VERSION=1.25
FROM golang:${GO_VERSION}-trixie AS build

WORKDIR /pgbrnach

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/pgbranch \
    ./cmd/pgbranch

FROM debian:trixie-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/pgbranch /usr/local/bin/pgbranch

WORKDIR /workspace
ENTRYPOINT ["/usr/local/bin/pgbranch"]
