FROM golang:1.22-bookworm AS builder

WORKDIR /src
COPY go.mod *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /muse2api .

FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive \
    MUSE2API_HOME=/app \
    MUSE2API_HOST=0.0.0.0 \
    MUSE2API_PORT=18610 \
    MUSE2API_CHROMIUM=/usr/bin/chromium \
    MUSE2API_CDP_PORT=19210

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    fonts-wqy-zenhei \
    fonts-freefont-ttf \
    procps \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /muse2api /app/muse2api
COPY admin.html version.json ./
COPY deploy/ ./deploy/
COPY extension/ ./extension/
COPY tools/ ./tools/

RUN mkdir -p /app/data/media /app/data/downloads /root/muse2api-profiles

VOLUME ["/app/data"]

EXPOSE 18610

CMD ["/app/muse2api"]
