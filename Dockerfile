FROM golang:1.25 AS builder

WORKDIR /src

# Copy go mod files first to leverage Docker layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Use buildx cache mounts to drastically speed up repeated Go builds
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    make

FROM debian:bookworm-slim
# Install ca-certificates in case telegraf makes HTTPS requests
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/telegraf /usr/bin/telegraf
ENTRYPOINT ["telegraf"]
