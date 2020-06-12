# Build stage
FROM golang:1.12.4 AS builder

# install libssl
RUN curl -L http://security.debian.org/debian-security/pool/updates/main/o/openssl/libssl1.0.0_1.0.1t-1+deb8u12_amd64.deb -o libssl.deb \
 && dpkg -i libssl.deb

# install rdkafka
RUN curl -L https://packages.confluent.io/deb/5.2/archive.key | apt-key add - \
 && echo "deb [arch=amd64] http://packages.confluent.io/deb/5.2 stable main" >> /etc/apt/sources.list \
 && apt-get update \
 && apt-get install -y \
    librdkafka1 \
    librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

# Enable support of go modules by default
ENV GO111MODULE on
ENV BASE_DIR /go/src/kafka-ambassador

# Install gosec
RUN wget -O - -q https://raw.githubusercontent.com/securego/gosec/master/install.sh | sh -s -- -b /usr/bin v2.3.0

# Warming modules cache with project dependencies
WORKDIR ${BASE_DIR}
COPY go.mod go.sum ./
RUN go mod download

# Copy project source code to WORKDIR
COPY . .

# Run gosec
RUN gosec ./...

# Run tests and build on success
RUN go test ./... \
 && go build -o /go/bin/kaffka-ambassador

# Final container stage
FROM ubuntu:16.04

# install rdkafka
RUN apt-get update \
 && apt-get install -y \
    curl \
    software-properties-common \
    python-software-properties \
 && curl -L https://packages.confluent.io/deb/5.2/archive.key | apt-key add - \
 && add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/5.2 stable main" \
 && apt-get install -y \
    apt-transport-https \
 && apt-get update \
 && apt install -y \
    librdkafka1 \
    librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /go/bin/kaffka-ambassador /bin/kafka-ambassador
