# ---------- build stage ----------
FROM golang:1.22-alpine AS build
RUN apk add --no-cache git bash curl
WORKDIR /src
# Salin go.mod dulu untuk download dependencies
COPY go.mod ./
# Download dependencies (ini akan populate module cache)
RUN go mod download
# Salin source code (folder bermasalah sudah di-exclude oleh .dockerignore)
COPY . .
# Generate go.sum dengan go mod tidy setelah semua source code di-copy
# go.sum diperlukan oleh go build untuk verifikasi checksum
RUN go mod tidy

# Build binaries
RUN CGO_ENABLED=0 go build -o /out/ingestor ./cmd/ingestor
RUN CGO_ENABLED=0 go build -o /out/generator ./cmd/generator
RUN CGO_ENABLED=0 go build -o /out/hotkey-manager ./cmd/hotkey-manager
RUN CGO_ENABLED=0 go build -o /out/offloader ./cmd/offloader
RUN CGO_ENABLED=0 go build -o /out/joiner ./cmd/joiner
RUN CGO_ENABLED=0 go build -o /out/materializer ./cmd/materializer
RUN CGO_ENABLED=0 go build -o /out/dashboard ./cmd/dashboard

# Hadoop client: salin tree dari image yang sama dengan stack HDFS (bde2020), tanpa wget ke mirror (sering 404/diblokir).
# Catatan: image base biasanya linux/amd64; di Apple Silicon Docker biasanya meng-emulasi otomatis.
FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8 AS hadoop-vendor

FROM alpine:3.20 AS hadoop-client
RUN apk add --no-cache bash curl openjdk11-jre
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
COPY --from=hadoop-vendor /opt/hadoop-3.2.1 /opt/hadoop-3.2.1
RUN ln -sf /opt/hadoop-3.2.1 /opt/hadoop

# ---------- runtime ingestor ----------
FROM alpine:3.20 AS ingestor
RUN apk add --no-cache bash curl openjdk11-jre
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
COPY --from=hadoop-client /opt/hadoop-3.2.1 /opt/hadoop
RUN mkdir -p /opt/hadoop/etc/hadoop && printf '%s\n' \
  '<?xml version="1.0"?><configuration>' \
  '<property><name>fs.defaultFS</name><value>hdfs://namenode:9000</value></property>' \
  '</configuration>' > /opt/hadoop/etc/hadoop/core-site.xml
ENV HADOOP_HOME=/opt/hadoop
ENV PATH="${HADOOP_HOME}/bin:${PATH}"
COPY --from=build /out/ingestor /app/ingestor
EXPOSE 8080
ENTRYPOINT ["/app/ingestor"]

# ---------- runtime generator ----------
FROM alpine:3.20 AS generator
RUN apk add --no-cache bash curl
COPY --from=build /out/generator /app/generator
ENTRYPOINT ["/app/generator"]

# ---------- runtime hotkey-manager ----------
FROM alpine:3.20 AS hotkey-manager
RUN apk add --no-cache bash curl
COPY --from=build /out/hotkey-manager /app/hotkey-manager
ENTRYPOINT ["/app/hotkey-manager"]

# ---------- runtime offloader (Redis -> HDFS) ----------
FROM alpine:3.20 AS offloader
RUN apk add --no-cache bash curl openjdk11-jre
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
COPY --from=hadoop-client /opt/hadoop-3.2.1 /opt/hadoop
# Client HDFS perlu tahu alamat namenode (resolve host "namenode" di Docker network)
RUN mkdir -p /opt/hadoop/etc/hadoop && printf '%s\n' \
  '<?xml version="1.0"?><configuration>' \
  '<property><name>fs.defaultFS</name><value>hdfs://namenode:9000</value></property>' \
  '</configuration>' > /opt/hadoop/etc/hadoop/core-site.xml
ENV HADOOP_HOME=/opt/hadoop
ENV PATH="${HADOOP_HOME}/bin:${PATH}"
COPY --from=build /out/offloader /app/offloader
ENTRYPOINT ["/app/offloader"]

# ---------- runtime joiner (stream join + Redis list) ----------
FROM alpine:3.20 AS joiner
RUN apk add --no-cache bash curl
COPY --from=build /out/joiner /app/joiner
EXPOSE 8080
ENTRYPOINT ["/app/joiner"]

# ---------- runtime dashboard (live pipeline UI) ----------
FROM alpine:3.20 AS dashboard
RUN apk add --no-cache bash curl
COPY --from=build /out/dashboard /app/dashboard
EXPOSE 8080
ENTRYPOINT ["/app/dashboard"]

# ---------- runtime materializer (batch drain list -> HDFS JSONL) ----------
FROM alpine:3.20 AS materializer
RUN apk add --no-cache bash curl openjdk11-jre
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
COPY --from=hadoop-client /opt/hadoop-3.2.1 /opt/hadoop
RUN mkdir -p /opt/hadoop/etc/hadoop && printf '%s\n' \
  '<?xml version="1.0"?><configuration>' \
  '<property><name>fs.defaultFS</name><value>hdfs://namenode:9000</value></property>' \
  '</configuration>' > /opt/hadoop/etc/hadoop/core-site.xml
ENV HADOOP_HOME=/opt/hadoop
ENV PATH="${HADOOP_HOME}/bin:${PATH}"
COPY --from=build /out/materializer /app/materializer
ENTRYPOINT ["/app/materializer"]
