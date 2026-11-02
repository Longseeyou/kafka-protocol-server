FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY app ./app
RUN go test ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /server ./app

FROM alpine:3.23
RUN addgroup -g 10001 server && adduser -D -u 10001 -G server server && mkdir /data && chown server:server /data
COPY --from=build /server /usr/local/bin/kafka-protocol-server
USER server
ENV LISTEN_ADDR=0.0.0.0:9092 DATA_DIR=/data TOPICS=demo:3
EXPOSE 9092
VOLUME ["/data"]
ENTRYPOINT ["kafka-protocol-server"]
