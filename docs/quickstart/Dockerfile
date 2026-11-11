# Build the example from the same checkout as this guide.
FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /quickstart-client ./docs/quickstart/client.go

# Reuse the released service image for its CA certificates and supported platforms.
FROM docker.io/milvusdb/woodpecker:v0.1.46
COPY --from=build /quickstart-client /usr/local/bin/quickstart-client
ENTRYPOINT ["/usr/local/bin/quickstart-client"]
CMD ["--config", "/examples/compose-client.yaml"]
