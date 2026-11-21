# Build the example from the same checkout as this guide.
# Base images pinned to registry digests so rebuilds cannot pick up different images.
FROM golang:1.25@sha256:699337d620559a59b4a2bb298ad59611e535d2ee755a34cf2d2a98f37578dc80 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /quickstart-client ./docs/quickstart/client.go

# Reuse the released service image for its CA certificates and supported platforms.
FROM docker.io/milvusdb/woodpecker:v0.1.46@sha256:008b303879a6c7c71167c00c0743bf9f469ba5afdf5a51672316840e1e376b13
COPY --from=build /quickstart-client /usr/local/bin/quickstart-client
ENTRYPOINT ["/usr/local/bin/quickstart-client"]
CMD ["--config", "/examples/compose-client.yaml"]
