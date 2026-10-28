# Build the real client for the selected node architecture before docker build:
# CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o .local/source-client ./tests/integration/diagnostics/sourceclient
# docker build -f tests/integration/diagnostics/sourceclient/Dockerfile -t wp-source-client-491:local .local
FROM scratch
COPY source-client /source-client
ENTRYPOINT ["/source-client"]
