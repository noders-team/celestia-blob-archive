.PHONY: build test opcompat proto

build:
	go build -o bin/ingester ./cmd/ingester
	go build -o bin/cba ./cmd/cba
	go build -o bin/api ./cmd/api

test:
	go test ./...

# op-alt-da's fallback provider against testdata/opcompat (separate module).
opcompat:
	cd tools/opcompat && go test ./...

proto:
	buf generate
