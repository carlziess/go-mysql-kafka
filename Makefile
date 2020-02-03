all: build

build: build-go-mysql-kafka

build-go-mysql-kafka:
	GO111MODULE=on go build -o bin/go-mysql-kafka ./cmd/go-mysql-kafka

test:
	GO111MODULE=on go test -timeout 1m --race ./...

clean:
	GO111MODULE=on go clean -i ./...
	@rm -rf bin
