generate:
	# Build and install generator binary first
	go generate github.com/tobgu/qframe/... || true
	go install github.com/tobgu/qframe/cmd/qfgenerate
	go generate github.com/tobgu/qframe/...

test: generate
	go test ./...

lint:
	~/go/bin/golangci-lint run .

ci: test lint

fmt: generate
	go fmt ./...

vet: generate
	go vet ./...

cov: generate
	go test github.com/tobgu/qframe/... -coverprofile=coverage.out -coverpkg=all
	go tool cover -html=coverage.out

deps:
	go get -t ./...

dev-deps: deps
	go get github.com/mauricelam/genny
	mkdir -p ~/go/bin
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b ~/go/bin v1.20.0

qplot_examples:
	cd contrib/gonum/qplot/examples \
		&& go run temperatures.go
