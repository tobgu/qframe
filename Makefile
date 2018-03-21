generate:
	# Build and install generator binary first
	go install github.com/tobgu/qframe/cmd/qfgenerate
	go generate github.com/tobgu/qframe/...

test: generate
	go test github.com/tobgu/qframe/

fmt: generate
	go fmt ./...

vet: generate
	go vet ./...

cov: generate
	go test github.com/tobgu/qframe/... -coverprofile=coverage.out -coverpkg=all
	go tool cover -html=coverage.out
