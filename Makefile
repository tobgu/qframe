generate:
	go install
	go generate github.com/tobgu/qframe/...

test: generate
	go test github.com/tobgu/qframe/

fmt: generate
	go fmt ./...

vet: generate
	go vet ./...

cov: generate
	go test github.com/tobgu/qframe/ -coverprofile=coverage.out
	go tool cover -html=coverage.out
