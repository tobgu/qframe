

generate:
	go generate dataframe/internal/template/*

test: generate
	go test github.com/tobgu/go-qcache/dataframe

fmt: generate
	go fmt ./...

vet: generate
	go vet ./...
