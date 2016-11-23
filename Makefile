PACKAGES="./qcache"

test:
	go test $(PACKAGES)

fmt:
	go fmt $(PACKAGES)

vet:
	go vet $(PACKAGES)
