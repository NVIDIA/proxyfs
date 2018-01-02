all: fmt install test vet

.PHONY: all bench clean cover fmt install test vet

bench:
	go test -bench .

clean:
	go clean -i .

cover:
	go test -cover .

fmt:
	go fmt .

install:
	go install .

test:
	go test .

vet:
	go vet .
