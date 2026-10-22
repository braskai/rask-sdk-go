.PHONY: help generate check-generated test lint

help:
	@echo "Use \`make <target>' where <target> is one of:"
	@echo "  generate         regenerate api/api.gen.go and the typed wrappers from api/openapi.yml"
	@echo "  check-generated  fail if generated code is out of date"
	@echo "  test             run tests with the race detector"
	@echo "  lint             run go vet and check formatting"

generate:
	go generate ./...

check-generated: generate
	git diff --exit-code -- api/api.gen.go operations.gen.go types.gen.go

test:
	go test -race ./...

lint:
	go vet ./...
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
