.PHONY: help generate check-generated build lint test acceptance acceptance-slow smoke

GOLANGCI_LINT ?= golangci-lint

help:
	@echo "generate         regenerate api/api.gen.go, resources.gen.go and the acceptance dispatch table"
	@echo "check-generated  fail if generated code is out of date"
	@echo "build            compile everything, examples included"
	@echo "lint             go vet, gofmt and golangci-lint"
	@echo "test             all tests with the race detector; the acceptance suite skips if the harness is missing"
	@echo "acceptance       the acceptance suite; fails if the harness is missing (RASK_SDK_ACCEPTANCE_BIN)"
	@echo "acceptance-slow  the same with the two slow scenarios, about 3 minutes more"
	@echo "smoke            examples/smoke; needs RASK_API_KEY and RASK_BASE_URL"

generate:
	go generate ./...

check-generated: generate
	git diff --exit-code -- api/api.gen.go resources.gen.go acceptance_dispatch_gen_test.go

build:
	go build ./...

lint:
	go vet ./...
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
	$(GOLANGCI_LINT) run ./...

test:
	go test -race ./...

acceptance:
	RASK_SDK_ACCEPTANCE_REQUIRED=1 go test -count=1 -run 'TestAcceptance|TestEveryOperationIsCallable|TestSmokeProgram' .

acceptance-slow:
	RUN_SLOW=1 RASK_SDK_ACCEPTANCE_REQUIRED=1 go test -count=1 -timeout 15m -run 'TestAcceptance' .

smoke:
	go run ./examples/smoke
