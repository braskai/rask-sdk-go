package rask

// Pinned here so that consumers of the module do not inherit the generator's dependencies.
//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.8.2 -config api/oapi-codegen.yaml api/openapi.json
//go:generate go run ./internal/cmd/genresources -spec api/openapi.json -out resources.gen.go -test-out acceptance_dispatch_gen_test.go
