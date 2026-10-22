package rask

// Regenerate the low-level client from api/openapi.yml, then the typed wrappers.
//go:generate go generate ./api
//go:generate go run ./internal/cmd/genwrappers
