package rask

import openapi_types "github.com/oapi-codegen/runtime/types"

// UUID is the identifier type used by the API. It is github.com/google/uuid's
// UUID, so values from that package can be passed directly.
type UUID = openapi_types.UUID

// Ptr returns a pointer to v, for setting optional fields:
//
//	rask.ProjectCreate{VideoId: id, DstLang: "es", Name: rask.Ptr("My project")}
func Ptr[T any](v T) *T { return &v }
