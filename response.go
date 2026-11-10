package rask

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// NoContent is the Data of an operation that answers 204.
type NoContent struct{}

// Meta is what every response carries besides its typed data: the raw status, headers and body, and the idempotency
// key the call used. Nothing here takes a second request.
type Meta struct {
	StatusCode int
	Header     http.Header
	// Body is the response body as received. Anything the typed Data has no field for is here: unknown fields of this
	// SDK version, for instance. See Lookup.
	Body []byte
	// IdempotencyKey is the key the call sent, or "" for operations that take none. Reusing it replays the call.
	IdempotencyKey string
}

// Response is the result of one operation.
type Response[T any] struct {
	// Data is the decoded body. It is the zero value on 304 and 204. Enums keep values this SDK does not know and
	// unions keep unknown variants as raw JSON; fields this SDK does not know are only in Body.
	Data T
	Meta
}

// ETag returns the ETag header: pass it as if_match on the next write.
func (m *Meta) ETag() string { return m.Header.Get("ETag") }

// Location returns the Location header. It is path-absolute (/v3/...) whatever the base URL: use the id from Data
// rather than following it.
func (m *Meta) Location() string { return m.Header.Get("Location") }

// RequestID returns the X-Request-Id header, the id to quote to support.
func (m *Meta) RequestID() string { return m.Header.Get("X-Request-Id") }

// IdempotentReplayed reports whether the server replayed a stored response for this key.
func (m *Meta) IdempotentReplayed() bool { return m.Header.Get("Idempotent-Replayed") == "true" }

// RetryAfter returns the Retry-After header, and whether there was a usable one.
func (m *Meta) RetryAfter() (time.Duration, bool) { return parseRetryAfter(m.Header, time.Now()) }

// NotModified reports a 304, the answer to a matching If-None-Match. Data is empty then.
func (m *Meta) NotModified() bool { return m.StatusCode == http.StatusNotModified }

// Lookup reads an RFC 6901 JSON Pointer from the raw body, so a value this SDK has no type for can still be read:
// resp.Lookup("/status"), resp.Lookup("/params/type"). Numbers come back as json.Number.
func (m *Meta) Lookup(pointer string) (any, bool) { return lookupPointer(m.Body, pointer) }

func lookupPointer(raw []byte, pointer string) (any, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var node any
	if err := decoder.Decode(&node); err != nil {
		return nil, false
	}
	if pointer == "" {
		return node, true
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, false
	}
	unescape := strings.NewReplacer("~1", "/", "~0", "~")
	for _, part := range strings.Split(pointer[1:], "/") {
		part = unescape.Replace(part)
		switch current := node.(type) {
		case map[string]any:
			next, ok := current[part]
			if !ok {
				return nil, false
			}
			node = next
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(current) {
				return nil, false
			}
			node = current[index]
		default:
			return nil, false
		}
	}
	return node, true
}

// parseRetryAfter reads Retry-After as seconds or as an HTTP date.
func parseRetryAfter(header http.Header, now time.Time) (time.Duration, bool) {
	value := strings.TrimSpace(header.Get("Retry-After"))
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds * float64(time.Second)), true
	}
	if at, err := http.ParseTime(value); err == nil {
		if wait := at.Sub(now); wait > 0 {
			return wait, true
		}
		return 0, true
	}
	return 0, false
}
