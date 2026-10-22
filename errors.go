package rask

import (
	"encoding/json"
	"fmt"
	"strings"
)

// APIError is returned when the API answers with a non-success status.
type APIError struct {
	// StatusCode is the HTTP status code.
	StatusCode int
	// Status is the machine-readable error status when the API provides one,
	// for example "not_found" or "unacceptable_upload_mime_type".
	Status string
	// Detail is the human-readable error description.
	Detail string
	// Body is the raw response body.
	Body []byte
}

func (e *APIError) Error() string {
	msg := fmt.Sprintf("rask: API error %d", e.StatusCode)
	if e.Status != "" {
		msg += " " + e.Status
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

// AuthError is returned when an access token cannot be obtained.
type AuthError struct{ Err error }

func (e *AuthError) Error() string { return "rask: authentication failed: " + e.Err.Error() }
func (e *AuthError) Unwrap() error { return e.Err }

func newAPIError(status int, body []byte) *APIError {
	e := &APIError{StatusCode: status, Body: body}
	var payload struct {
		Status string          `json:"status"`
		Detail json.RawMessage `json:"detail"`
	}
	if json.Unmarshal(body, &payload) != nil {
		e.Detail = strings.TrimSpace(string(body))
		return e
	}
	e.Status = payload.Status
	if e.Status == "" && len(payload.Detail) == 0 {
		// Not a Rask error body, e.g. {"message": "..."} from a gateway.
		e.Detail = strings.TrimSpace(string(body))
		return e
	}
	// detail is a string for most errors and a list of validation errors for 422.
	var detail string
	if json.Unmarshal(payload.Detail, &detail) == nil {
		e.Detail = detail
	} else if len(payload.Detail) > 0 {
		var items []struct {
			Loc []any  `json:"loc"`
			Msg string `json:"msg"`
		}
		if json.Unmarshal(payload.Detail, &items) == nil {
			parts := make([]string, 0, len(items))
			for _, it := range items {
				loc := make([]string, 0, len(it.Loc))
				for _, l := range it.Loc {
					loc = append(loc, fmt.Sprint(l))
				}
				parts = append(parts, strings.Join(loc, ".")+": "+it.Msg)
			}
			e.Detail = strings.Join(parts, "; ")
		} else {
			e.Detail = string(payload.Detail)
		}
	}
	return e
}

type statusResponse interface {
	GetBody() []byte
	StatusCode() int
}

func checkStatus(resp statusResponse, err error) error {
	if err != nil {
		return err
	}
	if code := resp.StatusCode(); code < 200 || code > 299 {
		return newAPIError(code, resp.GetBody())
	}
	return nil
}
