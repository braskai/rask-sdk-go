package rask

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/braskai/rask-sdk-go/v3/api"
)

// RaskError is an API problem (RFC 9457). Every API error unwraps to it, so errors.As with a *RaskError catches them
// all, and errors.As with a per-status type such as *NotFoundError catches one kind. Branch on Code, never on text.
type RaskError struct {
	Status         int
	Code           string // Stable code; a code this SDK has never seen is kept as is. Empty when the body is not a problem.
	Type           string
	Title          string
	Detail         string
	Instance       string
	RequestID      string // X-Request-Id
	Retryable      bool
	Retry          string // same_key, new_key or none
	Recommendation string
	Hint           string
	Errors         []api.ProblemFieldError

	// Balance members, on insufficient_balance and token_spend_limit_exceeded.
	CreditType       string
	RequiredMinutes  *int32
	BalanceMinutes   *int32
	RequiredProjects *int32
	BalanceProjects  *int32

	CurrentETag string        // precondition_failed: the resource's current ETag.
	RetryAfter  time.Duration // Retry-After; zero when absent.

	// Problem is the whole problem, with every per-code member and any member this SDK does not know.
	Problem api.Problem

	Header         http.Header
	Body           []byte
	IdempotencyKey string // The key the call sent, or "".
	Operation      string // operationId
}

func (e *RaskError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "rask: %s: status %d", e.Operation, e.Status)
	if e.Code != "" {
		b.WriteString(" " + e.Code)
	}
	if e.Detail != "" {
		b.WriteString(": " + e.Detail)
	}
	if e.RequestID != "" {
		b.WriteString(" (request " + e.RequestID + ")")
	}
	return b.String()
}

// Lookup reads an RFC 6901 JSON Pointer from the raw error body.
func (e *RaskError) Lookup(pointer string) (any, bool) { return lookupPointer(e.Body, pointer) }

// Per-status errors. Each embeds *RaskError, so its fields read directly: err.Code, err.RetryAfter.
type (
	// BadRequestError is a 400.
	BadRequestError struct{ *RaskError }
	// AuthenticationError is a 401.
	AuthenticationError struct{ *RaskError }
	// InsufficientBalanceError is a 402: see CreditType, RequiredMinutes and BalanceMinutes.
	InsufficientBalanceError struct{ *RaskError }
	// PermissionDeniedError is a 403.
	PermissionDeniedError struct{ *RaskError }
	// NotFoundError is a 404.
	NotFoundError struct{ *RaskError }
	// ConflictError is a 409.
	ConflictError struct{ *RaskError }
	// GoneError is a 410.
	GoneError struct{ *RaskError }
	// PreconditionFailedError is a 412: the if_match is stale, and CurrentETag holds the current one.
	PreconditionFailedError struct{ *RaskError }
	// RequestTooLargeError is a 413.
	RequestTooLargeError struct{ *RaskError }
	// UnsupportedMediaTypeError is a 415.
	UnsupportedMediaTypeError struct{ *RaskError }
	// ValidationError is a 422: see Errors.
	ValidationError struct{ *RaskError }
	// PreconditionRequiredError is a 428.
	PreconditionRequiredError struct{ *RaskError }
	// RateLimitError is a 429: see RetryAfter.
	RateLimitError struct{ *RaskError }
	// ServerError is a 5xx.
	ServerError struct{ *RaskError }
)

func (e *BadRequestError) Unwrap() error           { return e.RaskError }
func (e *AuthenticationError) Unwrap() error       { return e.RaskError }
func (e *InsufficientBalanceError) Unwrap() error  { return e.RaskError }
func (e *PermissionDeniedError) Unwrap() error     { return e.RaskError }
func (e *NotFoundError) Unwrap() error             { return e.RaskError }
func (e *ConflictError) Unwrap() error             { return e.RaskError }
func (e *GoneError) Unwrap() error                 { return e.RaskError }
func (e *PreconditionFailedError) Unwrap() error   { return e.RaskError }
func (e *RequestTooLargeError) Unwrap() error      { return e.RaskError }
func (e *UnsupportedMediaTypeError) Unwrap() error { return e.RaskError }
func (e *ValidationError) Unwrap() error           { return e.RaskError }
func (e *PreconditionRequiredError) Unwrap() error { return e.RaskError }
func (e *RateLimitError) Unwrap() error            { return e.RaskError }
func (e *ServerError) Unwrap() error               { return e.RaskError }

// JobFailedError is what Jobs.Wait returns for a failed job. It carries the job's problem and the job. To start the
// work again, make a new call: the problem's Retry says new_key.
type JobFailedError struct {
	*RaskError
	JobID string
	Job   api.Job
}

func (e *JobFailedError) Error() string {
	return fmt.Sprintf("rask: job %s failed: %s: %s", e.JobID, e.Code, e.Detail)
}

// Unwrap returns the job's problem.
func (e *JobFailedError) Unwrap() error { return e.RaskError }

// JobCanceledError is what Jobs.Wait returns for a canceled job. The problem is empty unless the job has an error
// (a project deleted while the job ran).
type JobCanceledError struct {
	*RaskError
	JobID string
	Job   api.Job
}

func (e *JobCanceledError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("rask: job %s was canceled", e.JobID)
	}
	return fmt.Sprintf("rask: job %s was canceled: %s", e.JobID, e.Code)
}

// Unwrap returns the job's problem.
func (e *JobCanceledError) Unwrap() error { return e.RaskError }

// WaitTimeoutError is what Jobs.Wait returns when the job outlives the timeout. The job keeps running.
type WaitTimeoutError struct {
	JobID      string
	Timeout    time.Duration
	LastStatus string // The status last seen, or "" if no poll answered.
}

func (e *WaitTimeoutError) Error() string {
	return fmt.Sprintf("rask: job %s did not finish in %s (last status %q); it keeps running", e.JobID, e.Timeout, e.LastStatus)
}

// ConnectionError is a request that got no usable answer: the connection failed or the attempt timed out, on every
// attempt the retry budget allowed. Unwrap gives the last cause, so errors.Is(err, context.Canceled) works.
type ConnectionError struct {
	Operation      string
	Attempts       int
	IdempotencyKey string // The key the call sent, or "". Retrying later with it cannot start the work twice.
	Err            error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("rask: %s: no answer after %d attempt(s): %v", e.Operation, e.Attempts, e.Err)
}

// Unwrap returns the last cause.
func (e *ConnectionError) Unwrap() error { return e.Err }

// DecodeError is a successful response whose body does not fit the type this SDK has for it, which only a contract
// change explains (unknown values and fields never cause it). The Response is returned beside it, with its raw Body
// and Lookup, because the operation did take effect: do not retry on this error.
type DecodeError struct {
	Operation string
	Err       error
}

func (e *DecodeError) Error() string {
	return fmt.Sprintf("rask: %s: the response does not fit its type: %v", e.Operation, e.Err)
}

// Unwrap returns the JSON error.
func (e *DecodeError) Unwrap() error { return e.Err }

// UploadError is a presigned upload that storage did not accept after the allowed attempts. The error never contains
// the presigned URL.
type UploadError struct {
	UploadID string
	Attempts int
	Status   int    // Storage's last HTTP status, or 0 if there was no answer.
	Err      error  // The last transport error, or nil.
	Detail   string // Start of storage's last answer.
}

func (e *UploadError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("rask: upload %s: storage did not accept the file after %d attempt(s): %v", e.UploadID, e.Attempts, e.Err)
	}
	return fmt.Sprintf("rask: upload %s: storage did not accept the file after %d attempt(s): status %d", e.UploadID, e.Attempts, e.Status)
}

// Unwrap returns the last transport error.
func (e *UploadError) Unwrap() error { return e.Err }

// newAPIError builds the typed error for a final non-success response.
func newAPIError(operationID string, status int, header http.Header, body []byte, key string, now time.Time) error {
	base := baseError(operationID, status, header, body, key, now)
	switch {
	case status == http.StatusBadRequest:
		return &BadRequestError{base}
	case status == http.StatusUnauthorized:
		return &AuthenticationError{base}
	case status == http.StatusPaymentRequired:
		return &InsufficientBalanceError{base}
	case status == http.StatusForbidden:
		return &PermissionDeniedError{base}
	case status == http.StatusNotFound:
		return &NotFoundError{base}
	case status == http.StatusConflict:
		return &ConflictError{base}
	case status == http.StatusGone:
		return &GoneError{base}
	case status == http.StatusPreconditionFailed:
		return &PreconditionFailedError{base}
	case status == http.StatusRequestEntityTooLarge:
		return &RequestTooLargeError{base}
	case status == http.StatusUnsupportedMediaType:
		return &UnsupportedMediaTypeError{base}
	case status == http.StatusUnprocessableEntity:
		return &ValidationError{base}
	case status == http.StatusPreconditionRequired:
		return &PreconditionRequiredError{base}
	case status == http.StatusTooManyRequests:
		return &RateLimitError{base}
	case status >= 500:
		return &ServerError{base}
	}
	return base
}

// baseError fills a RaskError from a response. A body that is no problem (a gateway's HTML, say) leaves the problem
// members empty and never fails: the status alone still types the error.
func baseError(operationID string, status int, header http.Header, body []byte, key string, now time.Time) *RaskError {
	e := &RaskError{
		Status:         status,
		Operation:      operationID,
		Header:         header,
		Body:           body,
		IdempotencyKey: key,
		RequestID:      header.Get("X-Request-Id"),
		Retryable:      isRetryableStatus(status),
	}
	if wait, ok := parseRetryAfter(header, now); ok {
		e.RetryAfter = wait
	}
	var problem api.Problem
	if err := json.Unmarshal(body, &problem); err == nil {
		e.applyProblem(problem)
	} else if code, ok := lookupPointer(body, "/code"); ok {
		// A member of an unexpected type broke the typed decode; keep what can be read.
		if s, isString := code.(string); isString {
			e.Code = s
		}
	}
	return e
}

func (e *RaskError) applyProblem(p api.Problem) {
	e.Problem = p
	if p.Code == "" && p.Title == "" {
		return // Valid JSON, but no problem.
	}
	e.Code = p.Code
	e.Type = p.Type
	e.Title = p.Title
	e.Detail = p.Detail
	e.Instance = p.Instance
	e.Retryable = p.Retryable
	e.Retry = string(p.Retry)
	e.Recommendation = string(p.Recommendation)
	if hint, err := p.Hint.Get(); err == nil {
		e.Hint = hint
	}
	e.Errors = p.Errors
	if p.CreditType != nil {
		e.CreditType = string(*p.CreditType)
	}
	e.RequiredMinutes = p.RequiredMinutes
	e.BalanceMinutes = p.BalanceMinutes
	e.RequiredProjects = p.RequiredProjects
	e.BalanceProjects = p.BalanceProjects
	if p.CurrentEtag != nil {
		e.CurrentETag = *p.CurrentEtag
	}
}

func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// isTransient reports whether an error from a finished call may pass on its own, so a long wait should outlast it.
func isTransient(err error) bool {
	var connection *ConnectionError
	if errors.As(err, &connection) {
		return true
	}
	var problem *RaskError
	return errors.As(err, &problem) && isRetryableStatus(problem.Status)
}

// argError names a required argument that is missing or unusable.
func argError(operationID, name, why string) error {
	return fmt.Errorf("rask: %s: %s %s", operationID, name, why)
}
