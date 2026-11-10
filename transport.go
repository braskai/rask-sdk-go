package rask

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	maxRedirects      = 5
	maxKeyInUseRetry  = 3
	backoffInitial    = 500 * time.Millisecond
	backoffFactor     = 1.5
	backoffMax        = 30 * time.Second
	backoffJitterFrom = 0.75 // A wait is backoff * [0.75, 1.25).
)

type ifMatchRule int

const (
	ifMatchNone ifMatchRule = iota
	ifMatchOptional
	ifMatchRequired
)

// operation is a contract operation's metadata; the variables are generated.
type operation struct {
	id         string
	method     string
	path       string
	carriesKey bool
	ifMatch    ifMatchRule
}

// RequestOption adjusts one call.
type RequestOption func(*callInfo)

// WithIdempotencyKey replaces the generated key of this call. Use it to resume work across process restarts.
func WithIdempotencyKey(key string) RequestOption {
	return func(c *callInfo) { c.key = &key }
}

// WithIfMatch sets If-Match on an operation where it is optional (UpdateTranslationSettings). Where it is required it
// is an argument of the method. The value goes out verbatim, quotes included: pass what ETag() or Data.Etag returned.
func WithIfMatch(etag string) RequestOption {
	return func(c *callInfo) { c.ifMatch = &etag }
}

// callInfo is one logical call. It travels in the context to doer.Do through the generated client.
type callInfo struct {
	op      *operation
	key     *string
	ifMatch *string
}

type callContextKey struct{}

// requiredArg is an argument that must not be empty.
type requiredArg struct{ name, value string }

// call runs one operation: it checks the arguments before any network, makes the idempotency key, and turns the
// final answer into a Response or a typed error.
func call[T any](ctx context.Context, c *Client, op *operation, opts []RequestOption, required []requiredArg, send func(context.Context, *callInfo) (*http.Response, error)) (*Response[T], error) {
	info := &callInfo{op: op}
	for _, opt := range opts {
		opt(info)
	}
	if err := info.validate(required); err != nil {
		return nil, err
	}
	if op.carriesKey && info.key == nil {
		key := uuid.NewString()
		info.key = &key
	}

	httpResponse, err := send(context.WithValue(ctx, callContextKey{}, info), info)
	if err != nil {
		return nil, err
	}
	defer func() { _ = httpResponse.Body.Close() }()
	body, err := io.ReadAll(httpResponse.Body) // In memory already: doer.Do read it inside the attempt.
	if err != nil {
		return nil, err
	}

	key := info.keyValue()
	if httpResponse.StatusCode != http.StatusNotModified && (httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300) {
		return nil, newAPIError(op.id, httpResponse.StatusCode, httpResponse.Header, body, key, c.now())
	}
	result := &Response[T]{Meta: Meta{
		StatusCode:     httpResponse.StatusCode,
		Header:         httpResponse.Header,
		Body:           body,
		IdempotencyKey: key,
	}}
	if len(body) == 0 || httpResponse.StatusCode == http.StatusNoContent || httpResponse.StatusCode == http.StatusNotModified {
		return result, nil
	}
	if _, isNone := any(result.Data).(NoContent); isNone {
		return result, nil
	}
	if err := json.Unmarshal(body, &result.Data); err != nil {
		return result, &DecodeError{Operation: op.id, Err: err}
	}
	return result, nil
}

func (info *callInfo) keyValue() string {
	if info.key == nil {
		return ""
	}
	return *info.key
}

func (info *callInfo) validate(required []requiredArg) error {
	for _, arg := range required {
		switch arg.value {
		case "":
			return argError(info.op.id, arg.name, "is required")
		case ".", "..":
			// A dot segment would resolve out of the path, or out of the base URL's prefix.
			return argError(info.op.id, arg.name, "must not be a dot segment")
		}
	}
	if info.key != nil && !info.op.carriesKey {
		return argError(info.op.id, "idempotency_key", "is not taken by this operation")
	}
	if info.key != nil && *info.key == "" {
		return argError(info.op.id, "idempotency_key", "must not be empty")
	}
	if info.ifMatch != nil && info.op.ifMatch != ifMatchOptional {
		return argError(info.op.id, "if_match", "is not an option of this operation")
	}
	return nil
}

// doer is the HttpRequestDoer of the generated client: one logical request, with its retries.
type doer struct{ c *Client }

// Do sends the request until an answer is final and returns it with the body in memory.
func (d doer) Do(req *http.Request) (*http.Response, error) {
	c := d.c
	ctx := req.Context()
	info, _ := ctx.Value(callContextKey{}).(*callInfo)
	if info == nil {
		return nil, errors.New("rask: request outside a call")
	}
	if !c.underBaseURL(req.URL) {
		return nil, fmt.Errorf("rask: %s: refusing a request outside the base URL", info.op.id)
	}

	started := c.now()
	keyInUse := 0
	for attempt := 0; ; attempt++ {
		response, body, err := c.attempt(req, attempt)
		retry := false
		var retryAfter time.Duration
		switch {
		case err != nil:
			if ctx.Err() != nil || !isRetryableTransportError(err) {
				return nil, &ConnectionError{Operation: info.op.id, Attempts: attempt + 1, IdempotencyKey: info.keyValue(), Err: err}
			}
			retry = true
		default:
			c.warnIfDeprecated(info.op.id, response.Header)
			retry = isRetryableStatus(response.StatusCode)
			if response.StatusCode == http.StatusConflict && problemCode(body) == "idempotency_key_in_use" {
				keyInUse++
				retry = keyInUse <= maxKeyInUseRetry
			}
			retryAfter, _ = parseRetryAfter(response.Header, c.now())
		}

		wait := max(c.backoff(attempt), retryAfter)
		retry = retry && (c.maxRetries < 0 || attempt < c.maxRetries)
		retry = retry && c.now().Sub(started)+wait <= retryBudget
		if !retry {
			if err != nil {
				return nil, &ConnectionError{Operation: info.op.id, Attempts: attempt + 1, IdempotencyKey: info.keyValue(), Err: err}
			}
			response.Body = io.NopCloser(bytes.NewReader(body))
			return response, nil
		}
		c.logRetry(ctx, info.op.id, attempt, wait)
		if err := c.sleep(ctx, wait); err != nil {
			return nil, &ConnectionError{Operation: info.op.id, Attempts: attempt + 1, IdempotencyKey: info.keyValue(), Err: err}
		}
	}
}

// attempt makes one HTTP exchange, bounded by the per-attempt timeout, and reads the whole body within it, so that a
// connection lost in mid-body counts as a failed attempt.
func (c *Client) attempt(req *http.Request, attempt int) (*http.Response, []byte, error) {
	ctx := req.Context()
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	send := req.Clone(ctx)
	if req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, nil, err
		}
		send.Body = body
	}
	c.setHeaders(send)

	started := c.now()
	c.logRequest(ctx, send, attempt)
	response, err := c.http.Do(send) //nolint:gosec // The caller configured this URL; requesting it is the SDK's job.
	if err != nil {
		c.logFailure(ctx, send, err)
		return nil, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		c.logFailure(ctx, send, err)
		return nil, nil, err
	}
	c.logResponse(ctx, send, response, c.now().Sub(started))
	return response, body, nil
}

func (c *Client) setHeaders(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.apiKey())
	req.Header.Set("Accept", "application/json, application/problem+json")
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("X-Rask-Client", c.clientID)
	if req.Body == nil || req.Body == http.NoBody {
		req.Header.Del("Content-Type") // The generated client sets it even when an optional body is left out.
	}
}

// checkRedirect is the redirect policy of every request. The key may only reach the base URL: same scheme, host and
// port, under the path prefix, on every hop so far. net/http's own rule is laxer: it keeps Authorization for another
// port of the same host name and for subdomains.
func (c *Client) checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return errRedirectRefused{"too many redirects"}
	}
	if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
		return errRedirectRefused{"a redirect to a scheme other than http or https"}
	}
	trusted := c.underBaseURL(req.URL)
	for _, hop := range via {
		trusted = trusted && c.underBaseURL(hop.URL)
	}
	if !trusted {
		req.Header.Del("Authorization")
		req.Header.Del("X-Rask-Client")
	}
	if c.debug {
		c.logger.Log(req.Context(), slog.LevelDebug, "rask: redirect", "url", c.redactURL(req.URL), "authorization_dropped", !trusted)
	}
	return nil
}

type errRedirectRefused struct{ reason string }

func (e errRedirectRefused) Error() string { return "rask: refused: " + e.reason }

// underBaseURL reports whether u is on the base URL's origin and under its path.
func (c *Client) underBaseURL(u *url.URL) bool {
	if u.Scheme != c.baseURL.Scheme || !sameHostPort(u, c.baseURL) {
		return false
	}
	return strings.HasPrefix(u.EscapedPath(), c.baseURL.EscapedPath()+"/")
}

func sameHostPort(a, b *url.URL) bool {
	return strings.EqualFold(a.Hostname(), b.Hostname()) && effectivePort(a) == effectivePort(b)
}

func effectivePort(u *url.URL) string {
	if port := u.Port(); port != "" {
		return port
	}
	if u.Scheme == "https" {
		return "443"
	}
	return "80"
}

func isRetryableTransportError(err error) bool {
	var refused errRedirectRefused
	if errors.As(err, &refused) {
		return false
	}
	var verification *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	if errors.As(err, &verification) || errors.As(err, &unknownAuthority) || errors.As(err, &hostname) || errors.As(err, &invalid) {
		return false
	}
	return true
}

func problemCode(body []byte) string {
	var problem struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(body, &problem) != nil {
		return ""
	}
	return problem.Code
}

// backoff is 0.5 s growing by 1.5 up to 30 s, with jitter (S3).
func (c *Client) backoff(attempt int) time.Duration {
	wait := float64(backoffInitial) * math.Pow(backoffFactor, float64(attempt))
	wait *= backoffJitterFrom + c.rand()/2
	return min(time.Duration(wait), backoffMax)
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) warnIfDeprecated(operationID string, header http.Header) {
	deprecation := header.Get("Deprecation")
	if deprecation == "" {
		return
	}
	if _, seen := c.warned.LoadOrStore(operationID, struct{}{}); seen {
		return
	}
	warning := DeprecationWarning{
		Operation:   operationID,
		Deprecation: deprecation,
		Sunset:      header.Get("Sunset"),
		Link:        header.Get("Link"),
	}
	if c.warn != nil {
		c.warn(warning)
		return
	}
	c.logger.Warn("rask: this operation is deprecated", "operation", warning.Operation, "sunset", warning.Sunset, "link", warning.Link)
}

// Debug logging. Bodies are never logged. Only these headers are, and Authorization only as a placeholder.

func (c *Client) logRequest(ctx context.Context, req *http.Request, attempt int) {
	if !c.debug {
		return
	}
	attrs := []any{"method", req.Method, "url", c.redactURL(req.URL), "attempt", attempt + 1}
	if req.Header.Get("Authorization") != "" {
		attrs = append(attrs, "authorization", "Bearer [REDACTED]")
	}
	if key := req.Header.Get("Idempotency-Key"); key != "" {
		attrs = append(attrs, "idempotency_key", key)
	}
	c.logger.Log(ctx, slog.LevelDebug, "rask: request", attrs...)
}

func (c *Client) logResponse(ctx context.Context, req *http.Request, response *http.Response, took time.Duration) {
	if !c.debug {
		return
	}
	final := req.URL // A custom RoundTripper may leave response.Request empty; after a redirect it is the last hop.
	if response.Request != nil {
		final = response.Request.URL
	}
	c.logger.Log(ctx, slog.LevelDebug, "rask: response", "method", req.Method, "url", c.redactURL(final),
		"status", response.StatusCode, "took", took, "request_id", response.Header.Get("X-Request-Id"),
		"retry_after", response.Header.Get("Retry-After"))
}

func (c *Client) logFailure(ctx context.Context, req *http.Request, err error) {
	if !c.debug {
		return
	}
	c.logger.Log(ctx, slog.LevelDebug, "rask: request failed", "method", req.Method, "url", c.redactURL(req.URL),
		"error", redactError(err).Error())
}

func (c *Client) logRetry(ctx context.Context, operationID string, attempt int, wait time.Duration) {
	if !c.debug {
		return
	}
	c.logger.Log(ctx, slog.LevelDebug, "rask: retrying", "operation", operationID, "attempt", attempt+1, "wait", wait)
}

// sensitiveQuery names query parameters that are never logged, even on the API host.
var sensitiveQuery = []string{"signature", "sig", "token", "credential", "key", "password", "secret"}

// redactURL prepares a URL for a log. Off the API host the whole query goes: a presigned URL is a credential. On it,
// values whose names look secret go.
func (c *Client) redactURL(u *url.URL) string {
	clean := *u
	clean.User = nil
	if clean.RawQuery == "" {
		return clean.String()
	}
	if !sameHostPort(u, c.baseURL) {
		clean.RawQuery = "REDACTED"
		return clean.String()
	}
	parts := strings.Split(clean.RawQuery, "&")
	for i, part := range parts {
		name, _, _ := strings.Cut(part, "=")
		lower := strings.ToLower(name)
		for _, sensitive := range sensitiveQuery {
			if strings.Contains(lower, sensitive) {
				parts[i] = name + "=REDACTED"
				break
			}
		}
	}
	clean.RawQuery = strings.Join(parts, "&")
	return clean.String()
}

// redactError removes the URL net/http puts in its errors: for a presigned URL, that URL is a credential.
func redactError(err error) error {
	var urlError *url.Error
	if errors.As(err, &urlError) {
		return fmt.Errorf("%s: %w", urlError.Op, urlError.Err)
	}
	return err
}
