// Package rask is the Go SDK for the Rask API v3.
//
//	client, err := rask.New() // RASK_API_KEY, and RASK_BASE_URL if set
//	me, err := client.Account.GetMe(ctx)
//
// Operations are grouped by resource and named after their operationId: client.Folders.CreateFolder,
// client.Jobs.GetJob. Each returns a *Response with the typed Data and the raw status, headers and body. Requests are
// retried, keyed and authenticated here; see New and the options. Types live in the api subpackage.
package rask

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/braskai/rask-sdk-go/v3/api"
)

const (
	// DefaultBaseURL is the production API. Override it with WithBaseURL or RASK_BASE_URL.
	DefaultBaseURL = "https://api.rask.ai/v3"
	// DefaultTimeout bounds one HTTP attempt: the server's 25 s budget plus a 20 s long-poll, with a margin (S13).
	DefaultTimeout = 60 * time.Second
	// DefaultWaitTimeout bounds Jobs.Wait; 4 h covers on-screen jobs, which take about 2.5 h (S6).
	DefaultWaitTimeout = 4 * time.Hour

	envAPIKey  = "RASK_API_KEY" //nolint:gosec // The variable's name, not a credential.
	envBaseURL = "RASK_BASE_URL"
	envLog     = "RASK_LOG"

	// retryBudget caps the time one logical call spends retrying (S3).
	retryBudget = 120 * time.Second
)

// Client talks to the Rask API. It is safe for concurrent use. Build it with New.
type Client struct {
	services

	// apiKey is a closure, not a string, so that no dump of the struct can print the key.
	apiKey     func() string
	baseURL    *url.URL
	baseString string
	timeout    time.Duration
	maxRetries int // negative: limited only by the retry budget
	http       *http.Client
	low        *api.Client
	userAgent  string
	clientID   string
	debug      bool
	logger     *slog.Logger
	warn       func(DeprecationWarning)
	warned     sync.Map // operation id -> struct{}

	// Replaced in tests so that the retry budget can be tested without the wait.
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
	rand  func() float64
}

// Option configures New.
type Option func(*config)

type config struct {
	apiKey     string
	baseURL    string
	timeout    time.Duration
	maxRetries int
	httpClient *http.Client
	logger     *slog.Logger
	debug      bool
	warn       func(DeprecationWarning)
}

// WithAPIKey sets the API key. Without it New reads RASK_API_KEY.
func WithAPIKey(key string) Option { return func(c *config) { c.apiKey = key } }

// WithBaseURL sets the API base URL, path prefix included: https://gw.example.com/rask/v3. Without it New reads
// RASK_BASE_URL, then uses DefaultBaseURL. A trailing slash makes no difference. Every operation and helper goes
// under this URL, and the API key goes nowhere else.
func WithBaseURL(baseURL string) Option { return func(c *config) { c.baseURL = baseURL } }

// WithTimeout sets the timeout of one HTTP attempt to the API (default 60 s). Zero turns it off. Presigned uploads are
// bounded by a stall timeout of the same length instead, since a large file takes longer than any fixed limit.
func WithTimeout(d time.Duration) Option { return func(c *config) { c.timeout = d } }

// WithMaxRetries limits the retries of one call; 0 turns retrying off. By default only the 120 s budget limits them.
func WithMaxRetries(n int) Option { return func(c *config) { c.maxRetries = n } }

// WithHTTPClient sets the HTTP client, for proxies, custom transports and tests. The SDK works on a copy and replaces
// its CheckRedirect, because that policy keeps the API key from following a redirect to another host.
func WithHTTPClient(client *http.Client) Option { return func(c *config) { c.httpClient = client } }

// WithLogger sets where the SDK writes debug traffic (when on) and deprecation warnings. It does not turn traffic
// logging on: that takes WithDebug or RASK_LOG=debug. Traffic is logged at slog.LevelDebug.
func WithLogger(logger *slog.Logger) Option { return func(c *config) { c.logger = logger } }

// WithDebug turns traffic logging on, as RASK_LOG=debug does. Logs show no bodies and redact Authorization and
// presigned query strings.
func WithDebug(on bool) Option { return func(c *config) { c.debug = on } }

// WithDeprecationHandler replaces the default deprecation warning (a Warn on the logger). It is called once per
// operation, on the first response with a Deprecation header.
func WithDeprecationHandler(handler func(DeprecationWarning)) Option {
	return func(c *config) { c.warn = handler }
}

// DeprecationWarning says that the server marked an operation as deprecated.
type DeprecationWarning struct {
	Operation   string // operationId
	Deprecation string // the Deprecation header
	Sunset      string // the Sunset header, if any
	Link        string // the Link header, if any
}

// New builds a Client. The API key comes from WithAPIKey or RASK_API_KEY, and the base URL from WithBaseURL,
// RASK_BASE_URL or DefaultBaseURL, in that order. An argument wins over the environment.
func New(opts ...Option) (*Client, error) {
	cfg := config{timeout: DefaultTimeout, maxRetries: -1}
	for _, opt := range opts {
		opt(&cfg)
	}

	key := cfg.apiKey
	if key == "" {
		key = os.Getenv(envAPIKey)
	}
	if key == "" {
		return nil, errors.New("rask: no API key: pass rask.WithAPIKey or set " + envAPIKey)
	}
	// The message must not repeat the key: it is about to be logged by whoever gets this error.
	if strings.ContainsAny(key, " \t\r\n") {
		return nil, errors.New("rask: the API key contains whitespace; check the value of " + envAPIKey)
	}

	rawBase := cfg.baseURL
	if rawBase == "" {
		rawBase = os.Getenv(envBaseURL)
	}
	if rawBase == "" {
		rawBase = DefaultBaseURL
	}
	base, err := parseBaseURL(rawBase)
	if err != nil {
		return nil, err
	}

	c := &Client{
		apiKey:     func() string { return key },
		baseURL:    base,
		baseString: base.String(),
		timeout:    cfg.timeout,
		maxRetries: cfg.maxRetries,
		// A development toolchain's version has spaces, which a product token cannot.
		userAgent: "rask-sdk-go/" + Version + " go/" + strings.ReplaceAll(strings.TrimPrefix(runtime.Version(), "go"), " ", "-"),
		clientID:  "rask-sdk-go/" + Version,
		debug:     cfg.debug || os.Getenv(envLog) == "debug",
		logger:    cfg.logger,
		warn:      cfg.warn,
		now:       time.Now,
		sleep:     sleepContext,
		rand:      rand.Float64,
	}
	if c.logger == nil {
		c.logger = defaultLogger(c.debug)
	}

	httpClient := http.Client{}
	if cfg.httpClient != nil {
		httpClient = *cfg.httpClient
	}
	httpClient.CheckRedirect = c.checkRedirect
	c.http = &httpClient

	// oapi-codegen resolves "./path" against the server, which keeps the prefix only with the trailing slash it adds.
	c.low, err = api.NewClient(c.baseString, api.WithHTTPClient(doer{c}))
	if err != nil {
		return nil, fmt.Errorf("rask: %w", err)
	}
	c.services = newServices(c)
	return c, nil
}

func defaultLogger(debug bool) *slog.Logger {
	if !debug {
		return slog.Default()
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// parseBaseURL validates a base URL and removes trailing slashes.
func parseBaseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, errors.New("rask: the base URL does not parse") // url.Error would repeat it, credentials and all.
	}
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return nil, fmt.Errorf("rask: the base URL needs an http or https scheme, got %q", u.Scheme)
	case u.Host == "":
		return nil, errors.New("rask: the base URL has no host")
	case u.User != nil:
		return nil, errors.New("rask: the base URL must not contain credentials; pass the key with rask.WithAPIKey")
	case u.RawQuery != "" || u.ForceQuery:
		return nil, errors.New("rask: the base URL must not have a query string")
	case u.Fragment != "":
		return nil, errors.New("rask: the base URL must not have a fragment")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = strings.TrimRight(u.RawPath, "/")
	return u, nil
}

// BaseURL returns the base URL in use, without a trailing slash.
func (c *Client) BaseURL() string { return c.baseString }

// Timeout returns the timeout of one HTTP attempt.
func (c *Client) Timeout() time.Duration { return c.timeout }

// String describes the client. The key never shows.
func (c *Client) String() string {
	return fmt.Sprintf("rask.Client{BaseURL: %q, Timeout: %s, APIKey: [REDACTED]}", c.baseString, c.timeout)
}

// GoString keeps %#v from printing the fields.
func (c *Client) GoString() string { return c.String() }

// Format keeps every fmt verb from printing the fields.
func (c *Client) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(c.String())) }

// Ptr returns a pointer to v, for optional request fields: api.UpdateProjectRequest{FolderId: rask.Ptr("")}.
func Ptr[T any](v T) *T { return &v }
