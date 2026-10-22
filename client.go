package rask

import (
	"context"
	"errors"
	"net/http"

	"github.com/braskai/rask-sdk-go/api"
)

const (
	// DefaultBaseURL is the production Rask API.
	DefaultBaseURL = "https://api.rask.ai"
	// DefaultTokenURL is the OAuth 2.0 token endpoint for API client credentials.
	DefaultTokenURL = "https://rask-prod.auth.us-east-2.amazoncognito.com/oauth2/token"
)

// DefaultScopes are the OAuth 2.0 scopes requested for API access.
var DefaultScopes = []string{"api/source", "api/input", "api/output", "api/limit"}

// RequestEditorFn can modify a request before it is sent, for example to add
// headers. It can be passed to any Client method.
type RequestEditorFn = api.RequestEditorFn

// Client is a Rask API client. It is safe for concurrent use.
type Client struct {
	api    *api.ClientWithResponses
	tokens *tokenSource
	http   *http.Client
}

type config struct {
	baseURL     string
	tokenURL    string
	scopes      []string
	httpClient  *http.Client
	accessToken string
	userAgent   string
}

// Option configures a Client.
type Option func(*config)

// WithBaseURL overrides the API base URL (default DefaultBaseURL).
func WithBaseURL(url string) Option { return func(c *config) { c.baseURL = url } }

// WithTokenURL overrides the OAuth 2.0 token endpoint (default DefaultTokenURL).
func WithTokenURL(url string) Option { return func(c *config) { c.tokenURL = url } }

// WithScopes overrides the requested OAuth 2.0 scopes (default DefaultScopes).
func WithScopes(scopes ...string) Option { return func(c *config) { c.scopes = scopes } }

// WithHTTPClient sets the HTTP client used for API and token requests. Its
// Transport is wrapped to add authentication; set a Timeout or use contexts
// to bound requests (media uploads can take a long time).
func WithHTTPClient(hc *http.Client) Option { return func(c *config) { c.httpClient = hc } }

// WithUserAgent sets the User-Agent header sent with API requests.
func WithUserAgent(ua string) Option { return func(c *config) { c.userAgent = ua } }

// WithAccessToken authenticates with a pre-issued access token instead of
// client credentials. The token is not refreshed.
func WithAccessToken(token string) Option { return func(c *config) { c.accessToken = token } }

// NewClient creates a client that authenticates with OAuth 2.0 client
// credentials. Tokens are fetched lazily, cached until shortly before they
// expire, and refreshed once if the API answers 401.
//
// Pass empty credentials together with WithAccessToken to use a pre-issued token.
func NewClient(clientID, clientSecret string, opts ...Option) (*Client, error) {
	cfg := config{
		baseURL:    DefaultBaseURL,
		tokenURL:   DefaultTokenURL,
		scopes:     DefaultScopes,
		httpClient: http.DefaultClient,
		userAgent:  "rask-sdk-go/" + Version,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.accessToken == "" && (clientID == "" || clientSecret == "") {
		return nil, errors.New("rask: client ID and secret are required unless WithAccessToken is used")
	}

	base := cfg.httpClient.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	tokens := newTokenSource(cfg, clientID, clientSecret)
	hc := *cfg.httpClient
	hc.Transport = &authTransport{base: base, tokens: tokens, userAgent: cfg.userAgent}

	low, err := api.NewClientWithResponses(cfg.baseURL, api.WithHTTPClient(&hc))
	if err != nil {
		return nil, err
	}
	return &Client{api: low, tokens: tokens, http: &hc}, nil
}

// API returns the low-level generated client. It shares this client's
// authentication and can be used for requests the typed methods don't cover.
func (c *Client) API() *api.ClientWithResponses { return c.api }

// AccessToken returns a valid access token, fetching one if needed.
func (c *Client) AccessToken(ctx context.Context) (string, error) {
	return c.tokens.token(ctx)
}

// response is implemented by every generated <Operation>Response type.
type response[T any] interface {
	GetJSON200() *T
	GetBody() []byte
	StatusCode() int
}

// unwrap returns the decoded 200 body, or an *APIError for any other status.
func unwrap[T any, R response[T]](resp R, err error) (*T, error) {
	if err != nil {
		return nil, err
	}
	if result := resp.GetJSON200(); result != nil {
		return result, nil
	}
	return nil, newAPIError(resp.StatusCode(), resp.GetBody())
}
