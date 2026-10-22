package rask

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// expiryMargin refreshes tokens slightly before they expire so that a token
// does not run out while a request is in flight.
const expiryMargin = time.Minute

type tokenSource struct {
	static string
	cfg    *clientcredentials.Config
	hc     *http.Client

	mu     sync.Mutex
	cached *oauth2.Token
}

func newTokenSource(c config, clientID, clientSecret string) *tokenSource {
	if c.accessToken != "" {
		return &tokenSource{static: c.accessToken}
	}
	return &tokenSource{
		cfg: &clientcredentials.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			TokenURL:     c.tokenURL,
			Scopes:       c.scopes,
			// Credentials go in the form body (client_secret_post), as in the
			// Python SDK.
			AuthStyle: oauth2.AuthStyleInParams,
		},
		hc: c.httpClient,
	}
}

func (s *tokenSource) token(ctx context.Context) (string, error) {
	if s.static != "" {
		return s.static, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cached != nil && (s.cached.Expiry.IsZero() || time.Until(s.cached.Expiry) > expiryMargin) {
		return s.cached.AccessToken, nil
	}
	tok, err := s.cfg.Token(context.WithValue(ctx, oauth2.HTTPClient, s.hc))
	if err != nil {
		return "", &AuthError{Err: err}
	}
	s.cached = tok
	return tok.AccessToken, nil
}

// invalidate drops the cached token unless it was already replaced.
func (s *tokenSource) invalidate(used string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cached != nil && s.cached.AccessToken == used {
		s.cached = nil
	}
}

type authTransport struct {
	base      http.RoundTripper
	tokens    *tokenSource
	userAgent string
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, used, err := t.send(req)
	if err != nil || resp.StatusCode != http.StatusUnauthorized || t.tokens.static != "" {
		return resp, err
	}
	// The token may have been revoked or expired early: refresh it and retry
	// once, if the request body can be replayed.
	if req.Body != nil && req.GetBody == nil {
		return resp, nil
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	t.tokens.invalidate(used)

	retry := req
	if req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		retry = req.Clone(req.Context())
		retry.Body = body
	}
	resp, _, err = t.send(retry)
	return resp, err
}

func (t *authTransport) send(req *http.Request) (*http.Response, string, error) {
	tok, err := t.tokens.token(req.Context())
	if err != nil {
		if req.Body != nil {
			req.Body.Close()
		}
		return nil, "", err
	}
	out := req.Clone(req.Context())
	out.Header.Set("Authorization", "Bearer "+tok)
	if t.userAgent != "" {
		out.Header.Set("User-Agent", t.userAgent)
	}
	resp, err := t.base.RoundTrip(out)
	return resp, tok, err
}
