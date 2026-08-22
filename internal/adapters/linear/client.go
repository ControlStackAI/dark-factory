// Package linear implements the bounded Linear Cloud GraphQL adapter.
package linear

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/ports"
)

const (
	defaultRequestTimeout  = 10 * time.Second
	defaultMaxRequestSize  = int64(1 << 20)
	defaultMaxResponseSize = int64(2 << 20)
	defaultMaxPages        = 100
	defaultPageSize        = 100
	defaultMaxRetries      = 3
	defaultInitialBackoff  = 100 * time.Millisecond
	defaultMaxRetryAfter   = 5 * time.Second
)

// Options are deliberately bounded. Zero values select conservative defaults.
type Options struct {
	Endpoint         string
	APIKey           string
	TeamID           string
	ProjectID        string
	IssueAllowlist   []string
	ReadyName        string
	InProgressName   string
	DoneName         string
	HTTPClient       *http.Client
	RequestTimeout   time.Duration
	MaxRequestBytes  int64
	MaxResponseBytes int64
	MaxPages         int
	PageSize         int
	MaxRetries       int
	InitialBackoff   time.Duration
	MaxRetryAfter    time.Duration
	Sleep            func(context.Context, time.Duration) error
	Hook             func(string) error
}

// Client is safe for concurrent use. State-changing reconciliation is serialized so two
// callers in one process cannot both observe an absent keyed comment and create it.
type Client struct {
	endpoint        string
	apiKey          string
	teamID          string
	projectID       string
	allowlist       map[string]struct{}
	readyName       string
	inProgressName  string
	doneName        string
	httpClient      *http.Client
	requestTimeout  time.Duration
	maxRequestBytes int64
	maxBytes        int64
	maxPages        int
	pageSize        int
	maxRetries      int
	initialBackoff  time.Duration
	maxRetryAfter   time.Duration
	sleep           func(context.Context, time.Duration) error
	hook            func(string) error
	mutationMu      sync.Mutex
}

func New(options Options) (*Client, error) {
	if strings.TrimSpace(options.Endpoint) == "" || strings.TrimSpace(options.APIKey) == "" ||
		strings.TrimSpace(options.TeamID) == "" || strings.TrimSpace(options.ProjectID) == "" {
		return nil, errors.New("Linear endpoint, API key, team, and project are required")
	}
	if options.ReadyName == "" || options.InProgressName == "" || options.DoneName == "" {
		return nil, errors.New("Linear lifecycle preferred names are required")
	}
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.Fragment != "" ||
		(endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && loopbackHost(endpoint.Hostname()))) {
		return nil, errors.New("Linear endpoint must be HTTPS (or loopback HTTP for tests) without user information or fragments")
	}
	for _, value := range []string{options.APIKey, options.TeamID, options.ProjectID, options.ReadyName, options.InProgressName, options.DoneName} {
		if strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n\x00") {
			return nil, errors.New("Linear options contain unsafe whitespace or control characters")
		}
	}
	if options.RequestTimeout == 0 {
		options.RequestTimeout = defaultRequestTimeout
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = defaultMaxResponseSize
	}
	if options.MaxRequestBytes == 0 {
		options.MaxRequestBytes = defaultMaxRequestSize
	}
	if options.MaxPages == 0 {
		options.MaxPages = defaultMaxPages
	}
	if options.PageSize == 0 {
		options.PageSize = defaultPageSize
	}
	if options.MaxRetries == 0 {
		options.MaxRetries = defaultMaxRetries
	}
	if options.InitialBackoff == 0 {
		options.InitialBackoff = defaultInitialBackoff
	}
	if options.MaxRetryAfter == 0 {
		options.MaxRetryAfter = defaultMaxRetryAfter
	}
	if options.RequestTimeout <= 0 || options.RequestTimeout > 2*time.Minute ||
		options.MaxRequestBytes < 1 || options.MaxRequestBytes > 16<<20 ||
		options.MaxResponseBytes < 1 || options.MaxResponseBytes > 64<<20 || options.MaxPages < 1 || options.MaxPages > 1000 ||
		options.PageSize < 1 || options.PageSize > 100 || options.MaxRetries < 0 ||
		options.MaxRetries > 20 || options.InitialBackoff <= 0 || options.InitialBackoff > options.MaxRetryAfter ||
		options.MaxRetryAfter <= 0 || options.MaxRetryAfter > time.Minute {
		return nil, errors.New("Linear transport bounds are invalid")
	}
	if options.HTTPClient == nil {
		options.HTTPClient = &http.Client{}
	}
	boundedHTTPClient := *options.HTTPClient
	boundedHTTPClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	if options.Sleep == nil {
		options.Sleep = sleepContext
	}
	allowlist := make(map[string]struct{}, len(options.IssueAllowlist))
	for _, id := range options.IssueAllowlist {
		if strings.TrimSpace(id) == "" {
			return nil, errors.New("Linear issue allowlist contains an empty ID")
		}
		if _, exists := allowlist[id]; exists {
			return nil, errors.New("Linear issue allowlist contains a duplicate ID")
		}
		allowlist[id] = struct{}{}
	}
	return &Client{
		endpoint: options.Endpoint, apiKey: options.APIKey, teamID: options.TeamID,
		projectID: options.ProjectID, allowlist: allowlist, readyName: options.ReadyName,
		inProgressName: options.InProgressName, doneName: options.DoneName,
		httpClient: &boundedHTTPClient, requestTimeout: options.RequestTimeout, maxRequestBytes: options.MaxRequestBytes,
		maxBytes: options.MaxResponseBytes, maxPages: options.MaxPages, pageSize: options.PageSize,
		maxRetries: options.MaxRetries, initialBackoff: options.InitialBackoff,
		maxRetryAfter: options.MaxRetryAfter, sleep: options.Sleep, hook: options.Hook,
	}, nil
}

func (c *Client) callHook(phase string) error {
	if c.hook == nil {
		return nil
	}
	return c.hook(phase)
}

func loopbackHost(host string) bool {
	return host == "localhost" || net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback()
}

type graphQLRequest struct {
	OperationName string         `json:"operationName"`
	Query         string         `json:"query"`
	Variables     map[string]any `json:"variables"`
}

type graphQLError struct {
	Message string `json:"message"`
}

type graphQLResponse struct {
	Data       json.RawMessage `json:"data"`
	Errors     []graphQLError  `json:"errors"`
	Extensions json.RawMessage `json:"extensions,omitempty"`
}

func (c *Client) query(ctx context.Context, operation, query string, variables map[string]any, target any) error {
	return c.request(ctx, operation, query, variables, target, false)
}

func (c *Client) mutate(ctx context.Context, operation, query string, variables map[string]any, target any) error {
	return c.request(ctx, operation, query, variables, target, true)
}

func (c *Client) request(ctx context.Context, operation, query string, variables map[string]any, target any, mutation bool) error {
	payload, err := json.Marshal(graphQLRequest{OperationName: operation, Query: query, Variables: variables})
	if err != nil {
		return fmt.Errorf("Linear GraphQL %s request encoding failed", operation)
	}
	if int64(len(payload)) > c.maxRequestBytes {
		return fmt.Errorf("Linear GraphQL %s request exceeds %d bytes", operation, c.maxRequestBytes)
	}
	attempts := 1 + c.maxRetries
	var last error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			delay := c.initialBackoff << min(attempt-1, 20)
			if retry, ok := last.(*retryableError); ok && retry.after > 0 {
				delay = retry.after
			}
			if delay > c.maxRetryAfter {
				delay = c.maxRetryAfter
			}
			if err := c.sleep(ctx, delay); err != nil {
				return fmt.Errorf("Linear GraphQL %s retry canceled: %w", operation, err)
			}
		}
		last = c.requestOnce(ctx, operation, payload, target)
		if last == nil {
			return nil
		}
		var retry *retryableError
		if !errors.As(last, &retry) || mutation && retry.status != http.StatusTooManyRequests {
			return last
		}
	}
	return fmt.Errorf("Linear GraphQL %s retry budget exhausted: %w", operation, last)
}

type retryableError struct {
	operation string
	status    int
	after     time.Duration
	cause     error
}

func (e *retryableError) Error() string {
	if e.status != 0 {
		return fmt.Sprintf("Linear GraphQL %s temporary HTTP status %d", e.operation, e.status)
	}
	return fmt.Sprintf("Linear GraphQL %s temporary transport failure", e.operation)
}
func (e *retryableError) Unwrap() error { return e.cause }

func (c *Client) requestOnce(ctx context.Context, operation string, payload []byte, target any) error {
	requestCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("Linear GraphQL %s request setup failed", operation)
	}
	req.Header.Set("Authorization", c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "dark-factory/0.1")
	response, err := c.httpClient.Do(req)
	if err != nil {
		return &retryableError{operation: operation, cause: err}
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		if err := drainBounded(response.Body, c.maxBytes); err != nil {
			return fmt.Errorf("Linear GraphQL %s response exceeds %d bytes", operation, c.maxBytes)
		}
		return fmt.Errorf("Linear GraphQL %s authentication failed", operation)
	}
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
		if err := drainBounded(response.Body, c.maxBytes); err != nil {
			return fmt.Errorf("Linear GraphQL %s response exceeds %d bytes", operation, c.maxBytes)
		}
		return &retryableError{operation: operation, status: response.StatusCode, after: retryAfter(response.Header.Get("Retry-After"), c.maxRetryAfter)}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if err := drainBounded(response.Body, c.maxBytes); err != nil {
			return fmt.Errorf("Linear GraphQL %s response exceeds %d bytes", operation, c.maxBytes)
		}
		return fmt.Errorf("Linear GraphQL %s failed with HTTP status %d", operation, response.StatusCode)
	}
	limited := io.LimitReader(response.Body, c.maxBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("Linear GraphQL %s response read failed", operation)
	}
	if int64(len(body)) > c.maxBytes {
		return fmt.Errorf("Linear GraphQL %s response exceeds %d bytes", operation, c.maxBytes)
	}
	var envelope graphQLResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&envelope); err != nil {
		return fmt.Errorf("Linear GraphQL %s returned malformed JSON", operation)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("Linear GraphQL %s returned malformed JSON", operation)
	}
	if len(envelope.Errors) > 0 {
		return fmt.Errorf("Linear GraphQL %s returned %d error(s); details redacted", operation, len(envelope.Errors))
	}
	if len(envelope.Data) == 0 || bytes.Equal(bytes.TrimSpace(envelope.Data), []byte("null")) {
		return fmt.Errorf("Linear GraphQL %s returned no data", operation)
	}
	if err := json.Unmarshal(envelope.Data, target); err != nil {
		return fmt.Errorf("Linear GraphQL %s returned an invalid data shape", operation)
	}
	return nil
}

func drainBounded(reader io.Reader, maximum int64) error {
	written, err := io.Copy(io.Discard, io.LimitReader(reader, maximum+1))
	if err != nil {
		return err
	}
	if written > maximum {
		return errors.New("response too large")
	}
	return nil
}

func retryAfter(raw string, maximum time.Duration) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
		d := time.Duration(seconds) * time.Second
		if d > maximum {
			return maximum
		}
		return d
	}
	if when, err := http.ParseTime(raw); err == nil {
		d := time.Until(when)
		if d < 0 {
			return 0
		}
		if d > maximum {
			return maximum
		}
		return d
	}
	return 0
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) allowedReference(id string) bool {
	if len(c.allowlist) == 0 {
		return true
	}
	_, ok := c.allowlist[id]
	return ok
}

func (c *Client) allowedIssue(issue remoteIssue) bool {
	if len(c.allowlist) == 0 {
		return true
	}
	_, byID := c.allowlist[issue.ID]
	_, byIdentifier := c.allowlist[issue.Identifier]
	return byID || byIdentifier
}

func (c *Client) enforceScope(issue remoteIssue) error {
	if err := c.enforceTeamProject(issue); err != nil {
		return err
	}
	if !c.allowedIssue(issue) {
		return fmt.Errorf("%w: Linear issue is outside the configured allowlist", ports.ErrConflict)
	}
	return nil
}

func (c *Client) enforceTeamProject(issue remoteIssue) error {
	if issue.ID == "" || issue.Identifier == "" || issue.Project == nil || issue.Team.ID == "" || issue.State.ID == "" {
		return errors.New("Linear returned an incomplete issue; response details redacted")
	}
	if issue.Project.ID != c.projectID || issue.Team.ID != c.teamID {
		return fmt.Errorf("%w: Linear issue is outside the configured team/project", ports.ErrConflict)
	}
	return nil
}
