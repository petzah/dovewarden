package doveadm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// UserAgent is sent with every request to Doveadm. main sets it to dovewarden/<version>.
var UserAgent = "dovewarden"

// Client handles communication with the Doveadm API
type Client struct {
	baseURL  string
	password string
	client   *http.Client
}

// NewClient creates a new Doveadm API client
func NewClient(baseURL, password string) *Client {
	return &Client{
		baseURL:  baseURL,
		password: password,
		client:   &http.Client{},
	}
}

// ResponseError represents an error entry returned by Doveadm
// [ [ "error", {"type":"exitCode","exitCode":75}, "dovewarden-sync" ] ]
type ResponseError struct {
	Type     string `json:"type"`
	ExitCode int    `json:"exitCode"`
}

// SyncError is a sync that Doveadm itself reported as failed.
type SyncError struct {
	Tag      string
	Type     string
	ExitCode int
}

func (e *SyncError) Error() string {
	return fmt.Sprintf("doveadm sync error (tag %s): %s (exitCode %d)", e.Tag, e.Type, e.ExitCode)
}

// UnreachableError means Doveadm itself could not be reached or is not serving: the
// connection failed, or a proxy in front of it answered 502, 503 or 504. It says nothing
// about the user, so retrying the user alone does not help.
type UnreachableError struct {
	Err error
}

func (e *UnreachableError) Error() string {
	return fmt.Sprintf("doveadm unreachable: %v", e.Err)
}

func (e *UnreachableError) Unwrap() error { return e.Err }

// IsUnreachable reports whether err means Doveadm could not be reached.
func IsUnreachable(err error) bool {
	var u *UnreachableError
	return errors.As(err, &u)
}

func unreachableStatus(code int) bool {
	return code == http.StatusBadGateway || code == http.StatusServiceUnavailable || code == http.StatusGatewayTimeout
}

// do sends req and reads the whole response. A failed connection is an UnreachableError,
// unless ctx was cancelled, which is the caller giving up rather than Doveadm failing.
func (c *Client) do(ctx context.Context, req *http.Request) (int, []byte, error) {
	req.Header.Set("User-Agent", UserAgent)
	resp, err := c.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 0, nil, fmt.Errorf("failed to send request: %w", err)
		}
		return 0, nil, &UnreachableError{Err: err}
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read response: %w", err)
	}
	if unreachableStatus(resp.StatusCode) {
		return resp.StatusCode, body, &UnreachableError{Err: fmt.Errorf("status %d", resp.StatusCode)}
	}
	return resp.StatusCode, body, nil
}

// Probe checks that Doveadm answers, without touching any mailbox: it asks for the list of
// commands. Any answer other than an UnreachableError counts as up.
func (c *Client) Probe(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/doveadm/v1", nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.SetBasicAuth("doveadm", c.password)
	_, _, err = c.do(ctx, req)
	return err
}

// SyncResponse represents the response from a sync operation
type SyncResponse struct {
	State string // Replication state for incremental sync
}

// responseEntry models a single Doveadm response array.
type responseEntry struct {
	Status       string
	Error        *ResponseError
	Response     map[string]interface{}
	ResponseList []map[string]interface{}
	Tag          string
}

func (r *responseEntry) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) < 3 {
		return fmt.Errorf("unexpected response format: %s", string(data))
	}

	if err := json.Unmarshal(raw[0], &r.Status); err != nil {
		return fmt.Errorf("failed to parse status: %w", err)
	}

	// Parse the second element based on status
	// If status is "error", parse as error payload; otherwise parse as response object
	if r.Status == "error" {
		var errPayload ResponseError
		if err := json.Unmarshal(raw[1], &errPayload); err != nil {
			return fmt.Errorf("failed to parse error payload: %w", err)
		}
		r.Error = &errPayload
	} else {
		// Parse as response object for successful responses. Dovecot may return
		// either a single map or an array of maps as the second element.
		var respObj map[string]interface{}
		if err := json.Unmarshal(raw[1], &respObj); err == nil && len(respObj) > 0 {
			r.Response = respObj
		} else {
			var respArr []map[string]interface{}
			if err := json.Unmarshal(raw[1], &respArr); err == nil {
				r.ResponseList = respArr
			}
		}
	}

	if err := json.Unmarshal(raw[2], &r.Tag); err != nil {
		return fmt.Errorf("failed to parse tag: %w", err)
	}

	return nil
}

// Sync performs a dsync operation for the given user to the specified destination.
// If state is provided (non-empty), it will be used for incremental sync.
// Returns the new state string for the next sync operation.
//
// An empty state is not a full sync: dsync still skips a mailbox whose message count,
// uidnext and highestmodseq are equal on both sides, even when the mail differs. Only
// full (doveadm sync -f) compares every mailbox.
func (c *Client) Sync(ctx context.Context, username string, destination string, state string, full bool) (*SyncResponse, error) {
	// Build the request payload according to Doveadm API format:
	// [["sync",{"destination":["$destination"],"user":"$username","state":"$state"},"tag1"]]
	params := map[string]interface{}{
		"destination": []string{destination},
		"state":       state,
		"user":        username,
	}
	if full {
		params["fullSync"] = true
	}

	payload := []interface{}{
		[]interface{}{
			"sync",
			params,
			"dovewarden-sync",
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/doveadm/v1", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("doveadm", c.password)

	status, respBody, err := c.do(ctx, req)
	if err != nil {
		return nil, err
	}

	// Check for HTTP errors
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("doveadm sync failed with status %d: %s", status, string(respBody))
	}

	// Doveadm API returns error with HTTP 200 but indicates failure in the response body
	var respPayload []responseEntry
	if err := json.Unmarshal(respBody, &respPayload); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	syncResp := &SyncResponse{}
	for _, entry := range respPayload {
		if entry.Status == "error" {
			if entry.Error != nil {
				return nil, &SyncError{Tag: entry.Tag, Type: entry.Error.Type, ExitCode: entry.Error.ExitCode}
			}
			return nil, fmt.Errorf("doveadm sync error (tag %s): unknown reason", entry.Tag)
		}

		// Extract state from response if available
		if entry.Response != nil {
			if stateVal, ok := entry.Response["state"].(string); ok {
				syncResp.State = stateVal
			}
		}
		if len(entry.ResponseList) > 0 {
			if stateVal, ok := entry.ResponseList[0]["state"].(string); ok {
				syncResp.State = stateVal
			}
		}
	}

	return syncResp, nil
}

// User represents a user returned by the user list command
type User struct {
	Username string `json:"username"`
	UID      string `json:"uid"`
	GID      string `json:"gid"`
	Home     string `json:"home"`
}

// ListUsers retrieves all users from the doveadm API
func (c *Client) ListUsers(ctx context.Context) ([]User, error) {
	// Build the request payload according to Doveadm API format:
	// [["user",{"userMask":"*"},"tag1"]]
	params := map[string]interface{}{
		"userMask": "*",
	}

	payload := []interface{}{
		[]interface{}{
			"user",
			params,
			"dovewarden-list-users",
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/doveadm/v1", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("doveadm", c.password)

	status, respBody, err := c.do(ctx, req)
	if err != nil {
		return nil, err
	}

	// Check for HTTP errors
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("doveadm user list failed with status %d: %s", status, string(respBody))
	}

	// Parse response
	var respPayload []responseEntry
	if err := json.Unmarshal(respBody, &respPayload); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	var users []User
	for _, entry := range respPayload {
		if entry.Status == "error" {
			if entry.Error != nil {
				return nil, fmt.Errorf("doveadm user list error (tag %s): %s (exitCode %d)", entry.Tag, entry.Error.Type, entry.Error.ExitCode)
			}
			return nil, fmt.Errorf("doveadm user list error (tag %s): unknown reason", entry.Tag)
		}

		// Extract users from response
		// Response contains {"userList": ["user1", "user2", ...]}
		if entry.Response != nil {
			if userList, ok := entry.Response["userList"].([]interface{}); ok {
				for _, item := range userList {
					if username, ok := item.(string); ok {
						user := User{
							Username: username,
						}
						users = append(users, user)
					}
				}
			}
		}
	}

	return users, nil
}
