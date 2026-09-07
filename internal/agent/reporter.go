package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/MKITConsulting/zensu-monitoring-agent/internal/redact"

	obs "github.com/MKITConsulting/zensu-monitoring-agent/internal/metrics"
)

// Reporter posts heartbeat batches to the Zensu API over outbound HTTPS. It is
// the only component that makes outbound network calls, and it only ever POSTs
// heartbeats to the single configured URL. (The optional metrics endpoint is a
// separate inbound listener owned by the metrics package.)
type Reporter struct {
	BaseURL string
	APIKey  string
	Client  *http.Client
	Metrics *obs.Metrics
}

// ValidateAPIURL applies the scrape URL's rules to the heartbeat base URL. Both
// are rendered into the same ConfigMap by the same mechanism, so both need the
// same refusal — an operator fronting a self-hosted Zensu with basic auth would
// otherwise put that password in a ConfigMap with no warning, while the chart
// deliberately routes the API key through a Secret.
func ValidateAPIURL(raw string) error {
	if err := validateURL(raw); err != nil {
		return fmt.Errorf("ZENSU_API_URL: %w", err)
	}
	return nil
}

// NewReporter builds a Reporter with the given request timeout. Redirects are
// refused: this client carries the API key, a heartbeat POST has no legitimate
// 3xx, and following one would send a credentialed request to a destination the
// operator never configured.
func NewReporter(baseURL, apiKey string, timeout time.Duration) *Reporter {
	return &Reporter{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		Client: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// Send POSTs a heartbeat batch to /api/runtime/heartbeat.
func (r *Reporter) Send(ctx context.Context, batch HeartbeatBatch) (err error) {
	start := time.Now()
	defer func() { r.Metrics.RecordHeartbeat(err == nil, time.Since(start)) }()

	body, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.BaseURL+"/api/runtime/heartbeat", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", r.APIKey)

	resp, err := r.Client.Do(req)
	if err != nil {
		return fmt.Errorf("heartbeat request failed: %s", redact.ForLog(redact.TransportReason(err)))
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusMultipleChoices {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, redact.SnippetBytes))
		return fmt.Errorf("heartbeat rejected (%d): %s", resp.StatusCode, redact.ForLog(string(snippet)))
	}
	return nil
}
