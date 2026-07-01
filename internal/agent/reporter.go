package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

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

// NewReporter builds a Reporter with the given request timeout.
func NewReporter(baseURL, apiKey string, timeout time.Duration) *Reporter {
	return &Reporter{
		BaseURL: baseURL,
		APIKey:  apiKey,
		Client:  &http.Client{Timeout: timeout},
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
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusMultipleChoices {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		return fmt.Errorf("heartbeat rejected (%s): %s", resp.Status, string(snippet))
	}
	return nil
}
