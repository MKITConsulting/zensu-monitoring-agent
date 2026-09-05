package redact

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

const leak = "http://collector:9090/metrics?token=s3cret"

func TestTransportReason(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"plain error", errors.New("refused"), "refused"},
		{"url error", &url.Error{Op: "Get", URL: leak, Err: errors.New("refused")}, "refused"},
		{"url error without a cause", &url.Error{Op: "Get", URL: leak}, "Get"},
		{"wrapped url error", fmt.Errorf("outer: %w", &url.Error{Op: "Get", URL: leak, Err: errors.New("refused")}), "refused"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := TransportReason(c.err)
			if got != c.want {
				t.Errorf("TransportReason = %q, want %q", got, c.want)
			}
			if strings.Contains(got, "s3cret") || strings.Contains(got, "collector:9090") {
				t.Errorf("the reason must not carry the request URL, got %q", got)
			}
		})
	}
}

func TestSanitize(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "hello", "hello"},
		{"trims surrounding space", "  hello  ", "hello"},
		{"drops newlines so one record stays one line", "a\nb\r\nc", "abc"},
		{"drops control bytes", "a\x00\x1bb", "ab"},
		{"drops non-ascii", "aé☃b", "ab"},
		{"keeps the printable range", " !~", "!~"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Sanitize(c.in); got != c.want {
				t.Errorf("Sanitize(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestTruncate(t *testing.T) {
	cases := []struct {
		name string
		in   string
		n    int
		want string
	}{
		{"shorter than the bound", "abc", 5, "abc"},
		{"exactly at the bound", "abcde", 5, "abcde"},
		{"longer than the bound", "abcdef", 5, "abcde"},
		{"zero bound", "abc", 0, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Truncate(c.in, c.n); got != c.want {
				t.Errorf("Truncate(%q, %d) = %q, want %q", c.in, c.n, got, c.want)
			}
		})
	}
}

// TestForLogBoundsAndStrips pins that the two rules compose: a peer sending a
// megabyte of control bytes must reach a log record as a short printable line.
func TestForLogBoundsAndStrips(t *testing.T) {
	got := ForLog(strings.Repeat("A\n", SnippetBytes*4))

	if len(got) > SnippetBytes {
		t.Errorf("length = %d, want at most %d", len(got), SnippetBytes)
	}
	if strings.ContainsAny(got, "\n\r") {
		t.Errorf("a log record must stay on one line, got %q", got)
	}
	if strings.Trim(got, "A") != "" {
		t.Errorf("only the printable bytes may survive, got %q", got)
	}
}
