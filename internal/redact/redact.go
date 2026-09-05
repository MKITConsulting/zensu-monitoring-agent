// Package redact bounds and strips the strings that reach a log record.
//
// It is a leaf with no dependency of its own, because both HTTP clients in this
// module need the same policy: the exposition reader quotes bytes a scraped peer
// chose, and the heartbeat reporter quotes bytes the backend chose, and neither
// concern belongs to the other's package.
package redact

import (
	"errors"
	"net/url"
	"strings"
)

// SnippetBytes bounds how much of a remote peer's body is quoted back into an
// error that will be logged. One bound for every client that logs remote bytes.
const SnippetBytes = 256

// StatusPhraseBytes bounds the peer-chosen HTTP reason phrase.
const StatusPhraseBytes = 64

// TransportReason strips the request URL out of an HTTP transport error.
// *url.Error embeds the full URL in its Error(), query string included, which
// would put back exactly what startup validation refuses to log.
func TransportReason(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Err != nil {
			return urlErr.Err.Error()
		}
		return urlErr.Op
	}
	return err.Error()
}

// ForLog reduces remote bytes to printable ASCII on one line, bounded by
// SnippetBytes.
func ForLog(s string) string { return Sanitize(Truncate(s, SnippetBytes)) }

// Sanitize reduces peer-controlled bytes to printable ASCII on one line before
// they reach a log record.
func Sanitize(s string) string {
	s = strings.TrimSpace(s)
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r > 0x7e {
			return -1
		}
		return r
	}, s)
}

// Truncate bounds a peer-controlled string to n bytes.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
