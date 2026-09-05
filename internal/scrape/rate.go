package scrape

import "time"

type observation struct {
	value float64
	at    time.Time
}

// MaxTrackedSeries bounds how many counter series a Rater remembers. The series
// come from an endpoint the agent does not control, so without a ceiling the peer
// would decide how much the agent retains for a whole heartbeat interval. Past
// the ceiling new fingerprints are refused rather than evicting established ones,
// so a flood cannot displace the series that are actually reporting.
const MaxTrackedSeries = 20_000

// Rater turns cumulative counter series into per-second rates by differencing
// consecutive scrapes. It holds one observation per series fingerprint, which is
// why it must live for the lifetime of the agent rather than per tick.
//
// It is NOT safe for concurrent use: Observe rebuilds an unsynchronized map. One
// caller per Rater, which is what a single heartbeat loop gives it.
type Rater struct {
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time

	// MaxTracked overrides MaxTrackedSeries; zero uses the constant.
	MaxTracked int

	prev    map[string]observation
	refused bool
}

// NewRater builds an empty Rater backed by the wall clock.
func NewRater() *Rater {
	return &Rater{prev: map[string]observation{}}
}

// RefusedSeries reports whether the last Observe hit the tracking ceiling. The
// caller surfaces it once rather than per tick.
func (r *Rater) RefusedSeries() bool { return r.refused }

// Observe consumes one scrape's counter series and returns those that had a
// usable predecessor, with Value replaced by the per-second rate and Kind set to
// KindGauge — a rate is an instantaneous value, not a cumulative one.
//
// Three cases yield nothing for a series, all of them normal rather than errors:
// its first observation (no predecessor to difference against), a counter reset
// (the process restarted, so the difference would be meaningless), and a
// non-advancing clock. State for fingerprints absent from this scrape is
// dropped, so pod churn cannot grow the map without bound.
//
// The tracking table is built in two passes so its ceiling is absolute.
// Admitting every established series unconditionally and topping up with new
// ones would let the table grow by the ceiling each tick until it reached the
// peer's whole series count, making the exposition's size the bound rather than
// ours. Established series still take precedence: they fill the table first, so
// a burst of new fingerprints cannot displace the ones actually reporting.
func (r *Rater) Observe(series []Series) []Series {
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	at := now()

	ceiling := r.MaxTracked
	if ceiling <= 0 {
		ceiling = MaxTrackedSeries
	}

	fps := make([]string, len(series))
	for i, s := range series {
		fps[i] = s.Fingerprint()
	}

	size := len(series)
	if size > ceiling {
		size = ceiling
	}
	next := make(map[string]observation, size)
	out := make([]Series, 0, size)
	r.refused = false

	for i, s := range series {
		if len(next) >= ceiling {
			break
		}
		if _, seen := r.prev[fps[i]]; !seen {
			continue
		}
		next[fps[i]] = observation{value: s.Value, at: at}
	}
	for i, s := range series {
		if _, carried := next[fps[i]]; carried {
			continue
		}
		if len(next) >= ceiling {
			r.refused = true
			continue
		}
		next[fps[i]] = observation{value: s.Value, at: at}
	}

	for i, s := range series {
		if _, admitted := next[fps[i]]; !admitted {
			continue
		}
		prev, seen := r.prev[fps[i]]
		if !seen {
			continue
		}
		elapsed := at.Sub(prev.at).Seconds()
		if elapsed <= 0 || s.Value < prev.value {
			continue
		}

		rated := s
		rated.Value = (s.Value - prev.value) / elapsed
		rated.Kind = KindGauge
		out = append(out, rated)
	}

	r.prev = next
	return out
}

// Tracked reports how many series the Rater currently holds state for.
func (r *Rater) Tracked() int {
	return len(r.prev)
}
