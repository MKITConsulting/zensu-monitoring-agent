package scrape

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestRater() (*Rater, *fakeClock) {
	clock := &fakeClock{t: time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)}
	r := NewRater()
	r.Now = clock.now
	return r, clock
}

func TestObserveYieldsNothingOnFirstScrape(t *testing.T) {
	r, _ := newTestRater()

	got := r.Observe([]Series{counter("c", map[string]string{"pod": "p"}, 100)})

	if len(got) != 0 {
		t.Errorf("first observation must yield no rate, got %+v", got)
	}
	if r.Tracked() != 1 {
		t.Errorf("tracked = %d, want the series to be remembered", r.Tracked())
	}
}

func TestObserveRatesAcrossScrapes(t *testing.T) {
	r, clock := newTestRater()
	labels := map[string]string{"pod": "p"}

	r.Observe([]Series{counter("c", labels, 100)})
	clock.advance(60 * time.Second)
	got := r.Observe([]Series{counter("c", labels, 130)})

	if len(got) != 1 {
		t.Fatalf("expected one rated series, got %d", len(got))
	}
	if want := 0.5; got[0].Value != want {
		t.Errorf("rate = %v, want %v (30 over 60s)", got[0].Value, want)
	}
	if got[0].Kind != KindGauge {
		t.Errorf("kind = %v, want gauge — a rate is instantaneous", got[0].Kind)
	}
	if got[0].Labels["pod"] != "p" || got[0].Name != "c" {
		t.Errorf("identity must survive rating, got %s %v", got[0].Name, got[0].Labels)
	}
}

func TestObserveSkipsOneTickOnCounterReset(t *testing.T) {
	r, clock := newTestRater()
	labels := map[string]string{"pod": "p"}

	r.Observe([]Series{counter("c", labels, 100)})
	clock.advance(60 * time.Second)
	if got := r.Observe([]Series{counter("c", labels, 5)}); len(got) != 0 {
		t.Fatalf("a counter reset must yield no rate, got %+v", got)
	}

	clock.advance(60 * time.Second)
	got := r.Observe([]Series{counter("c", labels, 35)})
	if len(got) != 1 {
		t.Fatalf("the scrape after a reset must rate again, got %d series", len(got))
	}
	if want := 0.5; got[0].Value != want {
		t.Errorf("post-reset rate = %v, want %v", got[0].Value, want)
	}
}

func TestObserveSkipsNonAdvancingClock(t *testing.T) {
	r, _ := newTestRater()
	labels := map[string]string{"pod": "p"}

	r.Observe([]Series{counter("c", labels, 100)})
	got := r.Observe([]Series{counter("c", labels, 130)})

	if len(got) != 0 {
		t.Errorf("a zero elapsed interval must not produce a division, got %+v", got)
	}
}

func TestObserveTracksSeriesIndependently(t *testing.T) {
	r, clock := newTestRater()
	a := map[string]string{"pod": "a"}
	b := map[string]string{"pod": "b"}

	r.Observe([]Series{counter("c", a, 100), counter("c", b, 200)})
	clock.advance(10 * time.Second)
	got := r.Observe([]Series{counter("c", a, 110), counter("c", b, 500)})

	if len(got) != 2 {
		t.Fatalf("expected both series rated, got %d", len(got))
	}
	byPod := map[string]float64{}
	for _, s := range got {
		byPod[s.Labels["pod"]] = s.Value
	}
	if byPod["a"] != 1 {
		t.Errorf("pod a rate = %v, want 1", byPod["a"])
	}
	if byPod["b"] != 30 {
		t.Errorf("pod b rate = %v, want 30", byPod["b"])
	}
}

func TestObserveDropsStateForVanishedSeries(t *testing.T) {
	r, clock := newTestRater()
	a := map[string]string{"pod": "a"}
	b := map[string]string{"pod": "b"}

	r.Observe([]Series{counter("c", a, 1), counter("c", b, 1)})
	if r.Tracked() != 2 {
		t.Fatalf("tracked = %d, want 2", r.Tracked())
	}

	clock.advance(10 * time.Second)
	r.Observe([]Series{counter("c", a, 2)})

	if r.Tracked() != 1 {
		t.Errorf("tracked = %d, want 1 — a pod that vanished must not be retained", r.Tracked())
	}
}

func TestObserveDropsPredecessorAfterSeriesVanishes(t *testing.T) {
	r, clock := newTestRater()
	labels := map[string]string{"pod": "p"}

	r.Observe([]Series{counter("c", labels, 100)})
	clock.advance(10 * time.Second)
	r.Observe(nil)
	clock.advance(10 * time.Second)

	if got := r.Observe([]Series{counter("c", labels, 300)}); len(got) != 0 {
		t.Errorf("a series that vanished and came back has no predecessor, got %+v", got)
	}

	clock.advance(10 * time.Second)
	got := r.Observe([]Series{counter("c", labels, 320)})
	if len(got) != 1 {
		t.Fatalf("the tick after the series reappeared must rate again, got %d series", len(got))
	}
	if want := 2.0; got[0].Value != want {
		t.Errorf("resumed rate = %v, want %v (20 over 10s)", got[0].Value, want)
	}
}

// TestObserveCeilingIsAbsolute pins that the tracking table cannot grow past its
// ceiling. Admitting every established series unconditionally and then topping
// up with new ones would let the table converge on the peer's whole series
// count, which is the peer's number, not ours.
// It also contracts the ceiling between ticks, which is what makes the FIRST
// pass load-bearing: without its own break, two established rows would both be
// admitted under a ceiling of one.
func TestObserveCeilingIsAbsolute(t *testing.T) {
	r, clock := newTestRater()
	r.MaxTracked = 2

	rows := func(names ...string) []Series {
		out := make([]Series, 0, len(names))
		for _, n := range names {
			out = append(out, counter("c", map[string]string{"pod": n}, 1))
		}
		return out
	}

	r.Observe(rows("a", "b", "c", "d"))
	if r.Tracked() != 2 {
		t.Fatalf("tracked = %d, want 2", r.Tracked())
	}
	if !r.RefusedSeries() {
		t.Error("refusing series must be reported")
	}

	clock.advance(10 * time.Second)
	r.Observe(rows("x", "y", "a", "b"))
	if r.Tracked() != 2 {
		t.Errorf("tracked = %d after a churning scrape, want 2 — the ceiling must be absolute", r.Tracked())
	}

	r.MaxTracked = 1
	clock.advance(10 * time.Second)
	r.Observe(rows("a", "b"))
	if r.Tracked() != 1 {
		t.Errorf("tracked = %d, want 1 — established series must also obey the ceiling", r.Tracked())
	}
	if !r.RefusedSeries() {
		t.Error("dropping an established series to the ceiling must be reported")
	}
}

// TestObserveKeepsEstablishedSeriesUnderPressure pins the precedence rule: a
// burst of new fingerprints must not displace the series that are reporting.
func TestObserveKeepsEstablishedSeriesUnderPressure(t *testing.T) {
	r, clock := newTestRater()
	r.MaxTracked = 1
	established := map[string]string{"pod": "keeper"}

	r.Observe([]Series{counter("c", established, 100)})
	clock.advance(60 * time.Second)

	got := r.Observe([]Series{
		counter("c", map[string]string{"pod": "flood-1"}, 1),
		counter("c", map[string]string{"pod": "flood-2"}, 1),
		counter("c", established, 160),
	})
	if len(got) != 1 {
		t.Fatalf("the established series must still rate, got %d rated", len(got))
	}
	if got[0].Labels["pod"] != "keeper" || got[0].Value != 1 {
		t.Errorf("rated %+v, want the keeper at 1/s", got[0])
	}
	if !r.RefusedSeries() {
		t.Error("the flood must be reported as refused")
	}
}

func TestRefusedSeriesResetsWhenBelowCeiling(t *testing.T) {
	r, clock := newTestRater()
	r.MaxTracked = 1

	r.Observe([]Series{
		counter("c", map[string]string{"pod": "a"}, 1),
		counter("c", map[string]string{"pod": "b"}, 1),
	})
	if !r.RefusedSeries() {
		t.Fatal("expected a refusal")
	}
	clock.advance(10 * time.Second)
	r.Observe([]Series{counter("c", map[string]string{"pod": "a"}, 2)})
	if r.RefusedSeries() {
		t.Error("RefusedSeries must describe the last scrape, not stay latched")
	}
}
