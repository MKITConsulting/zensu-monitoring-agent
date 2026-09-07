package agent

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	obs "github.com/MKITConsulting/zensu-monitoring-agent/internal/metrics"
	"github.com/MKITConsulting/zensu-monitoring-agent/internal/scrape"
)

// DefaultSlugLabel is the exposition label whose value is taken as the Zensu
// service slug. Operators project it in the collector with the k8sattributes
// processor from the zensu.dev/service annotation their Deployments already
// carry for uptime discovery.
const DefaultSlugLabel = "zensu_service"

// DefaultScrapeTimeout re-exports the scrape package's per-request timeout so
// cmd has one value to fall back on without importing internal/scrape directly
// and without restating the number.
const DefaultScrapeTimeout = scrape.DefaultTimeout

// ExpositionConfig configures the exposition-backed metric source.
type ExpositionConfig struct {
	URL          string
	SlugLabel    string
	CPUMetric    string
	MemoryMetric string
	Timeout      time.Duration
	// MaxBytes caps the exposition body. Zero uses scrape.DefaultMaxBytes. It is
	// configurable so an operator hitting the agent's memory limit can lower it
	// without waiting for a release.
	MaxBytes int64
}

type expositionSource struct {
	client    *scrape.Client
	rater     *scrape.Rater
	resolver  scrape.Resolver
	slugLabel string
	log       *slog.Logger
	metrics   *obs.Metrics

	// These latch the diagnostics for a permanently mismatched exposition, so a
	// standing misconfiguration costs one line rather than one per heartbeat.
	// unmatchedWarned is cleared on the first tick that resolves either role, so
	// a pipeline that is repaired and later breaks again is reported afresh.
	unmatchedWarned  atomic.Bool
	cpuUnresolved    atomic.Bool
	memoryUnresolved atomic.Bool
	cpuUnrateable    atomic.Bool
	memoryUnrateable atomic.Bool
	cpuIncomplete    atomic.Bool
	memoryIncomplete atomic.Bool
	raterFullWarned  atomic.Bool
}

// newExpositionSource builds a MetricSource that scrapes a Prometheus
// exposition once per tick. It is unexported because the source is stateful —
// it remembers the previous observation of every counter series — and rebuilding
// it per tick would silently stop every counter from ever rating. NewMetricSource
// is the only construction path, and it keeps the source for the agent's
// lifetime.
func newExpositionSource(cfg ExpositionConfig, log *slog.Logger, metrics *obs.Metrics) *expositionSource {
	if log == nil {
		log = slog.Default()
	}
	slugLabel := cfg.SlugLabel
	if slugLabel == "" {
		slugLabel = DefaultSlugLabel
	}
	client := scrape.NewClient(cfg.URL, cfg.Timeout)
	if cfg.MaxBytes > 0 {
		client.MaxBytes = cfg.MaxBytes
		if effective := client.EffectiveMaxBytes(); effective != cfg.MaxBytes {
			log.Warn("scrape body cap refused: it exceeds what this pod's heap limit affords, raise GOMEMLIMIT and resources.limits.memory with it",
				"configured_bytes", cfg.MaxBytes, "effective_bytes", effective)
		}
	}
	return &expositionSource{
		client:    client,
		rater:     scrape.NewRater(),
		resolver:  scrape.Resolver{CPUOverride: cfg.CPUMetric, MemoryOverride: cfg.MemoryMetric},
		slugLabel: slugLabel,
		log:       log,
		metrics:   metrics,
	}
}

func (s *expositionSource) Name() string { return SourceExposition }

// Samples scrapes once and attributes every recognized series to a service.
func (s *expositionSource) Samples(ctx context.Context, targets []ServiceTarget) (map[string][]MetricSample, error) {
	started := time.Now()
	series, err := s.client.Fetch(ctx, s.resolver.CandidateNames())
	s.metrics.RecordScrape(err == nil, time.Since(started))
	if err != nil {
		return nil, err
	}

	cpu, hasCPU := s.resolver.Select(scrape.RoleCPU, series)
	memory, hasMemory := s.resolver.Select(scrape.RoleMemory, series)

	var selections []scrape.Selection
	if hasCPU {
		selections = append(selections, cpu)
		s.cpuUnresolved.Store(false)
	} else {
		s.warnRoleUnresolved(scrape.RoleCPU, s.resolver.CPUOverride)
	}
	if hasMemory {
		selections = append(selections, memory)
		s.memoryUnresolved.Store(false)
	} else {
		s.warnRoleUnresolved(scrape.RoleMemory, s.resolver.MemoryOverride)
	}

	if len(selections) == 0 {
		if s.unmatchedWarned.CompareAndSwap(false, true) {
			s.log.Warn("exposition carries none of the known CPU/memory metrics; sending heartbeats without resource metrics",
				"series", len(series))
		}
		return nil, nil
	}
	s.unmatchedWarned.Store(false)

	index := newSlugIndex(targets)
	firstObservation := s.rater.Tracked() == 0
	rated := s.rateCounters(selections)

	out := make(map[string][]MetricSample, len(targets))
	for _, sel := range selections {
		s.reduce(out, sel, rated, index, firstObservation)
	}
	return out, nil
}

// warnRoleUnresolved reports a role the exposition never supplied. Without it a
// half-resolved exposition is completely silent: the other role still maps, so
// the services-mapped gauge looks healthy while one metric is simply missing.
// The commonest cause is an override that names a family the exposition spells
// differently, since overrides match verbatim while the built-in candidates
// tolerate unit suffixes.
func (s *expositionSource) warnRoleUnresolved(role scrape.Role, override string) {
	latch := &s.cpuUnresolved
	if role == scrape.RoleMemory {
		latch = &s.memoryUnresolved
	}
	if !latch.CompareAndSwap(false, true) {
		return
	}
	if override != "" {
		s.log.Warn("configured metric override matched nothing in the exposition; this role will report no samples",
			"role", role.String(), "override", override)
		return
	}
	s.log.Warn("exposition carries none of the known metrics for this role", "role", role.String())
}

// rateCounters converts every rateable counter series in a single Rater pass.
// Rating one selection at a time would be wrong: Observe rebuilds its state from
// the series it is handed, so a second call would discard the first's
// predecessors and no counter would ever produce a rate.
//
// Every row of a rateable selection is fed in, including rows that currently
// attribute to no service. Filtering by attribution would look like a memory
// bound but is not one — a peer that knows a slug can stamp it on unbounded rows
// — and it costs correctness: a service whose pod listing fails for one tick
// would lose its predecessors and then report nothing for a second tick too. The
// Rater's own ceiling is the bound.
//
// Selection.Series already carries only the summable rows, so the Rater and
// reduce see the same set by construction rather than by agreement. A pod-level
// row that was a rollup last tick and is not this one therefore arrives with no
// predecessor and withholds its service's role for that one tick, which is the
// same treatment any newly appearing series gets.
//
// A tick with NO rateable selection returns before touching the Rater. Observe
// rebuilds its table from what it is handed, so passing it nothing would discard
// every predecessor and cost the service a second tick of CPU once the family
// comes back — the same two-tick loss the paragraph above refuses for the
// attribution case. The gate is "no rateable selection", not "no rows", so a
// resolved counter family whose rows all vanish still prunes.
func (s *expositionSource) rateCounters(selections []scrape.Selection) map[string]float64 {
	var counters []scrape.Series
	rateable := false
	for _, sel := range selections {
		if sel.Rateable() {
			rateable = true
			counters = append(counters, sel.Series...)
		}
	}
	if !rateable {
		return nil
	}

	rated := make(map[string]float64, len(counters))
	for _, r := range s.rater.Observe(counters) {
		rated[r.Fingerprint()] = r.Value
	}
	if s.rater.RefusedSeries() {
		if s.raterFullWarned.CompareAndSwap(false, true) {
			s.log.Warn("exposition carries more counter series than the agent will track; some rates will be missing",
				"limit", scrape.MaxTrackedSeries)
		}
	} else {
		s.raterFullWarned.Store(false)
	}
	return rated
}

// reduce sums a selection's series per service slug and appends one sample per
// service, scaled to the backend's unit.
//
// A counter selection is all-or-nothing per service. Rates are withheld per
// SERIES — a pod's first observation, or a counter that just reset — so summing
// only the rows that did rate would report a fraction of a multi-pod service's
// CPU as though it were the whole, which is worse than reporting nothing: the
// operator's mapped-services gauge would still look healthy.
//
// firstObservation suppresses the withheld-service warning on the tick that
// first observes a counter, where every series is incomplete by construction.
// Reporting it would spend the latch on an artifact and hide the real case for
// the rest of the process's life.
func (s *expositionSource) reduce(out map[string][]MetricSample, sel scrape.Selection, rated map[string]float64, index slugIndex, firstObservation bool) {
	key := metricKeyFor(sel.Role)

	unrateable := s.roleLatch(sel.Role, latchUnrateable)
	if sel.Kind == scrape.KindCounter && !sel.Rateable() {
		if unrateable.CompareAndSwap(false, true) {
			s.log.Warn("selected metric is a cumulative counter this role cannot rate; the role will report no samples",
				"role", sel.Role.String(), "metric", sel.Candidate.Name)
		}
		return
	}
	unrateable.Store(false)

	totals := map[string]float64{}
	contributed := map[string]bool{}
	incomplete := map[string]bool{}
	dropped := 0

	for _, row := range sel.Series {
		slug, ok := index.resolve(row, s.slugLabel)
		if !ok {
			dropped++
			continue
		}
		value := row.Value
		if sel.Rateable() {
			value, ok = rated[row.Fingerprint()]
			if !ok {
				incomplete[slug] = true
				continue
			}
		}
		totals[slug] += value
		contributed[slug] = true
	}

	if dropped > 0 {
		s.log.Debug("dropped exposition rows that map to no tracked service",
			"metric", sel.Candidate.Name, "rows", dropped)
	}
	if sel.Excluded > 0 {
		s.log.Debug("excluded exposition rows as pod-level rollups or pause containers",
			"metric", sel.Candidate.Name, "rows", sel.Excluded)
	}

	incompleteLatch := s.roleLatch(sel.Role, latchIncomplete)
	switch {
	case len(incomplete) == 0:
		incompleteLatch.Store(false)
	case !firstObservation && incompleteLatch.CompareAndSwap(false, true):
		s.log.Warn("withholding a role for services whose pods did not all produce a rate; a partial sum would read as a real measurement",
			"role", sel.Role.String(), "services", len(incomplete))
	}

	for slug := range contributed {
		if incomplete[slug] {
			continue
		}
		value := totals[slug] * sel.Candidate.Scale
		if !scrape.IsFinite(value) {
			s.log.Warn("dropping non-finite resource sample", "service", slug, "metric", key)
			continue
		}
		out[slug] = append(out[slug], MetricSample{Key: key, Value: value})
	}
}

// metricKeyFor maps a resolver role to the backend registry key. The wire
// vocabulary is defined once, next to MetricSample in types.go, and does not
// travel into the exposition-side package.
func metricKeyFor(role scrape.Role) string {
	if role == scrape.RoleMemory {
		return MetricMemoryBytes
	}
	return MetricCPUMillicores
}

// latchKind selects which per-role one-shot warning a lookup refers to.
type latchKind int

const (
	latchUnrateable latchKind = iota
	latchIncomplete
)

// roleLatch returns the latch for one role and condition. Latches are per role
// because the roles are reduced in sequence: a single shared flag would be
// cleared by whichever role ran first, so the other role's standing
// misconfiguration would warn on every tick.
func (s *expositionSource) roleLatch(role scrape.Role, kind latchKind) *atomic.Bool {
	memory := role == scrape.RoleMemory
	if kind == latchIncomplete {
		if memory {
			return &s.memoryIncomplete
		}
		return &s.cpuIncomplete
	}
	if memory {
		return &s.memoryUnrateable
	}
	return &s.cpuUnrateable
}

// slugIndex resolves an exposition sample to a tracked service slug.
type slugIndex struct {
	known           map[string]bool
	byNamespacedPod map[string]string
	byPod           map[string]string
}

func newSlugIndex(targets []ServiceTarget) slugIndex {
	idx := slugIndex{
		known:           make(map[string]bool, len(targets)),
		byNamespacedPod: map[string]string{},
		byPod:           map[string]string{},
	}
	for _, t := range targets {
		idx.known[t.Slug] = true
		for _, name := range t.PodNames {
			if key, ok := scrape.PodKey(t.Namespace, name); ok {
				idx.byNamespacedPod[key] = t.Slug
			}
			if existing, seen := idx.byPod[name]; seen && existing != t.Slug {
				idx.byPod[name] = ""
				continue
			}
			idx.byPod[name] = t.Slug
		}
	}
	return idx
}

// resolve tries the explicit slug label first, then pod+namespace attribution.
// A pod name that is ambiguous across namespaces resolves only when the sample
// also carries a namespace label.
func (i slugIndex) resolve(row scrape.Series, slugLabel string) (string, bool) {
	if slug := row.Labels[slugLabel]; slug != "" {
		if i.known[slug] {
			return slug, true
		}
		return "", false
	}

	pod := scrape.PodLabel(row.Labels)
	if pod == "" {
		return "", false
	}
	if key, ok := scrape.PodKey(scrape.NamespaceLabel(row.Labels), pod); ok {
		slug, found := i.byNamespacedPod[key]
		return slug, found && slug != ""
	}
	slug, ok := i.byPod[pod]
	return slug, ok && slug != ""
}
