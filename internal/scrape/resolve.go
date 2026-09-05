package scrape

import "strings"

// Role is one of the two resource metrics the Zensu metric registry recognizes.
type Role int

const (
	// RoleCPU is the CPU role. internal/agent owns the wire key it maps to.
	RoleCPU Role = iota
	// RoleMemory is the memory role. internal/agent owns its wire key too.
	RoleMemory
)

// String names the role for log and error messages.
func (r Role) String() string {
	if r == RoleMemory {
		return "memory"
	}
	return "cpu"
}

// Candidate is one exposition metric the agent knows how to read. Name is the
// BASE name: the Prometheus exporter of a collector appends unit suffixes when
// add_metric_suffixes is on (the default), and that setting is not ours to
// control, so matching normalizes both sides.
//
// Scale converts the post-rating value to the backend's unit. A counter is
// rated to a per-second value before Scale is applied, which is why a CPU
// gauge in cores and a CPU counter in seconds share the same factor: rating
// seconds-per-second yields cores.
// Exact suppresses base-name normalization, so the candidate matches one
// exposition family and not every family sharing its base. Operator overrides
// set it; the built-in candidates do not, because tolerating the exporter's unit
// suffixes is the whole point of the table.
type Candidate struct {
	Name  string
	Scale float64
	Exact bool
}

// cpuCandidates and memoryCandidates are probed in order; the first whose base
// name is present in the exposition wins. The kubeletstats names come first
// because they are pod-scoped and need no further aggregation; the cAdvisor
// names are container-scoped, which the per-service summation collapses anyway.
var (
	cpuCandidates = []Candidate{
		{Name: "k8s_pod_cpu_usage", Scale: 1000},
		{Name: "k8s_pod_cpu_time", Scale: 1000},
		{Name: "container_cpu_usage", Scale: 1000},
	}
	memoryCandidates = []Candidate{
		{Name: "k8s_pod_memory_working_set", Scale: 1},
		{Name: "container_memory_working_set", Scale: 1},
	}
)

// unitSuffixes are stripped repeatedly from the end of a metric name so that
// k8s_pod_memory_working_set and k8s_pod_memory_working_set_bytes, or
// container_cpu_usage and container_cpu_usage_seconds_total, compare equal.
//
// Every entry names the same quantity under another unit or accumulation.
// "_ratio" is deliberately absent: a fraction of a limit is a DIFFERENT
// quantity, and folding it onto the absolute family's base would let a pod at
// 62 percent of its limit report 0.62 bytes, or 620 millicores.
var unitSuffixes = []string{"_total", "_bytes", "_seconds"}

// NormalizeName reduces a metric name to its base form by stripping trailing
// unit suffixes until none applies.
func NormalizeName(name string) string {
	for changed := true; changed; {
		changed = false
		for _, suffix := range unitSuffixes {
			if trimmed := strings.TrimSuffix(name, suffix); trimmed != name && trimmed != "" {
				name = trimmed
				changed = true
			}
		}
	}
	return name
}

// Selection is the resolved metric for one role, together with every series
// that carries it. Role travels WITH the selection rather than beside it: a
// caller that had to pass the two separately could pair a memory selection with
// the CPU role, which is exactly the mistake the rating rules exist to prevent.
// Series holds only the rows a caller may sum. Select applies the row policy
// once, so there is no second, wider set for a later consumer to reach for by
// mistake — the agreement between everything that reads a Selection is a
// property of the type rather than a convention each call site must remember.
// Excluded counts what the policy removed, so a service that disappears into it
// is observable rather than silent.
type Selection struct {
	Candidate Candidate
	Role      Role
	Kind      Kind
	Series    []Series
	Excluded  int
}

// Rateable reports whether this selection may be differenced into a per-second
// rate. Only CPU may: rating cumulative seconds per second yields cores, which
// the role's factor converts to millicores. A cumulative MEMORY counter rated
// the same way would yield bytes per second and ship under the memory key as if
// it were a byte count, so it is refused.
func (s Selection) Rateable() bool {
	return s.Kind == KindCounter && s.Role == RoleCPU
}

// summable applies the row policy to a matched family and returns the rows a
// caller may sum, plus how many it removed. Three rules, each its own predicate:
//
//   - A container-less row whose pod also has per-container rows is that pod's
//     rollup total; summing both counts the pod twice. The decision comes from
//     the rows themselves, never from the candidate's provenance, because an
//     operator override naming a container-scoped family carries nothing the
//     resolver could recognize.
//   - cAdvisor's pause pseudo-container is neither a workload container nor a
//     pod total, so it is dropped outright.
//   - Grouping is per POD, not per selection: an exposition may carry container
//     rows for one pod and only a pod-level row for another.
//
// Rows are grouped by namespace+pod when they carry both labels and by pod name
// alone when they carry no namespace, and the two sets never cross. Keeping them
// apart is what stops a pod name repeated across namespaces from deleting
// another namespace's only row, while still de-duplicating an exposition that
// projects no namespace at all — which the pod-only slug fallback supports and
// which would otherwise double every pod that has both shapes.
func summable(rows []Series) ([]Series, int) {
	rollupNamespaced := map[string]bool{}
	rollupPodOnly := map[string]bool{}
	for _, row := range rows {
		if !isContainerRow(row) {
			continue
		}
		if key, ok := row.podRef(); ok {
			rollupNamespaced[key] = true
			continue
		}
		if pod := PodLabel(row.Labels); pod != "" && NamespaceLabel(row.Labels) == "" {
			rollupPodOnly[pod] = true
		}
	}

	out := make([]Series, 0, len(rows))
	for _, row := range rows {
		if isPauseRow(row) || isSupersededRollup(row, rollupNamespaced, rollupPodOnly) {
			continue
		}
		out = append(out, row)
	}
	return out, len(rows) - len(out)
}

// isPauseRow reports whether row measures cAdvisor's pause pseudo-container.
func isPauseRow(row Series) bool {
	return ContainerLabel(row.Labels) == pauseContainer
}

// isContainerRow reports whether row measures one real workload container.
func isContainerRow(row Series) bool {
	name := ContainerLabel(row.Labels)
	return name != "" && name != pauseContainer
}

// isSupersededRollup reports whether row is a pod total the selection's own
// per-container rows already cover.
func isSupersededRollup(row Series, namespaced, podOnly map[string]bool) bool {
	if ContainerLabel(row.Labels) != "" {
		return false
	}
	if key, ok := row.podRef(); ok {
		return namespaced[key]
	}
	pod := PodLabel(row.Labels)
	return pod != "" && NamespaceLabel(row.Labels) == "" && podOnly[pod]
}

// podRef identifies the pod a row belongs to. ok is false when the row does not
// name both a namespace and a pod, in which case it must not be grouped with
// other rows at all.
func (s Series) podRef() (string, bool) {
	return PodKey(NamespaceLabel(s.Labels), PodLabel(s.Labels))
}

// podLabelNames, namespaceLabelNames and containerLabelNames are the spellings a
// row may use to identify itself. Receivers disagree: OTel semantic conventions
// produce k8s_pod_name, k8s_namespace_name and k8s_container_name, cAdvisor
// produces pod, namespace and container. The container spelling tolerates both
// for the same reason as the other two: reading only one would make every
// per-container row look container-less to the rollup check, which then sums the
// pod-level total on top of its own containers.
var (
	podLabelNames       = []string{"pod", "k8s_pod_name"}
	namespaceLabelNames = []string{"namespace", "k8s_namespace_name"}
	containerLabelNames = []string{"container", "k8s_container_name"}
)

// pauseContainer is cAdvisor's pseudo-container for a pod's infrastructure
// container. Its rows are neither a workload container nor a pod total, so they
// are dropped outright: summing them would add the pause container's usage to
// every pod on the cAdvisor path, and letting one stand in for a real container
// row would suppress the pod-level row of a pod that has no other rows at all.
// The name cannot collide with a real container, because Kubernetes container
// names are RFC 1123 labels and those are lowercase.
const pauseContainer = "POD"

// ContainerLabel returns the container name a row carries, whichever spelling it
// uses. An empty result means the row is pod-scoped rather than per-container.
func ContainerLabel(labels map[string]string) string {
	return firstLabel(labels, containerLabelNames)
}

// PodLabel returns the pod name a row carries, whichever spelling it uses.
func PodLabel(labels map[string]string) string { return firstLabel(labels, podLabelNames) }

// NamespaceLabel returns the namespace a row carries, whichever spelling it uses.
func NamespaceLabel(labels map[string]string) string {
	return firstLabel(labels, namespaceLabelNames)
}

// PodKey builds the single key under which a namespace+pod pair is looked up.
// Both sides of the seam use it, so "how a pod is named" is defined once: the
// resolver's rollup grouping and the agent's slug index cannot drift apart into
// two encodings with two separators.
func PodKey(namespace, pod string) (string, bool) {
	if namespace == "" || pod == "" {
		return "", false
	}
	return namespace + "\x1f" + pod, true
}

func firstLabel(labels map[string]string, names []string) string {
	for _, n := range names {
		if v := labels[n]; v != "" {
			return v
		}
	}
	return ""
}

// Resolver picks which exposition metric supplies each role. An override names
// a metric EXACTLY — it is matched verbatim, without the unit-suffix
// normalization the built-in candidates use, so it selects one family and not a
// whole suffix group. Its value is still scaled by the role's default factor, so
// an override is expected to expose CPU in cores (or cumulative seconds) and
// memory in bytes.
type Resolver struct {
	CPUOverride    string
	MemoryOverride string
}

// Select returns the winning metric for role, or ok=false when the exposition
// carries none of the candidates.
//
// Matching is by normalized base name, so a suffixed and an unsuffixed spelling
// of the same metric resolve together — but normalization can also map two
// DIFFERENT families onto one base (container_cpu_usage, a gauge in cores, and
// container_cpu_usage_seconds_total, a cumulative counter). Mixing those in one
// Selection would hand the caller rows of one kind labelled as the other, so the
// winning family is decided by exact exposition name and rows from any other
// family sharing the base are left out. Every row of that one family is
// returned, so a container-scoped metric still contributes all of its rows.
func (r Resolver) Select(role Role, all []Series) (Selection, bool) {
	for _, candidate := range r.candidates(role) {
		matched, kind, ok := matchFamily(candidate, all)
		if ok {
			rows, excluded := summable(matched)
			return Selection{Candidate: candidate, Role: role, Kind: kind, Series: rows, Excluded: excluded}, true
		}
	}
	return Selection{}, false
}

// matchFamily collects the rows of a single exposition family for candidate. An
// override matches only its exact name; a built-in candidate matches on the
// normalized base but then narrows to the first exact name it saw, so the result
// is always homogeneous in Kind.
//
// When several suffixed spellings share the base — an exposition carrying both
// container_cpu_usage and container_cpu_usage_seconds_total — the candidate's
// own spelling wins, and only failing that does the first one the peer emitted.
// Without the exact-match preference the choice would be decided by the order
// the scraped peer happens to write its families in, which is not ours to
// depend on.
func matchFamily(candidate Candidate, all []Series) ([]Series, Kind, bool) {
	var winner string
	for _, s := range all {
		if candidate.Exact {
			if s.Name != candidate.Name {
				continue
			}
		} else if NormalizeName(s.Name) != NormalizeName(candidate.Name) {
			continue
		}
		if s.Name == candidate.Name {
			winner = s.Name
			break
		}
		if winner == "" {
			winner = s.Name
		}
	}
	if winner == "" {
		return nil, KindUnsupported, false
	}

	var matched []Series
	kind := KindUnsupported
	for _, s := range all {
		if s.Name != winner {
			continue
		}
		matched = append(matched, s)
		kind = s.Kind
	}
	return matched, kind, len(matched) > 0
}

func (r Resolver) candidates(role Role) []Candidate {
	override, defaults := r.CPUOverride, cpuCandidates
	if role == RoleMemory {
		override, defaults = r.MemoryOverride, memoryCandidates
	}
	if override != "" {
		return []Candidate{{Name: override, Scale: defaults[0].Scale, Exact: true}}
	}
	return defaults
}

// CandidateNames returns the normalized base names the resolver can match, so a
// caller can filter an exposition down to them before materializing series.
func (r Resolver) CandidateNames() map[string]bool {
	keep := map[string]bool{}
	for _, role := range []Role{RoleCPU, RoleMemory} {
		for _, c := range r.candidates(role) {
			keep[NormalizeName(c.Name)] = true
		}
	}
	return keep
}
