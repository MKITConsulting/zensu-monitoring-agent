package scrape

import "testing"

func gauge(name string, labels map[string]string, v float64) Series {
	return Series{Name: name, Labels: labels, Value: v, Kind: KindGauge}
}

func counter(name string, labels map[string]string, v float64) Series {
	return Series{Name: name, Labels: labels, Value: v, Kind: KindCounter}
}

// TestNormalizeName also pins that a ratio does NOT fold onto the absolute
// family's base: it is a different quantity, not the same one under another unit.
func TestNormalizeName(t *testing.T) {
	cases := []struct{ in, want string }{
		{"k8s_pod_cpu_usage", "k8s_pod_cpu_usage"},
		{"k8s_pod_memory_working_set", "k8s_pod_memory_working_set"},
		{"k8s_pod_memory_working_set_bytes", "k8s_pod_memory_working_set"},
		{"container_cpu_usage_seconds_total", "container_cpu_usage"},
		{"k8s_pod_cpu_time_seconds", "k8s_pod_cpu_time"},
		{"container_memory_working_set_bytes", "container_memory_working_set"},
		{"node_cpu_ratio", "node_cpu_ratio"},
		{"container_memory_working_set_ratio", "container_memory_working_set_ratio"},
		{"plain", "plain"},
		{"_bytes", "_bytes"},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			if got := NormalizeName(c.in); got != c.want {
				t.Errorf("NormalizeName(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestSelectMatchesSuffixedAndUnsuffixedNames(t *testing.T) {
	cases := []struct {
		name         string
		exposed      string
		role         Role
		kind         Kind
		wantScale    float64
		wantRateable bool
	}{
		{"kubeletstats cpu gauge", "k8s_pod_cpu_usage", RoleCPU, KindGauge, 1000, false},
		{"kubeletstats cpu time counter", "k8s_pod_cpu_time_seconds", RoleCPU, KindCounter, 1000, true},
		{"kubeletstats memory unsuffixed", "k8s_pod_memory_working_set", RoleMemory, KindGauge, 1, false},
		{"kubeletstats memory suffixed", "k8s_pod_memory_working_set_bytes", RoleMemory, KindGauge, 1, false},
		{"cadvisor cpu suffixed counter", "container_cpu_usage_seconds_total", RoleCPU, KindCounter, 1000, true},
		{"cadvisor memory suffixed", "container_memory_working_set_bytes", RoleMemory, KindGauge, 1, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			row := gauge(c.exposed, map[string]string{"pod": "p"}, 1)
			if c.kind == KindCounter {
				row = counter(c.exposed, map[string]string{"pod": "p"}, 1)
			}

			sel, ok := Resolver{}.Select(c.role, []Series{row})
			if !ok {
				t.Fatalf("%s should resolve for role %s", c.exposed, c.role)
			}
			if len(sel.Series) != 1 {
				t.Errorf("matched %d series, want 1", len(sel.Series))
			}
			if sel.Candidate.Scale != c.wantScale {
				t.Errorf("scale = %v, want %v", sel.Candidate.Scale, c.wantScale)
			}
			if sel.Kind != c.kind {
				t.Errorf("kind = %v, want %v", sel.Kind, c.kind)
			}
			if got := sel.Rateable(); got != c.wantRateable {
				t.Errorf("Rateable() = %v, want %v", got, c.wantRateable)
			}
		})
	}
}

// TestMatchFamilyPrefersTheCandidateSpelling pins that the winner is not decided
// by the order the peer writes its families in. Both spellings normalize onto
// container_cpu_usage, and the suffixed one is emitted first.
func TestMatchFamilyPrefersTheCandidateSpelling(t *testing.T) {
	all := []Series{
		counter("container_cpu_usage_seconds_total", map[string]string{"pod": "p"}, 100),
		gauge("container_cpu_usage", map[string]string{"pod": "p"}, 0.5),
	}

	sel, ok := Resolver{}.Select(RoleCPU, all)
	if !ok {
		t.Fatal("expected a CPU selection")
	}
	if sel.Series[0].Name != "container_cpu_usage" {
		t.Errorf("matched %q, want the candidate's own spelling regardless of peer order", sel.Series[0].Name)
	}
	if sel.Kind != KindGauge {
		t.Errorf("kind = %v, want gauge — the exact family, not its suffixed sibling", sel.Kind)
	}
}

// TestSelectPrefersCPUTimeOverCadvisor pins the middle rung of the CPU table.
// Without it the k8s_pod_cpu_time candidate could be deleted and every test
// would still pass, because the cAdvisor name below it matches the same role.
func TestSelectPrefersCPUTimeOverCadvisor(t *testing.T) {
	all := []Series{
		counter("container_cpu_usage_seconds_total", map[string]string{"pod": "p"}, 100),
		counter("k8s_pod_cpu_time_seconds", map[string]string{"k8s_pod_name": "p"}, 50),
	}

	sel, ok := Resolver{}.Select(RoleCPU, all)
	if !ok {
		t.Fatal("expected a CPU selection")
	}
	if sel.Candidate.Name != "k8s_pod_cpu_time" {
		t.Errorf("candidate = %q, want k8s_pod_cpu_time — kubeletstats outranks cAdvisor", sel.Candidate.Name)
	}
	if sel.Kind != KindCounter || !sel.Rateable() {
		t.Errorf("kind = %v rateable = %v, want a rateable counter", sel.Kind, sel.Rateable())
	}
	if names := (Resolver{}).CandidateNames(); !names[NormalizeName("k8s_pod_cpu_time_seconds")] {
		t.Error("the keep filter must admit the suffixed spelling, or Fetch would drop the family before Select sees it")
	}
}

// TestSelectNarrowsToOneFamily pins that two exposition families sharing a
// normalized base are never mixed into one Selection. Mixing them would label
// cumulative seconds as a gauge and sum them as though they were cores.
func TestSelectNarrowsToOneFamily(t *testing.T) {
	all := []Series{
		gauge("container_cpu_usage", map[string]string{"pod": "p"}, 0.5),
		counter("container_cpu_usage_seconds_total", map[string]string{"pod": "p"}, 1234),
	}

	sel, ok := Resolver{}.Select(RoleCPU, all)
	if !ok {
		t.Fatal("expected a CPU selection")
	}
	if len(sel.Series) != 1 {
		t.Fatalf("selection must hold one family, got %d rows: %+v", len(sel.Series), sel.Series)
	}
	if sel.Kind != KindGauge || sel.Series[0].Value != 0.5 {
		t.Errorf("got kind=%v value=%v, want the gauge family alone", sel.Kind, sel.Series[0].Value)
	}
}

// TestSelectOverrideMatchesExactNameOnly pins that an override selects one
// family verbatim. Under normalized matching the counter below would win by
// decode order, which is what the "verbatim" contract exists to prevent.
func TestSelectOverrideMatchesExactNameOnly(t *testing.T) {
	all := []Series{
		counter("my_cpu_seconds_total", map[string]string{"pod": "p"}, 999),
		gauge("my_cpu", map[string]string{"pod": "p"}, 0.5),
	}

	sel, ok := Resolver{CPUOverride: "my_cpu"}.Select(RoleCPU, all)
	if !ok {
		t.Fatal("the override should resolve")
	}
	if len(sel.Series) != 1 {
		t.Fatalf("override must match one family, got %d rows: %+v", len(sel.Series), sel.Series)
	}
	if sel.Kind != KindGauge || sel.Series[0].Value != 0.5 {
		t.Errorf("got kind=%v value=%v, want only the exactly named family", sel.Kind, sel.Series[0].Value)
	}
}

func TestSelectionCarriesItsRole(t *testing.T) {
	all := []Series{
		gauge("k8s_pod_cpu_usage", map[string]string{"pod": "p"}, 1),
		gauge("k8s_pod_memory_working_set_bytes", map[string]string{"pod": "p"}, 2),
	}
	for _, role := range []Role{RoleCPU, RoleMemory} {
		sel, ok := Resolver{}.Select(role, all)
		if !ok {
			t.Fatalf("%s should resolve", role)
		}
		if sel.Role != role {
			t.Errorf("Role = %v, want %v — the role must travel with the selection", sel.Role, role)
		}
	}
}

func TestSelectionRateableOnlyForCPUCounters(t *testing.T) {
	cases := []struct {
		name string
		sel  Selection
		want bool
	}{
		{"cpu counter", Selection{Role: RoleCPU, Kind: KindCounter}, true},
		{"cpu gauge", Selection{Role: RoleCPU, Kind: KindGauge}, false},
		{"memory counter", Selection{Role: RoleMemory, Kind: KindCounter}, false},
		{"memory gauge", Selection{Role: RoleMemory, Kind: KindGauge}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.sel.Rateable(); got != c.want {
				t.Errorf("Rateable() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestSelectionRollupIsPerPod(t *testing.T) {
	withContainers := gauge("m", map[string]string{"namespace": "prod", "pod": "a", "container": "app"}, 1)
	rollupOfA := gauge("m", map[string]string{"namespace": "prod", "pod": "a"}, 2)
	podOnlyB := gauge("m", map[string]string{"namespace": "prod", "pod": "b"}, 3)

	kept, excluded := summable([]Series{withContainers, rollupOfA, podOnlyB})
	if excluded != 1 {
		t.Errorf("excluded = %d, want 1", excluded)
	}

	var values []float64
	for _, row := range kept {
		values = append(values, row.Value)
	}
	if len(kept) != 2 {
		t.Fatalf("kept %d rows %v, want the container row and pod b", len(kept), values)
	}
	if values[0] != 1 {
		t.Error("a container row is never a rollup")
	}
	if values[1] != 3 {
		t.Error("pod b has no container rows, so its only row must be kept")
	}
	for _, v := range values {
		if v == 2 {
			t.Error("pod a has container rows, so its container-less row repeats their total")
		}
	}
}

// TestSelectionRollupIgnoresUnidentifiedPods pins that a row which does not name
// both a namespace and a pod cannot join another pod's rollup set. Grouping on a
// bare pod name would let one namespace's containers delete another namespace's
// only row.
func TestSelectionRollupIgnoresUnidentifiedPods(t *testing.T) {
	kept, _ := summable([]Series{
		gauge("m", map[string]string{"namespace": "prod", "pod": "web-0", "container": "app"}, 1),
		gauge("m", map[string]string{"pod": "web-0"}, 5),
	})

	if got := len(kept); got != 2 {
		t.Errorf("kept %d rows, want 2 — a row without a namespace must not be treated as another namespace's rollup", got)
	}
}

// TestSelectionRollupAcceptsBothContainerSpellings pins that the rollup
// discriminator tolerates the OTel spelling too. Reading only "container" would
// make every k8s_container_name row look pod-scoped, so the rollup set would stay
// empty and the pod-level total would be summed on top of its own containers.
func TestSelectionRollupAcceptsBothContainerSpellings(t *testing.T) {
	for _, label := range []string{"container", "k8s_container_name"} {
		t.Run(label, func(t *testing.T) {
			kept, _ := summable([]Series{
				gauge("m", map[string]string{"namespace": "prod", "pod": "a", label: "app"}, 1),
				gauge("m", map[string]string{"namespace": "prod", "pod": "a"}, 9),
			})
			if len(kept) != 1 || kept[0].Value != 1 {
				t.Errorf("kept %+v, want only the per-container row", kept)
			}
		})
	}
}

// TestSummableDropsThePauseContainer pins that cAdvisor's pause
// pseudo-container is not summed as a workload container. Treating it as one
// would add its usage to every pod on the cAdvisor path, and would also mark the
// pod's genuine rollup row as superseded when no real container row exists.
func TestSummableDropsThePauseContainer(t *testing.T) {
	kept, excluded := summable([]Series{
		gauge("m", map[string]string{"namespace": "prod", "pod": "a", "container": "POD"}, 7),
		gauge("m", map[string]string{"namespace": "prod", "pod": "a", "container": "app"}, 1),
		gauge("m", map[string]string{"namespace": "prod", "pod": "b", "container": "POD"}, 7),
		gauge("m", map[string]string{"namespace": "prod", "pod": "b"}, 5),
	})
	if excluded != 2 {
		t.Errorf("excluded = %d, want 2 — both pause rows", excluded)
	}

	var values []float64
	for _, row := range kept {
		values = append(values, row.Value)
	}
	for _, v := range values {
		if v == 7 {
			t.Errorf("the pause container must not be summed, kept %v", values)
		}
	}
	if len(values) != 2 || values[0] != 1 || values[1] != 5 {
		t.Errorf("kept %v, want the real container of pod a and the rollup row of pod b", values)
	}
}

// TestSummableDeduplicatesNamespacelessRows pins the pod-only grouping. The slug
// index supports a row that names its pod but no namespace, and README documents
// that fallback, so an exposition in that shape must still have its pod-level
// rollup removed — otherwise every pod carrying both shapes ships at double.
func TestSummableDeduplicatesNamespacelessRows(t *testing.T) {
	kept, excluded := summable([]Series{
		gauge("m", map[string]string{"pod": "api-1", "container": "app"}, 10),
		gauge("m", map[string]string{"pod": "api-1"}, 10),
	})

	if excluded != 1 {
		t.Errorf("excluded = %d, want 1", excluded)
	}
	if len(kept) != 1 || kept[0].Labels["container"] != "app" {
		t.Errorf("kept %+v, want only the container row", kept)
	}
}

// TestSummableKeepsNamespacedAndNamespacelessApart pins that the two grouping
// sets never cross. One namespace's container rows must not delete another
// namespace's only row, which is why the namespaced key exists at all.
func TestSummableKeepsNamespacedAndNamespacelessApart(t *testing.T) {
	kept, _ := summable([]Series{
		gauge("m", map[string]string{"namespace": "prod", "pod": "web-0", "container": "app"}, 1),
		gauge("m", map[string]string{"namespace": "staging", "pod": "web-0"}, 5),
		gauge("m", map[string]string{"pod": "web-0"}, 9),
	})

	var values []float64
	for _, row := range kept {
		values = append(values, row.Value)
	}
	if len(kept) != 3 {
		t.Errorf("kept %v, want all three — no group may reach into another", values)
	}
}

func TestSeriesPodRef(t *testing.T) {
	cases := []struct {
		name   string
		labels map[string]string
		wantOK bool
	}{
		{"cadvisor spelling", map[string]string{"namespace": "prod", "pod": "p"}, true},
		{"otel spelling", map[string]string{"k8s_namespace_name": "prod", "k8s_pod_name": "p"}, true},
		{"pod only", map[string]string{"pod": "p"}, false},
		{"namespace only", map[string]string{"namespace": "prod"}, false},
		{"neither", map[string]string{"container": "app"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, ok := (Series{Labels: c.labels}).podRef(); ok != c.wantOK {
				t.Errorf("ok = %v, want %v", ok, c.wantOK)
			}
		})
	}
}

func TestSelectPrefersEarlierCandidate(t *testing.T) {
	all := []Series{
		counter("container_cpu_usage_seconds_total", map[string]string{"pod": "p"}, 100),
		gauge("k8s_pod_cpu_usage", map[string]string{"k8s_pod_name": "p"}, 0.5),
	}

	sel, ok := Resolver{}.Select(RoleCPU, all)
	if !ok {
		t.Fatal("expected a CPU selection")
	}
	if sel.Candidate.Name != "k8s_pod_cpu_usage" {
		t.Errorf("winner = %q, want the kubeletstats gauge to outrank cAdvisor", sel.Candidate.Name)
	}
	if sel.Kind != KindGauge {
		t.Errorf("kind = %v, want gauge from the winning family", sel.Kind)
	}
}

func TestSelectCollectsEveryMatchingSeries(t *testing.T) {
	all := []Series{
		counter("container_cpu_usage_seconds_total", map[string]string{"pod": "p", "container": "app"}, 10),
		counter("container_cpu_usage_seconds_total", map[string]string{"pod": "p", "container": "sidecar"}, 5),
		gauge("unrelated_metric", map[string]string{"pod": "p"}, 1),
	}

	sel, ok := Resolver{}.Select(RoleCPU, all)
	if !ok {
		t.Fatal("expected a CPU selection")
	}
	if len(sel.Series) != 2 {
		t.Errorf("matched %d series, want both container rows", len(sel.Series))
	}
	if sel.Kind != KindCounter {
		t.Errorf("kind = %v, want counter", sel.Kind)
	}
}

func TestSelectOverrideWinsOverCandidateList(t *testing.T) {
	all := []Series{
		gauge("k8s_pod_cpu_usage", map[string]string{"pod": "p"}, 0.5),
		gauge("my_custom_cpu_cores", map[string]string{"pod": "p"}, 0.9),
	}

	sel, ok := Resolver{CPUOverride: "my_custom_cpu_cores"}.Select(RoleCPU, all)
	if !ok {
		t.Fatal("override should resolve")
	}
	if sel.Candidate.Name != "my_custom_cpu_cores" {
		t.Errorf("winner = %q, want the override", sel.Candidate.Name)
	}
	if sel.Candidate.Scale != 1000 {
		t.Errorf("override scale = %v, want the CPU role default 1000", sel.Candidate.Scale)
	}
	if sel.Series[0].Value != 0.9 {
		t.Errorf("override matched the wrong series (value %v)", sel.Series[0].Value)
	}
}

func TestSelectOverrideMissingFallsThroughToNothing(t *testing.T) {
	all := []Series{gauge("k8s_pod_cpu_usage", map[string]string{"pod": "p"}, 0.5)}

	if _, ok := (Resolver{CPUOverride: "not_exposed"}).Select(RoleCPU, all); ok {
		t.Error("an override that names an absent metric must not silently fall back to the candidate list")
	}
}

func TestSelectReportsMissWhenNoCandidatePresent(t *testing.T) {
	all := []Series{gauge("something_else", map[string]string{"pod": "p"}, 1)}

	if _, ok := (Resolver{}).Select(RoleCPU, all); ok {
		t.Error("CPU should not resolve from an unrelated exposition")
	}
	if _, ok := (Resolver{}).Select(RoleMemory, all); ok {
		t.Error("memory should not resolve from an unrelated exposition")
	}
}

func TestRoleString(t *testing.T) {
	if RoleCPU.String() != "cpu" || RoleMemory.String() != "memory" {
		t.Errorf("role names = %q/%q", RoleCPU.String(), RoleMemory.String())
	}
}
