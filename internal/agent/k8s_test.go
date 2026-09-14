package agent

import (
	"context"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"
)

func TestDeriveStatus(t *testing.T) {
	cases := []struct {
		name           string
		ready, desired int32
		want           string
	}{
		{"all replicas ready", 3, 3, StatusUp},
		{"more ready than desired", 5, 3, StatusUp},
		{"some ready", 1, 2, StatusDegraded},
		{"none ready", 0, 2, StatusDown},
		{"scaled to zero", 0, 0, StatusDown},
		{"ready without a desire", 2, 0, StatusDown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := DeriveStatus(c.ready, c.desired); got != c.want {
				t.Errorf("DeriveStatus(%d,%d) = %s, want %s", c.ready, c.desired, got, c.want)
			}
		})
	}
}

func TestMapDeployment(t *testing.T) {
	entry, ok := MapDeployment(*deployment("default", "auth-api", "auth", 3, 3))
	if !ok {
		t.Fatal("annotated deployment should map")
	}
	if entry.Slug != "auth" || entry.Name != "auth-api" || entry.Status != StatusUp {
		t.Errorf("unexpected entry: %+v", entry)
	}
	if entry.ReadyReplicas == nil || *entry.ReadyReplicas != 3 ||
		entry.DesiredReplicas == nil || *entry.DesiredReplicas != 3 {
		t.Errorf("replica counts wrong: %+v", entry)
	}

	if _, ok := MapDeployment(*deployment("default", "db", "", 1, 1)); ok {
		t.Error("unannotated deployment must be skipped")
	}
}

func TestSumPodMetrics(t *testing.T) {
	items := []metricsv1beta1.PodMetrics{
		podMetrics("default", "api-1",
			container("app", 100, 200_000_000),
			container("sidecar", 50, 30_000_000),
		),
		podMetrics("default", "api-2",
			container("app", 250, 300_000_000),
		),
	}
	cpu, mem := sumPodMetrics(items)
	if cpu != 400 {
		t.Errorf("cpu = %d, want 400 (100 + 50 + 250)", cpu)
	}
	if mem != 530_000_000 {
		t.Errorf("mem = %d, want 530000000 (200M + 30M + 300M)", mem)
	}
}

func TestPodMetricsForSelector_NilMetricsClient(t *testing.T) {
	r := NewClientsetLister(fake.NewSimpleClientset(), nil)
	cpu, mem, available, err := r.PodMetricsForSelector(context.Background(), "default", "app=api")
	if !errors.Is(err, ErrMetricsAPIUnavailable) {
		t.Errorf("err = %v, want ErrMetricsAPIUnavailable", err)
	}
	if available || cpu != 0 || mem != 0 {
		t.Errorf("expected unavailable zero metrics, got cpu=%d mem=%d available=%v", cpu, mem, available)
	}
}

// TestDeploymentSelectorHonoursMatchExpressions pins that a Deployment which
// selects its pods only through MatchExpressions still yields a usable selector.
// Reading MatchLabels alone rendered "" for it, and every caller reads "" as
// "this workload selects nothing".
func TestDeploymentSelectorHonoursMatchExpressions(t *testing.T) {
	cases := []struct {
		name     string
		selector *metav1.LabelSelector
		want     string
	}{
		{name: "no selector at all", selector: nil, want: ""},
		{name: "present but empty selects everything, so callers must skip it",
			selector: &metav1.LabelSelector{}, want: ""},
		{name: "match labels", selector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "api"}}, want: "app=api"},
		{name: "match expressions only", selector: &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      "app",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{"api"},
			}}}, want: "app in (api)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := appsv1.Deployment{Spec: appsv1.DeploymentSpec{Selector: c.selector}}
			if got := deploymentSelector(d); got != c.want {
				t.Errorf("deploymentSelector() = %q, want %q", got, c.want)
			}
		})
	}
}

// TestIsMetricsAPIMissingDiscriminates pins the predicate the auto composite
// turns on. Its doc comment promises that a transient condition is NOT read as a
// missing API, and nothing checked that: treating one as missing is what strands
// the agent on the fallback, and treating a genuinely absent API as transient
// would keep it retrying a cluster that has no metrics-server at all.
func TestIsMetricsAPIMissingDiscriminates(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "not found is the absent API", err: apierrors.NewNotFound(
			schema.GroupResource{Group: "metrics.k8s.io", Resource: "pods"}, ""), want: true},
		{name: "service unavailable is transient", err: apierrors.NewServiceUnavailable("backend down"), want: false},
		{name: "server timeout is transient", err: apierrors.NewTimeoutError("slow", 1), want: false},
		{name: "forbidden is a permission problem, not an absent API", err: apierrors.NewForbidden(
			schema.GroupResource{Group: "metrics.k8s.io", Resource: "pods"}, "", errors.New("rbac")), want: false},
		{name: "a plain error is not a status error at all", err: errors.New("dial tcp: connection refused"), want: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isMetricsAPIMissing(c.err); got != c.want {
				t.Errorf("isMetricsAPIMissing(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// servePodMetrics wires the metrics fake to answer one list call.
//
// Seeding through NewSimpleClientset does not work here: the tracker stores a
// PodMetrics under the "podmetricses" resource, while the generated client lists
// it as "pods" — metrics.k8s.io names the resource after the object it measures.
// The two never meet, so a seeded object is invisible. A reactor on the resource
// the client actually asks for is the path that works.
//
// The generated client still applies the request's label selector to whatever
// the reactor returns, so every item must carry labels matching the selector
// under test or it is filtered out and the totals come back zero.
func servePodMetrics(items []metricsv1beta1.PodMetrics, err error) *metricsfake.Clientset {
	c := metricsfake.NewSimpleClientset()
	c.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		if err != nil {
			return true, nil, err
		}
		return true, &metricsv1beta1.PodMetricsList{Items: items}, nil
	})
	return c
}

// TestPodMetricsForSelectorReadsTheMetricsAPI covers the adapter against a real
// metrics client rather than the nil one every other test passes. Only the nil
// branch was exercised before, so the sum across pods and containers was
// unproven at this boundary.
func TestPodMetricsForSelectorReadsTheMetricsAPI(t *testing.T) {
	metricsClient := servePodMetrics([]metricsv1beta1.PodMetrics{
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api-1", Labels: map[string]string{"app": "api"}},
			Containers: []metricsv1beta1.ContainerMetrics{container("app", 120, 300), container("sidecar", 30, 70)},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api-2", Labels: map[string]string{"app": "api"}},
			Containers: []metricsv1beta1.ContainerMetrics{container("app", 50, 130)},
		},
	}, nil)
	lister := NewClientsetLister(fake.NewSimpleClientset(), metricsClient)

	cpu, mem, available, err := lister.PodMetricsForSelector(context.Background(), "prod", "app=api")
	if err != nil {
		t.Fatalf("PodMetricsForSelector: %v", err)
	}
	if !available {
		t.Error("available = false, want true — the API answered")
	}
	if cpu != 200 {
		t.Errorf("cpu = %d, want 200 — every container of every matching pod", cpu)
	}
	if mem != 500 {
		t.Errorf("mem = %d, want 500 — every container of every matching pod", mem)
	}
}

// TestPodMetricsForSelectorScopesTheRequest pins that both of the caller's
// arguments reach the API. A lister that dropped the selector would report the
// whole namespace as one service; one that dropped the namespace would list
// cluster-wide and fold another tenant's pods into this service. Every other
// assertion here would still hold in both cases.
func TestPodMetricsForSelectorScopesTheRequest(t *testing.T) {
	const (
		wantNamespace = "prod"
		wantSelector  = "app=api"
	)
	metricsClient := metricsfake.NewSimpleClientset()
	var seenNamespace, seenSelector string
	metricsClient.PrependReactor("list", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		seenNamespace = a.GetNamespace()
		seenSelector = a.(k8stesting.ListAction).GetListRestrictions().Labels.String()
		return true, &metricsv1beta1.PodMetricsList{}, nil
	})
	lister := NewClientsetLister(fake.NewSimpleClientset(), metricsClient)

	cpu, mem, _, err := lister.PodMetricsForSelector(context.Background(), wantNamespace, wantSelector)
	if err != nil {
		t.Fatalf("PodMetricsForSelector: %v", err)
	}
	if cpu != 0 || mem != 0 {
		t.Errorf("cpu, mem = %d, %d, want 0, 0 — an empty list totals nothing", cpu, mem)
	}
	if seenNamespace != wantNamespace {
		t.Errorf("namespace reached the API as %q, want %q", seenNamespace, wantNamespace)
	}
	if seenSelector != wantSelector {
		t.Errorf("selector reached the API as %q, want %q", seenSelector, wantSelector)
	}
}

// TestPodMetricsForSelectorClassifiesErrors pins the two branches that decide
// whether the agent degrades for this cluster or just for this tick. Both return
// no samples, so only the error distinguishes them, and the auto composite
// switches sources on exactly that difference.
func TestPodMetricsForSelectorClassifiesErrors(t *testing.T) {
	cases := []struct {
		name      string
		reaction  error
		wantErrIs error
	}{
		{
			name: "an absent API is reported as unavailable for the whole cluster",
			reaction: apierrors.NewNotFound(
				schema.GroupResource{Group: "metrics.k8s.io", Resource: "pods"}, ""),
			wantErrIs: ErrMetricsAPIUnavailable,
		},
		{
			name:     "a transient failure bubbles up as itself",
			reaction: apierrors.NewServiceUnavailable("backend down"),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lister := NewClientsetLister(fake.NewSimpleClientset(), servePodMetrics(nil, c.reaction))

			cpu, mem, available, err := lister.PodMetricsForSelector(context.Background(), "prod", "app=api")
			if available {
				t.Error("available = true, want false on any read failure")
			}
			if cpu != 0 || mem != 0 {
				t.Errorf("cpu, mem = %d, %d, want 0, 0 — a failed read measured nothing", cpu, mem)
			}
			if c.wantErrIs != nil {
				if !errors.Is(err, c.wantErrIs) {
					t.Fatalf("err = %v, want %v so the composite can fall through", err, c.wantErrIs)
				}
				return
			}
			if errors.Is(err, ErrMetricsAPIUnavailable) {
				t.Fatalf("err = %v, want it NOT classified as an absent API — that would strand the agent on the fallback", err)
			}
			if !errors.Is(err, c.reaction) {
				t.Errorf("err = %v, want the API's own error verbatim (%v)", err, c.reaction)
			}
		})
	}
}
