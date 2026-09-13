package agent

import (
	"context"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
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
