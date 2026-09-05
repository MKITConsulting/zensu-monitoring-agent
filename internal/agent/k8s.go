package agent

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

// AnnotationService is the annotation a workload must carry to be reported.
// Opt-in by design: only annotated Deployments are observed.
const AnnotationService = "zensu.dev/service"

// ErrMetricsAPIUnavailable signals that the cluster has no metrics.k8s.io API
// registered (no metrics-server installed). It is a normal, expected condition
// on self-hosted clusters: the agent logs it once and keeps sending heartbeats
// without per-service CPU/memory. Callers should treat it as "metrics not
// available" rather than a failure of the tick.
var ErrMetricsAPIUnavailable = fmt.Errorf("metrics.k8s.io API not available: %w", ErrSourceUnavailable)

// ClusterReader is the minimal read-only Kubernetes surface the agent needs:
// list Deployments, list the Pods behind a Deployment to sum restarts, and read
// per-pod CPU/memory usage from metrics-server. The agent only ever reads — it
// never mutates anything.
type ClusterReader interface {
	ListDeployments(ctx context.Context, namespace string) ([]appsv1.Deployment, error)
	ListPods(ctx context.Context, namespace, selector string) ([]corev1.Pod, error)
	// PodMetricsForSelector sums current CPU (millicores) and memory (bytes)
	// across every container of every pod matching selector in namespace.
	//
	// available reports whether usable metrics were obtained. When the cluster
	// has no metrics-server, the returned error is ErrMetricsAPIUnavailable and
	// available is false; callers degrade gracefully (heartbeat without
	// metrics) instead of failing. Transient errors are returned as-is with
	// available false.
	PodMetricsForSelector(ctx context.Context, namespace, selector string) (cpuMillicores, memBytes int64, available bool, err error)
}

type clientsetLister struct {
	client  kubernetes.Interface
	metrics metricsclient.Interface
}

func (l clientsetLister) ListDeployments(ctx context.Context, namespace string) ([]appsv1.Deployment, error) {
	list, err := l.client.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (l clientsetLister) ListPods(ctx context.Context, namespace, selector string) ([]corev1.Pod, error) {
	list, err := l.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// PodMetricsForSelector reads PodMetrics from metrics-server for the matched
// pods and totals CPU/memory across all their containers. A missing
// metrics.k8s.io API (no metrics-server) is mapped to ErrMetricsAPIUnavailable
// so the caller can degrade gracefully; any other error is returned verbatim.
func (l clientsetLister) PodMetricsForSelector(ctx context.Context, namespace, selector string) (int64, int64, bool, error) {
	if l.metrics == nil {
		return 0, 0, false, ErrMetricsAPIUnavailable
	}
	list, err := l.metrics.MetricsV1beta1().PodMetricses(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		if isMetricsAPIMissing(err) {
			return 0, 0, false, ErrMetricsAPIUnavailable
		}
		return 0, 0, false, err
	}
	cpu, mem := sumPodMetrics(list.Items)
	return cpu, mem, true, nil
}

// isMetricsAPIMissing reports whether err indicates the metrics.k8s.io API
// group/resource is not registered on the cluster (no metrics-server). The
// aggregated API server surfaces this as a NotFound StatusError on
// discovery/list. Transient conditions (server timeout, unavailable backend)
// are intentionally NOT treated as missing — they bubble up as transient.
func isMetricsAPIMissing(err error) bool {
	return apierrors.IsNotFound(err)
}

// NewClientsetLister wraps Kubernetes + metrics clientsets as a ClusterReader.
// The metrics client may be nil; PodMetricsForSelector then reports the metrics
// API as unavailable (graceful degrade).
func NewClientsetLister(client kubernetes.Interface, metrics metricsclient.Interface) ClusterReader {
	return clientsetLister{client: client, metrics: metrics}
}

// NewInClusterLister builds a ClusterReader from in-cluster config (the
// ServiceAccount token mounted into the pod). It constructs both the core
// Kubernetes client and the metrics-server client; if the cluster lacks
// metrics-server, metric reads degrade gracefully at call time.
func NewInClusterLister() (ClusterReader, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes client: %w", err)
	}
	metrics, err := metricsclient.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("metrics client: %w", err)
	}
	return clientsetLister{client: client, metrics: metrics}, nil
}

// MapDeployment converts a Deployment to a ServiceHeartbeat when it carries the
// zensu.dev/service annotation. The bool is false for unannotated workloads,
// which the agent ignores. RestartCount is filled in separately from the
// Deployment's Pods (see Agent.Collect).
func MapDeployment(d appsv1.Deployment) (ServiceHeartbeat, bool) {
	slug := d.Annotations[AnnotationService]
	if slug == "" {
		return ServiceHeartbeat{}, false
	}
	ready := d.Status.ReadyReplicas
	var desired int32
	if d.Spec.Replicas != nil {
		desired = *d.Spec.Replicas
	}
	r, de := ready, desired
	return ServiceHeartbeat{
		Slug:            slug,
		Name:            d.Name,
		Status:          DeriveStatus(ready, desired),
		ReadyReplicas:   &r,
		DesiredReplicas: &de,
	}, true
}

// deploymentSelector renders a Deployment's pod selector (MatchLabels) as a
// label-selector string. Returns "" when the Deployment has no MatchLabels, so
// callers skip pod listing rather than accidentally matching every pod.
func deploymentSelector(d appsv1.Deployment) string {
	if d.Spec.Selector == nil || len(d.Spec.Selector.MatchLabels) == 0 {
		return ""
	}
	return labels.Set(d.Spec.Selector.MatchLabels).AsSelector().String()
}

// podNames lists the Pod names behind a service, used by metric sources that
// attribute samples through pod-scoped labels.
func podNames(pods []corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.Name)
	}
	return names
}

// sumRestarts totals the container restart counts across the given Pods.
func sumRestarts(pods []corev1.Pod) int32 {
	var total int32
	for _, p := range pods {
		for _, cs := range p.Status.ContainerStatuses {
			total += cs.RestartCount
		}
	}
	return total
}

// sumPodMetrics totals CPU (millicores) and memory (bytes) across every
// container of every supplied PodMetrics. CPU uses MilliValue() (e.g. 250m ->
// 250); memory uses Value() in bytes.
func sumPodMetrics(items []metricsv1beta1.PodMetrics) (cpuMillicores, memBytes int64) {
	for _, pm := range items {
		for i := range pm.Containers {
			usage := pm.Containers[i].Usage
			cpuMillicores += usage.Cpu().MilliValue()
			memBytes += usage.Memory().Value()
		}
	}
	return cpuMillicores, memBytes
}
