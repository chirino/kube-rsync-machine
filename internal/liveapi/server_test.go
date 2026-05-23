package liveapi

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/control"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestSnapshotEndpointReturnsReplayEvents(t *testing.T) {
	hub := control.NewEventHub(10)
	_, err := hub.PublishSource(control.SourceEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Running",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/backup/backups/run-1", nil)
	rec := httptest.NewRecorder()
	New(hub).Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
	}
	var events []control.ControlEvent
	if err := json.Unmarshal(rec.Body.Bytes(), &events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Source == nil || events[0].Source.SourceName != "files" {
		t.Fatalf("unexpected events: %#v", events)
	}
}

func TestEventsEndpointStreamsReplay(t *testing.T) {
	hub := control.NewEventHub(10)
	_, err := hub.PublishTarget(control.TargetEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
		Phase:           "Ready",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/backup/backups/run-1/events", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	New(hub).Handler().ServeHTTP(rec, req)

	scanner := bufio.NewScanner(strings.NewReader(rec.Body.String()))
	found := false
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "event: target") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected target event in SSE body: %q", rec.Body.String())
	}
}

func TestEventsEndpointReplaysAfterLastEventID(t *testing.T) {
	hub := control.NewEventHub(10)
	_, err := hub.PublishSource(control.SourceEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Preparing",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := hub.PublishSource(control.SourceEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Running",
	})
	if err != nil {
		t.Fatal(err)
	}
	third, err := hub.PublishSource(control.SourceEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Succeeded",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/backup/backups/run-1/events", nil).WithContext(ctx)
	req.Header.Set("Last-Event-ID", uintToString(second.Sequence))
	rec := httptest.NewRecorder()
	New(hub).Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	if strings.Contains(body, "Preparing") || strings.Contains(body, "Running") {
		t.Fatalf("expected replay to skip events through sequence %d, got %q", second.Sequence, body)
	}
	if !strings.Contains(body, "id: "+uintToString(third.Sequence)) || !strings.Contains(body, "Succeeded") {
		t.Fatalf("expected replayed third event, got %q", body)
	}
}

func TestGlobalEventsEndpointFiltersReplay(t *testing.T) {
	hub := control.NewEventHub(10)
	first, err := hub.PublishSource(control.SourceEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Preparing",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = hub.PublishSource(control.SourceEvent{
		RunNamespace:    "other",
		RunName:         "run-2",
		RunKind:         control.RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Running",
	})
	if err != nil {
		t.Fatal(err)
	}
	third, err := hub.PublishSource(control.SourceEvent{
		RunNamespace:    "backup",
		RunName:         "run-3",
		RunKind:         control.RunKindRestore,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Succeeded",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events?namespace=backup&kind=restore", nil).WithContext(ctx)
	req.Header.Set("Last-Event-ID", uintToString(first.Sequence))
	rec := httptest.NewRecorder()
	New(hub).Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	if strings.Contains(body, "run-1") || strings.Contains(body, "run-2") {
		t.Fatalf("expected filtered replay to exclude older and other namespace events, got %q", body)
	}
	if !strings.Contains(body, "id: "+uintToString(third.Sequence)) || !strings.Contains(body, "run-3") {
		t.Fatalf("expected filtered replayed restore event, got %q", body)
	}
}

func TestEventsEndpointRejectsInvalidLastEventID(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/backup/backups/run-1/events", nil)
	req.Header.Set("Last-Event-ID", "not-a-sequence")
	rec := httptest.NewRecorder()
	New(control.NewEventHub(10)).Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
	}
	var response errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != "invalid_last_event_id" {
		t.Fatalf("unexpected error response: %#v", response)
	}
}

func TestCachedListEndpointsAndFiltering(t *testing.T) {
	server := NewWithControl(control.NewService(control.NewEventHub(10)), WithClient(fakeLiveClient(t,
		&krmv1alpha1.RsyncMachine{
			ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "archive"},
			Spec: krmv1alpha1.RsyncMachineSpec{
				PVCName: "archive-pvc",
			},
		},
		&krmv1alpha1.RsyncMachine{
			ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "other"},
			Spec: krmv1alpha1.RsyncMachineSpec{
				PVCName: "other-pvc",
			},
		},
		&krmv1alpha1.BackupJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "run-a"},
			Spec:       krmv1alpha1.BackupJobSpec{MachineRef: krmv1alpha1.ObjectReference{Name: "archive"}},
			Status:     krmv1alpha1.BackupJobStatus{Phase: krmv1alpha1.RunPhaseRunning},
		},
		&krmv1alpha1.BackupJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "run-b"},
			Spec:       krmv1alpha1.BackupJobSpec{MachineRef: krmv1alpha1.ObjectReference{Name: "other"}},
			Status:     krmv1alpha1.BackupJobStatus{Phase: krmv1alpha1.RunPhaseSucceeded},
		},
		&krmv1alpha1.BackupSource{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "files"},
			Spec:       krmv1alpha1.BackupSourceSpec{MachineRef: krmv1alpha1.ObjectReference{Namespace: "backup", Name: "archive"}, PVC: "files-pvc"},
		},
	)))

	var machines krmv1alpha1.RsyncMachineList
	getJSON(t, server.Handler(), "/api/v1/namespaces/backup/machines?source=app/files", &machines)
	if len(machines.Items) != 1 || machines.Items[0].Name != "archive" {
		t.Fatalf("unexpected filtered machines: %#v", machines.Items)
	}

	var runs krmv1alpha1.BackupJobList
	getJSON(t, server.Handler(), "/api/v1/namespaces/backup/backups?phase=Running&machine=archive", &runs)
	if len(runs.Items) != 1 || runs.Items[0].Name != "run-a" {
		t.Fatalf("unexpected filtered runs: %#v", runs.Items)
	}

	var allMachines krmv1alpha1.RsyncMachineList
	getJSON(t, server.Handler(), "/api/v1/machines", &allMachines)
	if len(allMachines.Items) != 2 {
		t.Fatalf("unexpected global machines: %#v", allMachines.Items)
	}

	var allRuns krmv1alpha1.BackupJobList
	getJSON(t, server.Handler(), "/api/v1/backups", &allRuns)
	if len(allRuns.Items) != 2 {
		t.Fatalf("unexpected global backup runs: %#v", allRuns.Items)
	}

	var allSources krmv1alpha1.BackupSourceList
	getJSON(t, server.Handler(), "/api/v1/sources", &allSources)
	if len(allSources.Items) != 1 || allSources.Items[0].Namespace != "app" {
		t.Fatalf("unexpected global sources: %#v", allSources.Items)
	}
}

func TestCachedDetailEndpointsAndRestorePoints(t *testing.T) {
	createdAt := metav1.NewTime(time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC))
	server := NewWithControl(control.NewService(control.NewEventHub(10)), WithClient(fakeLiveClient(t,
		&krmv1alpha1.RsyncMachine{
			ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "archive"},
			Spec:       krmv1alpha1.RsyncMachineSpec{PVCName: "backup-pvc"},
			Status: krmv1alpha1.RsyncMachineStatus{RestorePoints: []krmv1alpha1.RestorePoint{
				{Snapshot: "latest", ResolvesTo: "hourly/2026-05-20T12-00-00Z", CreatedAt: &createdAt},
				{Snapshot: "daily/2026-05-20", Tier: "daily", CreatedAt: &createdAt},
			}},
		},
		&krmv1alpha1.BackupSource{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "files"},
			Spec:       krmv1alpha1.BackupSourceSpec{MachineRef: krmv1alpha1.ObjectReference{Namespace: "backup", Name: "archive"}, PVC: "files-pvc"},
		},
		&krmv1alpha1.RestoreJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "restore-a"},
			Spec: krmv1alpha1.RestoreJobSpec{
				SourceRef: krmv1alpha1.ObjectReference{Namespace: "app", Name: "files"},
			},
			Status: krmv1alpha1.RestoreJobStatus{Phase: krmv1alpha1.RunPhaseRunning},
		},
	)))

	var target krmv1alpha1.RsyncMachine
	getJSON(t, server.Handler(), "/api/v1/namespaces/backup/machines/archive", &target)
	if target.Spec.PVCName != "backup-pvc" {
		t.Fatalf("unexpected target detail: %#v", target)
	}

	var points restorePointList
	getJSON(t, server.Handler(), "/api/v1/namespaces/backup/machines/archive/restorepoints?tier=daily", &points)
	if len(points.Items) != 1 || points.Items[0].Snapshot != "daily/2026-05-20" {
		t.Fatalf("unexpected restore points: %#v", points.Items)
	}

	var sources krmv1alpha1.BackupSourceList
	getJSON(t, server.Handler(), "/api/v1/namespaces/app/sources?pvc=files-pvc&capture=Auto", &sources)
	if len(sources.Items) != 1 || sources.Items[0].Name != "files" {
		t.Fatalf("unexpected sources: %#v", sources.Items)
	}

	var restores krmv1alpha1.RestoreJobList
	getJSON(t, server.Handler(), "/api/v1/namespaces/backup/restores?phase=Running&machine=archive&source=app/files", &restores)
	if len(restores.Items) != 1 || restores.Items[0].Name != "restore-a" {
		t.Fatalf("unexpected restores: %#v", restores.Items)
	}

	var allRestores krmv1alpha1.RestoreJobList
	getJSON(t, server.Handler(), "/api/v1/restores", &allRestores)
	if len(allRestores.Items) != 1 || allRestores.Items[0].Name != "restore-a" {
		t.Fatalf("unexpected global restores: %#v", allRestores.Items)
	}
}

func TestCachedDetailNotFoundReturnsNormalizedError(t *testing.T) {
	server := NewWithControl(control.NewService(control.NewEventHub(10)), WithClient(fakeLiveClient(t)))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/backup/machines/missing", nil)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
	}
	var response errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != "not_found" {
		t.Fatalf("unexpected error response: %#v", response)
	}
}

func TestFrontendServesSPAAndKeepsAPI404(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html>app</html>"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "assets"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "assets", "app.js"), []byte("console.log('app')"), 0o644); err != nil {
		t.Fatal(err)
	}
	handler := NewWithControl(control.NewService(control.NewEventHub(10)), WithFrontendDir(dir)).Handler()

	for _, path := range []string{"/", "/restore/history/deep-link"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "app") {
			t.Fatalf("expected SPA index for %s, got %d %q", path, rec.Code, rec.Body.String())
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/assets/app.js", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "console.log") {
		t.Fatalf("expected static asset, got %d %q", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/missing", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "not_found") {
		t.Fatalf("expected API 404, got %d %q", rec.Code, rec.Body.String())
	}
}

func TestControlMutationEndpointsAreNotExposedOverHTTP(t *testing.T) {
	server := NewWithControl(control.NewService(control.NewEventHub(10)))
	req := httptest.NewRequest(http.MethodPost, "/control/v1/target/events", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected old control endpoint to be absent, got %d: %s", rec.Code, rec.Body.String())
	}
}

func fakeLiveClient(t *testing.T, objects ...crclient.Object) crclient.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := krmv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		Build()
}

func getJSON(t *testing.T, handler http.Handler, path string, out any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d for %s: %s", rec.Code, path, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatal(err)
	}
}

func uintToString(value uint64) string {
	return strconv.FormatUint(value, 10)
}
