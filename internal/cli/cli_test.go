package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/chirino/kube-rsync-machine/internal/dataplane"
)

func TestControlRestorePoints(t *testing.T) {
	createdAt := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	points := controlRestorePoints([]dataplane.RestorePoint{{
		Snapshot:   "latest",
		ResolvesTo: "hourly/2026-05-20T12-00-00Z",
		Tier:       "hourly",
		CreatedAt:  createdAt,
	}}, "latest", 4096)

	if len(points) != 1 {
		t.Fatalf("unexpected points: %#v", points)
	}
	if points[0].Snapshot != "latest" || points[0].ResolvesTo != "hourly/2026-05-20T12-00-00Z" || points[0].CreatedAt != "2026-05-20T12:00:00Z" {
		t.Fatalf("unexpected point: %#v", points[0])
	}
	if points[0].BytesTransferred != 4096 {
		t.Fatalf("unexpected bytes transferred: %#v", points[0])
	}
}

func TestRestoreReportsMissingSnapshotPathClearly(t *testing.T) {
	app := New("test", "test", "test")
	app.out = &bytes.Buffer{}
	app.errOut = &bytes.Buffer{}

	err := app.restore(context.Background(), []string{
		"--snapshot", t.TempDir() + "/missing",
		"--destination", t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "snapshot path") || !strings.Contains(err.Error(), "is not accessible") {
		t.Fatalf("expected clear missing snapshot error, got %v", err)
	}
}

func TestUsageDoesNotAdvertiseTriggerCommand(t *testing.T) {
	app := New("test", "test", "test")
	app.out = &bytes.Buffer{}
	errOut := &bytes.Buffer{}
	app.errOut = errOut

	err := app.Run(context.Background(), []string{"kube-rsync-machine", "help"})
	if err != nil {
		t.Fatalf("help failed: %v", err)
	}
	if strings.Contains(errOut.String(), "trigger") {
		t.Fatalf("usage still advertises trigger command:\n%s", errOut.String())
	}
}

func TestLogFinalizedSnapshotPointsUsesStructuredLogging(t *testing.T) {
	var out bytes.Buffer
	app := New("test", "test", "test")
	app.out = &out

	app.logFinalizedSnapshotPoints([]string{
		"hourly/2026-05-22T19-05-07Z",
		"latest -> hourly/2026-05-22T19-05-07Z",
	})

	logs := out.String()
	if !strings.Contains(logs, ` krm snapshot created snapshot="hourly/2026-05-22T19-05-07Z"`) {
		t.Fatalf("missing structured snapshot creation log:\n%s", logs)
	}
	if !strings.Contains(logs, ` krm snapshot alias updated alias="latest" resolvesTo="hourly/2026-05-22T19-05-07Z"`) {
		t.Fatalf("missing structured snapshot alias log:\n%s", logs)
	}
	if strings.Contains(logs, "\nhourly/2026-05-22T19-05-07Z\n") || strings.Contains(logs, "\nlatest -> hourly/2026-05-22T19-05-07Z\n") {
		t.Fatalf("found unstructured snapshot point log:\n%s", logs)
	}
}
