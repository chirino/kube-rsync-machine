package dataplane

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFinalizeBackupPromotesPartialCreatesLatestSymlinkAndPrunes(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "hourly", "2026-05-19T01-00-00Z", "app", "files", "old.txt"), "old")
	writeFile(t, filepath.Join(root, ".partial", "run-1", "app", "files", "data.txt"), "hello")

	points, err := FinalizeBackup(FinalizeOptions{
		TargetRoot: root,
		RunID:      "run-1",
		Timestamp:  "2026-05-20T02-03-04Z",
		Sources:    []string{"app/files"},
		Retention: RetentionPolicy{
			Hourly:  1,
			Daily:   1,
			Weekly:  1,
			Monthly: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(points) == 0 {
		t.Fatal("expected restore points")
	}
	snapshotFile := filepath.Join(root, "hourly", "2026-05-20T02-03-04Z", "app", "files", "data.txt")
	latestFile := filepath.Join(root, "latest", "app", "files", "data.txt")
	assertExists(t, snapshotFile)
	assertExists(t, latestFile)
	assertNotExists(t, filepath.Join(root, ".partial", "run-1"))
	assertNotExists(t, filepath.Join(root, "hourly", "2026-05-19T01-00-00Z"))
	assertSymlinkTarget(t, filepath.Join(root, "latest"), "hourly/2026-05-20T02-03-04Z")
	assertExists(t, filepath.Join(root, "daily", "2026-05-20", "app", "files", "data.txt"))
	assertExists(t, filepath.Join(root, "weekly", "2026-05-18", "app", "files", "data.txt"))
	assertExists(t, filepath.Join(root, "monthly", "2026-05", "app", "files", "data.txt"))
}

func TestFinalizeBackupUsesWeekStartForWeeklySnapshots(t *testing.T) {
	root := t.TempDir()
	opts := FinalizeOptions{
		TargetRoot: root,
		RunID:      "run-saturday",
		Timestamp:  "2026-05-23T02-03-04Z",
		Sources:    []string{"app/files"},
		Retention:  RetentionPolicy{Hourly: -1, Daily: -1, Weekly: -1, Monthly: -1},
	}
	writeFile(t, filepath.Join(root, ".partial", opts.RunID, "app", "files", "saturday.txt"), "saturday")
	if _, err := FinalizeBackup(opts); err != nil {
		t.Fatal(err)
	}
	assertExists(t, filepath.Join(root, "weekly", "2026-05-18", "app", "files", "saturday.txt"))

	opts.RunID = "run-sunday"
	opts.Timestamp = "2026-05-24T02-03-04Z"
	writeFile(t, filepath.Join(root, ".partial", opts.RunID, "app", "files", "sunday.txt"), "sunday")
	if _, err := FinalizeBackup(opts); err != nil {
		t.Fatal(err)
	}
	assertExists(t, filepath.Join(root, "weekly", "2026-05-18", "app", "files", "saturday.txt"))
	assertNotExists(t, filepath.Join(root, "weekly", "2026-05-24"))

	opts.RunID = "run-monday"
	opts.Timestamp = "2026-05-25T02-03-04Z"
	writeFile(t, filepath.Join(root, ".partial", opts.RunID, "app", "files", "monday.txt"), "monday")
	if _, err := FinalizeBackup(opts); err != nil {
		t.Fatal(err)
	}
	assertExists(t, filepath.Join(root, "weekly", "2026-05-25", "app", "files", "monday.txt"))
}

func TestFinalizeBackupAlreadyPromotedSnapshotIsIdempotent(t *testing.T) {
	root := t.TempDir()
	opts := FinalizeOptions{
		TargetRoot: root,
		RunID:      "run-1",
		Timestamp:  "2026-05-20T02-03-04Z",
		Sources:    []string{"app/files"},
		Retention: RetentionPolicy{
			Hourly:  2,
			Daily:   2,
			Weekly:  2,
			Monthly: 2,
		},
	}
	writeFile(t, filepath.Join(root, ".partial", "run-1", "app", "files", "data.txt"), "hello")
	if _, err := FinalizeBackup(opts); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(root, "latest")); err != nil {
		t.Fatal(err)
	}

	points, err := FinalizeBackup(opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) == 0 {
		t.Fatal("expected restore points from idempotent finalize")
	}
	snapshotFile := filepath.Join(root, "hourly", "2026-05-20T02-03-04Z", "app", "files", "data.txt")
	latestFile := filepath.Join(root, "latest", "app", "files", "data.txt")
	assertExists(t, snapshotFile)
	assertExists(t, latestFile)
	assertSymlinkTarget(t, filepath.Join(root, "latest"), "hourly/2026-05-20T02-03-04Z")
}

func TestFinalizeBackupRejectsUnsafeExpectedSource(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, ".partial", "run-1", "app", "files", "data.txt"), "hello")
	_, err := FinalizeBackup(FinalizeOptions{
		TargetRoot: root,
		RunID:      "run-1",
		Timestamp:  "2026-05-20T02-03-04Z",
		Sources:    []string{"../escape"},
	})
	if err == nil {
		t.Fatal("expected unsafe path error")
	}
}

func TestFinalizeBackupRejectsUnsafeRunID(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, ".partial", "run-1", "app", "files", "data.txt"), "hello")
	_, err := FinalizeBackup(FinalizeOptions{
		TargetRoot: root,
		RunID:      "../run-1",
		Timestamp:  "2026-05-20T02-03-04Z",
		Sources:    []string{"app/files"},
	})
	if err == nil {
		t.Fatal("expected unsafe run id error")
	}
}

func TestNormalizeDestinationPath(t *testing.T) {
	path, err := NormalizeDestinationPath("app-prod", "./sites//demo/files")
	if err != nil {
		t.Fatal(err)
	}
	if path != "app-prod/sites/demo/files" {
		t.Fatalf("unexpected normalized path %q", path)
	}
	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "root", path: "/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, err := NormalizeDestinationPath("app-prod", tc.path)
			if err != nil {
				t.Fatal(err)
			}
			if path != "app-prod" {
				t.Fatalf("unexpected normalized path %q", path)
			}
		})
	}

	for _, tc := range []struct {
		name      string
		namespace string
		path      string
	}{
		{name: "absolute", namespace: "app", path: "/data"},
		{name: "escape", namespace: "app", path: "../data"},
		{name: "backslash", namespace: "app", path: `data\files`},
		{name: "nested namespace", namespace: "app/prod", path: "data"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NormalizeDestinationPath(tc.namespace, tc.path); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestScanRestorePointsReportsLatestAndTiers(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "hourly", "2026-05-20T02-03-04Z", "app", "files", "data.txt"), "hourly")
	if err := os.Symlink("hourly/2026-05-20T02-03-04Z", filepath.Join(root, "latest")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, "daily", "2026-05-20", "app", "files", "data.txt"), "daily")
	writeFile(t, filepath.Join(root, "weekly", "2026-05-18", "app", "files", "data.txt"), "weekly")
	writeFile(t, filepath.Join(root, "monthly", "2026-05", "app", "files", "data.txt"), "monthly")

	points, err := ScanRestorePoints(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 5 {
		t.Fatalf("expected 5 restore points, got %d: %#v", len(points), points)
	}
	if points[0].Snapshot != "latest" || points[0].ResolvesTo != "hourly/2026-05-20T02-03-04Z" {
		t.Fatalf("unexpected latest point: %#v", points[0])
	}
	if points[1].Snapshot != "weekly/2026-05-18" || points[1].Tier != "weekly" {
		t.Fatalf("unexpected sorted tier point: %#v", points[1])
	}
	wantCreatedAt, err := time.Parse(SnapshotTimestampLayout, "2026-05-20T02-03-04Z")
	if err != nil {
		t.Fatal(err)
	}
	if !points[0].CreatedAt.Equal(wantCreatedAt) {
		t.Fatalf("latest createdAt = %s, want %s", points[0].CreatedAt, wantCreatedAt)
	}
}

func TestAcquireTargetLockReleaseAndConflict(t *testing.T) {
	root := t.TempDir()
	lock, err := AcquireTargetLock(root, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireTargetLock(root, "run-2"); err == nil {
		t.Fatal("expected lock conflict")
	}
	if err := (&TargetLock{path: filepath.Join(root, ".krm-run.lock"), runID: "run-2"}).Release(); err == nil {
		t.Fatal("expected non-owner release to fail")
	}
	if err := lock.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, ".krm-run.lock")); !os.IsNotExist(err) {
		t.Fatalf("expected lock file removed, got %v", err)
	}
}

func TestCleanupStalePartialsKeepsActiveRuns(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, ".partial", "active", "data.txt"), "active")
	writeFile(t, filepath.Join(root, ".partial", "stale-a", "data.txt"), "stale")
	writeFile(t, filepath.Join(root, ".partial", "stale-b", "data.txt"), "stale")

	removed, err := CleanupStalePartials(root, []string{"active"})
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 2 || removed[0] != "stale-a" || removed[1] != "stale-b" {
		t.Fatalf("unexpected removed partials: %#v", removed)
	}
	assertExists(t, filepath.Join(root, ".partial", "active"))
	assertNotExists(t, filepath.Join(root, ".partial", "stale-a"))
	assertNotExists(t, filepath.Join(root, ".partial", "stale-b"))
}

func TestApplyRetentionReturnsRemovedSnapshots(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "hourly", "2026-05-20T01-00-00Z", "data.txt"), "old")
	writeFile(t, filepath.Join(root, "hourly", "2026-05-20T02-00-00Z", "data.txt"), "new")

	removed, err := ApplyRetention(root, RetentionPolicy{Hourly: 1, Daily: -1, Weekly: -1, Monthly: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0] != "hourly/2026-05-20T01-00-00Z" {
		t.Fatalf("unexpected removed snapshots: %#v", removed)
	}
	assertNotExists(t, filepath.Join(root, "hourly", "2026-05-20T01-00-00Z"))
	assertExists(t, filepath.Join(root, "hourly", "2026-05-20T02-00-00Z"))
}

func TestApplyRetentionRemovesLatestSymlinkWhenLastHourlyIsDeleted(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "hourly", "2026-05-20T01-00-00Z", "data.txt"), "old")
	if err := os.Symlink("hourly/2026-05-20T01-00-00Z", filepath.Join(root, "latest")); err != nil {
		t.Fatal(err)
	}

	removed, err := ApplyRetention(root, RetentionPolicy{Hourly: 0, Daily: -1, Weekly: -1, Monthly: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0] != "hourly/2026-05-20T01-00-00Z" {
		t.Fatalf("unexpected removed snapshots: %#v", removed)
	}
	assertNotExists(t, filepath.Join(root, "hourly", "2026-05-20T01-00-00Z"))
	assertNotExists(t, filepath.Join(root, "latest"))
}

func TestRecoverSpaceRemovesOldestUnprotectedSnapshot(t *testing.T) {
	root := t.TempDir()
	old := filepath.Join(root, "hourly", "2026-05-20T01-00-00Z")
	newer := filepath.Join(root, "hourly", "2026-05-20T02-00-00Z")
	writeFile(t, filepath.Join(old, "data.txt"), "old")
	writeFile(t, filepath.Join(newer, "data.txt"), "new")
	oldTime := time.Now().Add(-2 * time.Hour)
	newTime := time.Now().Add(-1 * time.Hour)
	if err := os.Chtimes(old, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(newer, newTime, newTime); err != nil {
		t.Fatal(err)
	}

	removed, err := RecoverSpace(root, ^uint64(0), []string{"hourly/2026-05-20T02-00-00Z"})
	if err == nil {
		t.Fatal("expected insufficient space error after pruning candidates")
	}
	if len(removed) != 1 || removed[0] != "hourly/2026-05-20T01-00-00Z" {
		t.Fatalf("unexpected removed snapshots: %#v", removed)
	}
	assertNotExists(t, old)
	assertExists(t, newer)
}

func writeFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", path, err)
	}
}

func assertNotExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected %s not to exist, got %v", path, err)
	}
}

func assertSymlinkTarget(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.Readlink(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("expected %s to point to %q, got %q", path, want, got)
	}
}
