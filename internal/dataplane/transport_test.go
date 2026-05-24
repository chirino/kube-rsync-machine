package dataplane

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
)

func TestMTLSTransferWritesOnlyAssignedPartialSubtree(t *testing.T) {
	ca, err := tlsutil.NewRunCA("backup", "run-1", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	targetBundle, err := ca.Mint(tlsutil.TargetIdentity("backup", "run-1", "backup", "archive"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	sourceIdentity := tlsutil.SourceIdentity("backup", "run-1", "app", "files")
	sourceBundle, err := ca.Mint(sourceIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	sourceRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(sourceRoot, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceRoot, "nested", "data.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	targetRoot := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- ServeTargetReceiver(ctx, listener, TargetReceiverOptions{
			TargetRoot: targetRoot,
			RunID:      "backup-run-1",
			TLSBundle:  targetBundle,
			Sources: []ExpectedTransferSource{{
				Identity:    sourceIdentity,
				Destination: ".partial/backup-run-1/app/files",
			}},
		})
	}()
	if err := SendSource(ctx, SourceSenderOptions{
		Source:         sourceRoot,
		Destination:    ".partial/backup-run-1/app/files",
		TargetEndpoint: listener.Addr().String(),
		TLSBundle:      sourceBundle,
		ExpectedTarget: tlsutil.TargetIdentity("backup", "run-1", "backup", "archive"),
	}); err != nil {
		select {
		case serverErr := <-errCh:
			t.Logf("server error after send failure: %v", serverErr)
		default:
		}
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(targetRoot, ".partial", "backup-run-1", "app", "files", "nested", "data.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "payload" {
		t.Fatalf("expected transferred payload, got %q", string(data))
	}
}

func TestMTLSMirrorTransferWritesDirectlyToTargetRoot(t *testing.T) {
	ca, err := tlsutil.NewRunCA("backup", "run-1", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	targetBundle, err := ca.Mint(tlsutil.TargetIdentity("backup", "run-1", "backup", "archive"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	sourceIdentity := tlsutil.SourceIdentity("backup", "run-1", "app", "files")
	sourceBundle, err := ca.Mint(sourceIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	sourceRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(sourceRoot, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceRoot, "nested", "data.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	targetRoot := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- ServeTargetReceiver(ctx, listener, TargetReceiverOptions{
			TargetRoot: targetRoot,
			RunID:      "backup-run-1",
			Mirror:     true,
			TLSBundle:  targetBundle,
			Sources: []ExpectedTransferSource{{
				Identity:    sourceIdentity,
				Destination: ".",
			}},
		})
	}()
	if err := SendSource(ctx, SourceSenderOptions{
		Source:         sourceRoot,
		Destination:    ".",
		TargetEndpoint: listener.Addr().String(),
		TLSBundle:      sourceBundle,
		ExpectedTarget: tlsutil.TargetIdentity("backup", "run-1", "backup", "archive"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(targetRoot, "nested", "data.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "payload" {
		t.Fatalf("expected transferred payload, got %q", string(data))
	}
	for _, unexpected := range []string{".partial", "latest", "hourly", "daily", "weekly", "monthly"} {
		if _, err := os.Stat(filepath.Join(targetRoot, unexpected)); !os.IsNotExist(err) {
			t.Fatalf("did not expect %s in mirror target", unexpected)
		}
	}
}

func TestSeedPartialDestinationHardlinksFromLatest(t *testing.T) {
	targetRoot := t.TempDir()
	latestSource := filepath.Join(targetRoot, "latest", "app", "files")
	if err := os.MkdirAll(filepath.Join(latestSource, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	latestFile := filepath.Join(latestSource, "nested", "data.txt")
	if err := os.WriteFile(latestFile, []byte("previous"), 0o644); err != nil {
		t.Fatal(err)
	}

	seeded, err := seedPartialDestination(targetRoot, ".partial/backup-run-1/app/files")
	if err != nil {
		t.Fatal(err)
	}
	if !seeded {
		t.Fatal("expected destination to be seeded from latest")
	}
	partialFile := filepath.Join(targetRoot, ".partial", "backup-run-1", "app", "files", "nested", "data.txt")
	if data, err := os.ReadFile(partialFile); err != nil {
		t.Fatal(err)
	} else if string(data) != "previous" {
		t.Fatalf("expected seeded content, got %q", string(data))
	}
	assertSameFile(t, latestFile, partialFile)
}

func TestSeedPartialDestinationAllowsMissingLatest(t *testing.T) {
	targetRoot := t.TempDir()
	seeded, err := seedPartialDestination(targetRoot, ".partial/backup-run-1/app/files")
	if err != nil {
		t.Fatal(err)
	}
	if seeded {
		t.Fatal("did not expect destination to be seeded without latest")
	}
	if info, err := os.Stat(filepath.Join(targetRoot, ".partial", "backup-run-1", "app", "files")); err != nil {
		t.Fatal(err)
	} else if !info.IsDir() {
		t.Fatal("expected partial destination directory")
	}
}

func TestMTLSTransferRejectsWrongSenderIdentity(t *testing.T) {
	ca, err := tlsutil.NewRunCA("backup", "run-1", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	targetBundle, err := ca.Mint(tlsutil.TargetIdentity("backup", "run-1", "backup", "archive"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	wrongBundle, err := ca.Mint(tlsutil.SourceIdentity("backup", "run-1", "app", "wrong"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	sourceRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(sourceRoot, "data.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- ServeTargetReceiver(ctx, listener, TargetReceiverOptions{
			TargetRoot: t.TempDir(),
			RunID:      "backup-run-1",
			TLSBundle:  targetBundle,
			Sources: []ExpectedTransferSource{{
				Identity:    tlsutil.SourceIdentity("backup", "run-1", "app", "files"),
				Destination: ".partial/backup-run-1/app/files",
			}},
		})
	}()
	_ = SendSource(ctx, SourceSenderOptions{
		Source:         sourceRoot,
		Destination:    ".partial/backup-run-1/app/files",
		TargetEndpoint: listener.Addr().String(),
		TLSBundle:      wrongBundle,
		ExpectedTarget: tlsutil.TargetIdentity("backup", "run-1", "backup", "archive"),
	})
	if err := <-errCh; err == nil {
		t.Fatal("expected receiver to reject wrong sender identity")
	}
}

func assertSameFile(t *testing.T, left, right string) {
	t.Helper()
	leftInfo, err := os.Stat(left)
	if err != nil {
		t.Fatal(err)
	}
	rightInfo, err := os.Stat(right)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(leftInfo, rightInfo) {
		t.Fatalf("expected %s and %s to be hard links to the same file", left, right)
	}
}

func TestRestoreTargetServesRequestedSnapshotSubtree(t *testing.T) {
	ca, err := tlsutil.NewRunCA("backup", "restore-1", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	targetIdentity := tlsutil.TargetIdentity("backup", "restore-1", "backup", "archive")
	targetBundle, err := ca.Mint(targetIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	writerIdentity := tlsutil.SourceIdentity("backup", "restore-1", "app", "files")
	writerBundle, err := ca.Mint(writerIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	targetRoot := t.TempDir()
	snapshotRoot := filepath.Join(targetRoot, "hourly", "2026-05-20T10-00-00Z", "app", "files")
	if err := os.MkdirAll(filepath.Join(snapshotRoot, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapshotRoot, "nested", "data.txt"), []byte("restore-payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	destinationRoot := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- ServeRestoreTarget(ctx, listener, RestoreTargetOptions{
			TargetRoot: targetRoot,
			TLSBundle:  targetBundle,
			Writer: ExpectedRestoreWriter{
				Identity: writerIdentity,
				Snapshot: "hourly/2026-05-20T10-00-00Z",
				Source:   "app/files",
			},
		})
	}()
	if err := ReceiveRestore(ctx, RestoreWriterOptions{
		Destination:    destinationRoot,
		Snapshot:       "hourly/2026-05-20T10-00-00Z",
		Source:         "app/files",
		TargetEndpoint: listener.Addr().String(),
		TLSBundle:      writerBundle,
		ExpectedTarget: targetIdentity,
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(destinationRoot, "nested", "data.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "restore-payload" {
		t.Fatalf("expected transferred restore payload, got %q", string(data))
	}
}

func TestRestoreTargetServesCurrentMirrorSubtree(t *testing.T) {
	ca, err := tlsutil.NewRunCA("backup", "restore-1", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	targetIdentity := tlsutil.TargetIdentity("backup", "restore-1", "backup", "archive")
	targetBundle, err := ca.Mint(targetIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	writerIdentity := tlsutil.SourceIdentity("backup", "restore-1", "app", "files")
	writerBundle, err := ca.Mint(writerIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	targetRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(targetRoot, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(targetRoot, "nested", "data.txt"), []byte("mirror-payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	destinationRoot := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- ServeRestoreTarget(ctx, listener, RestoreTargetOptions{
			TargetRoot: targetRoot,
			TLSBundle:  targetBundle,
			Writer: ExpectedRestoreWriter{
				Identity: writerIdentity,
				Snapshot: "current",
				Source:   ".",
			},
		})
	}()
	if err := ReceiveRestore(ctx, RestoreWriterOptions{
		Destination:    destinationRoot,
		Snapshot:       "current",
		Source:         ".",
		TargetEndpoint: listener.Addr().String(),
		TLSBundle:      writerBundle,
		ExpectedTarget: targetIdentity,
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(destinationRoot, "nested", "data.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "mirror-payload" {
		t.Fatalf("expected transferred restore payload, got %q", string(data))
	}
}

func TestRestoreRsyncClientArgsIncludeTypedOptions(t *testing.T) {
	args := rsyncRestoreClientArgs(RestoreWriterOptions{
		Destination:   "/restore",
		Delete:        true,
		OneFileSystem: true,
	}, "127.0.0.1:1873")
	want := []string{"-a", "--info=progress2", "--stats", "--numeric-ids", "--delete", "--one-file-system", "rsync://127.0.0.1:1873/krm/", "/restore/"}
	if strings.Join(args, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("unexpected restore rsync args:\n got %#v\nwant %#v", args, want)
	}
}

func TestRsyncDaemonConfigUsesProcessIdentity(t *testing.T) {
	config := rsyncDaemonConfig("/backup/.partial/run/app", false)
	for _, want := range []string{
		"use chroot = false\n",
		"numeric ids = yes\n",
		"read only = false\n",
		"\tpath = /backup/.partial/run/app\n",
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("expected config to contain %q, got:\n%s", want, config)
		}
	}
	if os.Geteuid() == 0 {
		for _, want := range []string{
			"uid = 0\n",
			"gid = " + strconv.Itoa(os.Getegid()) + "\n",
		} {
			if !strings.Contains(config, want) {
				t.Fatalf("expected root config to contain %q, got:\n%s", want, config)
			}
		}
		return
	}
	if strings.Contains(config, "uid = ") || strings.Contains(config, "gid = ") {
		t.Fatalf("non-root config should not force uid/gid, got:\n%s", config)
	}
}

func TestRestoreTargetRejectsUnexpectedRequestedPath(t *testing.T) {
	ca, err := tlsutil.NewRunCA("backup", "restore-1", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	targetIdentity := tlsutil.TargetIdentity("backup", "restore-1", "backup", "archive")
	targetBundle, err := ca.Mint(targetIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	writerIdentity := tlsutil.SourceIdentity("backup", "restore-1", "app", "files")
	writerBundle, err := ca.Mint(writerIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- ServeRestoreTarget(ctx, listener, RestoreTargetOptions{
			TargetRoot: t.TempDir(),
			TLSBundle:  targetBundle,
			Writer: ExpectedRestoreWriter{
				Identity: writerIdentity,
				Snapshot: "hourly/2026-05-20T10-00-00Z",
				Source:   "app/files",
			},
		})
	}()
	_ = ReceiveRestore(ctx, RestoreWriterOptions{
		Destination:    t.TempDir(),
		Snapshot:       "hourly/2026-05-20T10-00-00Z",
		Source:         "app/other",
		TargetEndpoint: listener.Addr().String(),
		TLSBundle:      writerBundle,
		ExpectedTarget: targetIdentity,
	})
	if err := <-errCh; err == nil {
		t.Fatal("expected restore target to reject unexpected requested source")
	}
}

func TestParseExpectedTransferSourcesSupportsLegacyFinalizeOnlySources(t *testing.T) {
	receivers, finalize, err := ParseExpectedTransferSources("backup", "run-1", "backup-run-1", []string{
		"app/files",
		"app/source=app/data",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(receivers) != 1 {
		t.Fatalf("expected one receiver source, got %d", len(receivers))
	}
	if receivers[0].Destination != ".partial/backup-run-1/app/data" {
		t.Fatalf("unexpected receiver destination %q", receivers[0].Destination)
	}
	if len(finalize) != 2 || finalize[0] != "app/data" || finalize[1] != "app/files" {
		t.Fatalf("unexpected finalize sources %#v", finalize)
	}
}

func TestParseExpectedTransferSourcesWithMirrorRoot(t *testing.T) {
	receivers, finalize, err := ParseExpectedTransferSourcesWithStrategy("backup", "run-1", "backup-run-1", true, []string{
		"app/source=.",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(receivers) != 1 {
		t.Fatalf("expected one receiver source, got %d", len(receivers))
	}
	if receivers[0].Destination != "." {
		t.Fatalf("unexpected receiver destination %q", receivers[0].Destination)
	}
	if len(finalize) != 1 || finalize[0] != "" {
		t.Fatalf("unexpected finalize sources %#v", finalize)
	}
}
