package dataplane

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseRsyncStats(t *testing.T) {
	output := "\r      1,024  50%  1.00MB/s    0:00:01 (xfr#1, to-chk=1/2)\n" +
		"\r      2,048 100%  2.00MB/s    0:00:00 (xfr#2, to-chk=0/2)\n" +
		"Number of files: 12 (reg: 10, dir: 2)\n" +
		"Number of regular files transferred: 3\n" +
		"Total file size: 4,096 bytes\n" +
		"Total transferred file size: 2,048 bytes\n" +
		"sent 2,200 bytes  received 100 bytes  4,600.00 bytes/sec\n" +
		"total size is 4,096  speedup is 1.78\n"

	stats := ParseRsyncStats(output)
	if stats.Percent != 100 {
		t.Fatalf("expected percent 100, got %d", stats.Percent)
	}
	if stats.BytesTransferred != 2048 {
		t.Fatalf("expected 2048 bytes transferred, got %d", stats.BytesTransferred)
	}
	if stats.RateBytesPerSecond != 4600 {
		t.Fatalf("expected 4600 B/s, got %d", stats.RateBytesPerSecond)
	}
	if stats.TotalFiles != 12 || stats.FilesTransferred != 3 {
		t.Fatalf("unexpected file counts: %#v", stats)
	}
	if stats.TotalFileSize != 4096 || stats.BytesSent != 2200 || stats.BytesReceived != 100 {
		t.Fatalf("unexpected rsync summary bytes: %#v", stats)
	}
	if stats.Speedup != 1.78 {
		t.Fatalf("expected speedup 1.78, got %v", stats.Speedup)
	}
	if !stats.Summary {
		t.Fatalf("expected summary stats")
	}
}

func TestParseRsyncStatsZeroFileTransferSummary(t *testing.T) {
	output := "Number of files: 14,278 (reg: 11,565, dir: 2,713)\n" +
		"Number of regular files transferred: 0\n" +
		"Total file size: 248,560,272 bytes\n" +
		"Total transferred file size: 0 bytes\n" +
		"sent 437,865 bytes  received 10,983 bytes  299,232.00 bytes/sec\n" +
		"total size is 248,560,272  speedup is 553.77\n"

	stats := ParseRsyncStats(output)
	if !stats.Summary {
		t.Fatal("expected summary stats")
	}
	if stats.TotalFiles != 14278 || stats.FilesTransferred != 0 {
		t.Fatalf("unexpected file counts: %#v", stats)
	}
	if stats.BytesTransferred != 0 || stats.TotalFileSize != 248560272 {
		t.Fatalf("unexpected file byte counts: %#v", stats)
	}
	if stats.BytesSent != 437865 || stats.BytesReceived != 10983 {
		t.Fatalf("unexpected wire byte counts: %#v", stats)
	}
}

func TestRsyncDryRunLogsExecutableCommand(t *testing.T) {
	var out bytes.Buffer

	_, err := Rsync(t.Context(), RsyncOptions{
		Source:      "/data/source dir",
		Destination: "/backup/target",
		Delete:      true,
		DryRun:      true,
		Stdout:      &out,
	})
	if err != nil {
		t.Fatalf("Rsync returned error: %v", err)
	}
	got := strings.TrimSpace(out.String())
	if !strings.HasPrefix(got, "rsync ") {
		t.Fatalf("expected command to start with rsync, got %q", got)
	}
	if strings.HasPrefix(got, "rsync rsync ") {
		t.Fatalf("command duplicated executable name: %q", got)
	}
	if !strings.Contains(got, "'/data/source dir/'") {
		t.Fatalf("expected shell-quoted source path, got %q", got)
	}
	if !strings.Contains(got, "--delete") || !strings.Contains(got, "--numeric-ids") {
		t.Fatalf("expected rsync flags in dry-run command, got %q", got)
	}
}
