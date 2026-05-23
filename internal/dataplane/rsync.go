package dataplane

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RsyncOptions struct {
	Source        string
	Destination   string
	Delete        bool
	OneFileSystem bool
	DryRun        bool
	Stdout        io.Writer
	Stderr        io.Writer
	Progress      func(TransferStats)
}

type TransferStats struct {
	Percent            uint32
	BytesTransferred   uint64
	RateBytesPerSecond uint64
	FilesTransferred   uint64
	TotalFiles         uint64
	TotalFileSize      uint64
	BytesSent          uint64
	BytesReceived      uint64
	Speedup            float64
	Summary            bool
}

func Rsync(ctx context.Context, opts RsyncOptions) (TransferStats, error) {
	if opts.Source == "" || opts.Destination == "" {
		return TransferStats{}, fmt.Errorf("source and destination are required")
	}
	args := rsyncArgs(opts)
	args = append(args, slash(opts.Source), slash(opts.Destination))
	if opts.DryRun {
		_, err := fmt.Fprintf(opts.Stdout, "%s\n", shellCommand("rsync", args))
		return TransferStats{}, err
	}
	return runRsyncCommand(ctx, args, opts.Stdout, opts.Stderr, opts.Progress)
}

func rsyncArgs(opts RsyncOptions) []string {
	args := []string{"-a", "--info=progress2", "--stats", "--numeric-ids"}
	if opts.Delete {
		args = append(args, "--delete")
	}
	if opts.OneFileSystem {
		args = append(args, "--one-file-system")
	}
	return args
}

func runRsyncCommand(ctx context.Context, args []string, stdout, stderr io.Writer, progress func(TransferStats)) (TransferStats, error) {
	logLine(stdout, "starting rsync command", "command", shellCommand("rsync", args))
	started := time.Now()
	cmd := exec.CommandContext(ctx, "rsync", args...)
	var capture bytes.Buffer
	lockedCapture := &lockedWriter{w: &capture}
	cmd.Stdout = progressWriter(stdout, lockedCapture, progress)
	cmd.Stderr = progressWriter(stderr, lockedCapture, nil)
	err := cmd.Run()
	stats := ParseRsyncStats(capture.String())
	if progress != nil {
		progress(stats)
	}
	if err != nil {
		logLine(stdout, "rsync command failed", "duration", time.Since(started).Round(time.Millisecond).String(), "exitCode", fmt.Sprintf("%d", rsyncExitCode(err)), "error", err.Error(), "stats", formatTransferStats(stats))
	} else {
		logLine(stdout, "rsync command completed", "duration", time.Since(started).Round(time.Millisecond).String(), "stats", formatTransferStats(stats))
	}
	return stats, err
}

func rsyncExitCode(err error) int {
	var exitErr *exec.ExitError
	if err != nil && errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	if err != nil {
		return 1
	}
	return 0
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}

type rsyncProgressWriter struct {
	out      io.Writer
	capture  io.Writer
	progress func(TransferStats)
	buf      strings.Builder
}

func progressWriter(out io.Writer, capture io.Writer, progress func(TransferStats)) io.Writer {
	return &rsyncProgressWriter{out: out, capture: capture, progress: progress}
}

func (w *rsyncProgressWriter) Write(p []byte) (int, error) {
	if w.out != nil {
		if _, err := w.out.Write(p); err != nil {
			return 0, err
		}
	}
	if w.capture != nil {
		_, _ = w.capture.Write(p)
	}
	if w.progress != nil {
		for _, b := range p {
			switch b {
			case '\n', '\r':
				w.flushProgress()
			default:
				_ = w.buf.WriteByte(b)
			}
		}
	}
	return len(p), nil
}

func (w *rsyncProgressWriter) flushProgress() {
	line := strings.TrimSpace(w.buf.String())
	w.buf.Reset()
	if line == "" {
		return
	}
	stats, ok := parseRsyncProgressLine(line)
	if ok {
		w.progress(stats)
	}
}

var (
	rsyncProgressPattern = regexp.MustCompile(`^\s*([0-9,.]+)\s+([0-9]{1,3})%\s+([0-9,.]+)([KMGTPE]?B)/s`)
	numberFieldPattern   = regexp.MustCompile(`[^0-9.]`)
	leadingNumberPattern = regexp.MustCompile(`^\s*([0-9][0-9,.]*)`)
)

func ParseRsyncStats(output string) TransferStats {
	stats := TransferStats{}
	output = strings.ReplaceAll(output, "\r", "\n")
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if progress, ok := parseRsyncProgressLine(line); ok {
			if progress.Percent >= stats.Percent {
				stats.Percent = progress.Percent
				stats.BytesTransferred = progress.BytesTransferred
				stats.RateBytesPerSecond = progress.RateBytesPerSecond
			}
			continue
		}
		switch {
		case strings.HasPrefix(line, "Number of files:"):
			stats.Summary = true
			stats.TotalFiles = parseLeadingUint(strings.TrimPrefix(line, "Number of files:"))
		case strings.HasPrefix(line, "Number of regular files transferred:"):
			stats.Summary = true
			stats.FilesTransferred = parseLeadingUint(strings.TrimPrefix(line, "Number of regular files transferred:"))
		case strings.HasPrefix(line, "Total file size:"):
			stats.Summary = true
			stats.TotalFileSize = parseLeadingUint(strings.TrimPrefix(line, "Total file size:"))
		case strings.HasPrefix(line, "Total transferred file size:"):
			stats.Summary = true
			stats.BytesTransferred = parseLeadingUint(strings.TrimPrefix(line, "Total transferred file size:"))
		case strings.HasPrefix(line, "sent "):
			if sent, received, ok := parseRsyncSentReceived(line); ok {
				stats.Summary = true
				stats.BytesSent = sent
				stats.BytesReceived = received
			}
			if rate, ok := parseRsyncSummaryRate(line); ok {
				stats.Summary = true
				stats.RateBytesPerSecond = rate
			}
		case strings.Contains(line, " bytes/sec"):
			if rate, ok := parseRsyncSummaryRate(line); ok {
				stats.Summary = true
				stats.RateBytesPerSecond = rate
			}
		case strings.Contains(line, "speedup is"):
			stats.Summary = true
			stats.Speedup = parseTrailingFloat(line)
		}
	}
	return stats
}

func parseRsyncProgressLine(line string) (TransferStats, bool) {
	matches := rsyncProgressPattern.FindStringSubmatch(line)
	if matches == nil {
		return TransferStats{}, false
	}
	bytesTransferred := parseLeadingUint(matches[1])
	percent, _ := strconv.ParseUint(matches[2], 10, 32)
	rate := parseRate(matches[3], matches[4])
	return TransferStats{Percent: uint32(percent), BytesTransferred: bytesTransferred, RateBytesPerSecond: rate}, true
}

func parseLeadingUint(value string) uint64 {
	match := leadingNumberPattern.FindStringSubmatch(value)
	if match == nil {
		return 0
	}
	clean := strings.ReplaceAll(match[1], ",", "")
	if clean == "" {
		return 0
	}
	if strings.Contains(clean, ".") {
		parsed, _ := strconv.ParseFloat(clean, 64)
		return uint64(parsed)
	}
	parsed, _ := strconv.ParseUint(clean, 10, 64)
	return parsed
}

func parseTrailingFloat(line string) float64 {
	idx := strings.LastIndex(line, " ")
	if idx < 0 {
		return 0
	}
	parsed, _ := strconv.ParseFloat(numberFieldPattern.ReplaceAllString(line[idx+1:], ""), 64)
	return parsed
}

func parseRsyncSummaryRate(line string) (uint64, bool) {
	before, _, ok := strings.Cut(line, " bytes/sec")
	if !ok {
		return 0, false
	}
	fields := strings.Fields(before)
	if len(fields) == 0 {
		return 0, false
	}
	return parseLeadingUint(fields[len(fields)-1]), true
}

func parseRsyncSentReceived(line string) (uint64, uint64, bool) {
	fields := strings.Fields(line)
	if len(fields) < 6 || fields[0] != "sent" || fields[2] != "bytes" || fields[3] != "received" || fields[5] != "bytes" {
		return 0, 0, false
	}
	return parseLeadingUint(fields[1]), parseLeadingUint(fields[4]), true
}

func parseRate(value, unit string) uint64 {
	parsed, _ := strconv.ParseFloat(strings.ReplaceAll(value, ",", ""), 64)
	multiplier := float64(1)
	switch strings.ToUpper(unit) {
	case "KB":
		multiplier = 1024
	case "MB":
		multiplier = 1024 * 1024
	case "GB":
		multiplier = 1024 * 1024 * 1024
	case "TB":
		multiplier = 1024 * 1024 * 1024 * 1024
	case "PB":
		multiplier = 1024 * 1024 * 1024 * 1024 * 1024
	case "EB":
		multiplier = 1024 * 1024 * 1024 * 1024 * 1024 * 1024
	}
	return uint64(parsed * multiplier)
}

func slash(path string) string {
	if path == "" || path[len(path)-1] == '/' {
		return path
	}
	return path + "/"
}
