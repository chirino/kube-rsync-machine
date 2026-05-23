package dataplane

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

func logLine(out io.Writer, message string, fields ...string) {
	if out == nil {
		return
	}
	var b strings.Builder
	b.WriteString(time.Now().UTC().Format(time.RFC3339))
	b.WriteString(" krm ")
	b.WriteString(message)
	for i := 0; i+1 < len(fields); i += 2 {
		key := strings.TrimSpace(fields[i])
		if key == "" {
			continue
		}
		b.WriteByte(' ')
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(fields[i+1]))
	}
	b.WriteByte('\n')
	_, _ = io.WriteString(out, b.String())
}

func shellCommand(name string, args []string) string {
	parts := make([]string, 0, len(args)+1)
	parts = append(parts, shellQuote(name))
	for _, arg := range args {
		parts = append(parts, shellQuote(arg))
	}
	return strings.Join(parts, " ")
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	if strings.IndexFunc(value, func(r rune) bool {
		return !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || strings.ContainsRune("@%_+=:,./-", r))
	}) < 0 {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func formatTransferStats(stats TransferStats) string {
	return fmt.Sprintf("percent=%d bytes=%d rateBytesPerSecond=%d files=%d totalFiles=%d totalFileSize=%d bytesSent=%d bytesReceived=%d speedup=%.2f", stats.Percent, stats.BytesTransferred, stats.RateBytesPerSecond, stats.FilesTransferred, stats.TotalFiles, stats.TotalFileSize, stats.BytesSent, stats.BytesReceived, stats.Speedup)
}
