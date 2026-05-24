package dataplane

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
)

const maxTransferHeaderBytes = 64 * 1024

const transferProtocolRsync = "rsync"

type ExpectedTransferSource struct {
	Identity    tlsutil.Identity
	Destination string
}

type ExpectedRestoreWriter struct {
	Identity tlsutil.Identity
	Snapshot string
	Source   string
}

type TargetReceiverOptions struct {
	TargetRoot              string
	RunID                   string
	Mirror                  bool
	TLSBundle               tlsutil.Bundle
	Sources                 []ExpectedTransferSource
	ContinueOnTransferError bool
	Log                     io.Writer
}

type RestoreTargetOptions struct {
	TargetRoot string
	TLSBundle  tlsutil.Bundle
	Writer     ExpectedRestoreWriter
	Log        io.Writer
}

type SourceSenderOptions struct {
	Source         string
	Destination    string
	TargetEndpoint string
	TLSBundle      tlsutil.Bundle
	TLSDir         string
	ExpectedTarget tlsutil.Identity
	Delete         bool
	OneFileSystem  bool
	DryRun         bool
	Stdout         io.Writer
	Stderr         io.Writer
	Progress       func(TransferStats)
}

type RestoreWriterOptions struct {
	Destination    string
	Snapshot       string
	Source         string
	TargetEndpoint string
	TLSBundle      tlsutil.Bundle
	ExpectedTarget tlsutil.Identity
	Delete         bool
	OneFileSystem  bool
	DryRun         bool
	Stdout         io.Writer
	Stderr         io.Writer
	Progress       func(TransferStats)
}

type transferHeader struct {
	Destination string `json:"destination"`
	Snapshot    string `json:"snapshot,omitempty"`
	Source      string `json:"source,omitempty"`
	Protocol    string `json:"protocol"`
}

func ServeTargetReceiver(ctx context.Context, listener net.Listener, opts TargetReceiverOptions) error {
	if opts.TargetRoot == "" || opts.RunID == "" {
		return fmt.Errorf("target root and run id are required")
	}
	if len(opts.Sources) == 0 {
		return nil
	}
	root, err := filepath.Abs(opts.TargetRoot)
	if err != nil {
		return err
	}
	expected := make(map[tlsutil.Identity]string, len(opts.Sources))
	for _, source := range opts.Sources {
		if source.Identity == (tlsutil.Identity{}) {
			return fmt.Errorf("source identity is required")
		}
		destination, err := NormalizeTargetSubpath(source.Destination)
		if err != nil {
			return fmt.Errorf("invalid destination for %s: %w", source.Identity.URI(), err)
		}
		expected[source.Identity] = destination
	}
	config, err := tlsutil.TLSConfig(opts.TLSBundle, tlsutil.Identity{}, "")
	if err != nil {
		return err
	}
	tlsListener := tls.NewListener(listener, config)
	defer tlsListener.Close()
	logLine(opts.Log, "target receiver listening", "address", listener.Addr().String(), "targetRoot", root, "runID", opts.RunID, "expectedSources", fmt.Sprintf("%d", len(expected)), "continueOnTransferError", fmt.Sprintf("%t", opts.ContinueOnTransferError))

	type result struct {
		identity tlsutil.Identity
		err      error
	}
	results := make(chan result, len(expected))
	var wg sync.WaitGroup
	go func() {
		<-ctx.Done()
		_ = tlsListener.Close()
	}()
	completed := map[tlsutil.Identity]struct{}{}
	for len(completed) < len(expected) {
		logLine(opts.Log, "waiting for source transfer", "completed", fmt.Sprintf("%d", len(completed)), "expected", fmt.Sprintf("%d", len(expected)))
		conn, err := tlsListener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				wg.Wait()
				return ctx.Err()
			}
			return err
		}
		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			identity, err := receiveTransfer(root, expected, conn, opts.Mirror, opts.Log)
			results <- result{identity: identity, err: err}
		}(conn)
		res := <-results
		if res.err != nil {
			logLine(opts.Log, "source transfer failed", "identity", res.identity.URI(), "error", res.err.Error())
			if opts.ContinueOnTransferError {
				continue
			}
			_ = tlsListener.Close()
			wg.Wait()
			return res.err
		}
		completed[res.identity] = struct{}{}
		logLine(opts.Log, "source transfer completed", "identity", res.identity.URI(), "completed", fmt.Sprintf("%d", len(completed)), "expected", fmt.Sprintf("%d", len(expected)))
	}
	_ = tlsListener.Close()
	wg.Wait()
	return nil
}

func ServeRestoreTarget(ctx context.Context, listener net.Listener, opts RestoreTargetOptions) error {
	if opts.TargetRoot == "" {
		return fmt.Errorf("target root is required")
	}
	if opts.Writer.Identity == (tlsutil.Identity{}) {
		return fmt.Errorf("restore writer identity is required")
	}
	snapshot, err := NormalizeRelativePath(opts.Writer.Snapshot)
	if err != nil {
		return fmt.Errorf("invalid restore snapshot: %w", err)
	}
	source, err := NormalizeTargetSubpath(opts.Writer.Source)
	if err != nil {
		return fmt.Errorf("invalid restore source: %w", err)
	}
	root, err := filepath.Abs(opts.TargetRoot)
	if err != nil {
		return err
	}
	config, err := tlsutil.TLSConfig(opts.TLSBundle, tlsutil.Identity{}, "")
	if err != nil {
		return err
	}
	tlsListener := tls.NewListener(listener, config)
	defer tlsListener.Close()
	logLine(opts.Log, "restore target listening", "address", listener.Addr().String(), "targetRoot", root, "expectedWriter", opts.Writer.Identity.URI(), "snapshot", snapshot, "source", source)
	go func() {
		<-ctx.Done()
		_ = tlsListener.Close()
	}()
	conn, err := tlsListener.Accept()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	return serveRestoreTransfer(root, ExpectedRestoreWriter{
		Identity: opts.Writer.Identity,
		Snapshot: snapshot,
		Source:   source,
	}, conn, opts.Log)
}

func SendSource(ctx context.Context, opts SourceSenderOptions) error {
	if opts.Source == "" || opts.Destination == "" || opts.TargetEndpoint == "" {
		return fmt.Errorf("source, destination, and target endpoint are required")
	}
	if _, err := NormalizeTargetSubpath(opts.Destination); err != nil {
		return fmt.Errorf("invalid destination: %w", err)
	}
	if opts.DryRun {
		_, err := fmt.Fprintf(opts.Stdout, "rsync-over-mtls source=%q targetEndpoint=%q destination=%q\n", opts.Source, opts.TargetEndpoint, opts.Destination)
		return err
	}
	if _, err := exec.LookPath("rsync"); err != nil {
		return fmt.Errorf("rsync is required for source transfers: %w", err)
	}
	return sendSourceRsyncTunnel(ctx, opts)
}

func sendSourceRsyncTunnel(ctx context.Context, opts SourceSenderOptions) error {
	config, err := tlsutil.TLSConfig(opts.TLSBundle, opts.ExpectedTarget, "")
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen for local rsync bridge: %w", err)
	}
	defer listener.Close()
	logLine(opts.Stdout, "source sender bridge listening", "address", listener.Addr().String(), "source", opts.Source, "destination", opts.Destination, "targetEndpoint", opts.TargetEndpoint, "expectedTarget", opts.ExpectedTarget.URI())
	errCh := make(chan error, 1)
	go func() {
		errCh <- bridgeLocalRsyncToTarget(ctx, listener, opts, config)
	}()
	args := rsyncTunnelClientArgs(opts, listener.Addr().String())
	logLine(opts.Stdout, "source sender executing local rsync into bridge", "source", opts.Source, "bridge", listener.Addr().String(), "destination", opts.Destination)
	_, runErr := runRsyncCommand(ctx, args, opts.Stdout, opts.Stderr, opts.Progress)
	_ = listener.Close()
	bridgeErr := <-errCh
	if runErr != nil {
		return runErr
	}
	return bridgeErr
}

func bridgeLocalRsyncToTarget(ctx context.Context, listener net.Listener, opts SourceSenderOptions, config *tls.Config) error {
	localConn, err := listener.Accept()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	defer localConn.Close()
	dialer := &tls.Dialer{Config: config}
	logLine(opts.Stdout, "source sender connecting to target receiver", "targetEndpoint", opts.TargetEndpoint, "expectedTarget", opts.ExpectedTarget.URI())
	targetConn, err := dialer.DialContext(ctx, "tcp", opts.TargetEndpoint)
	if err != nil {
		return fmt.Errorf("connect target receiver: %w", err)
	}
	defer targetConn.Close()
	header := transferHeader{Destination: opts.Destination, Protocol: transferProtocolRsync}
	logLine(opts.Stdout, "source sender sending transfer header", "destination", header.Destination, "protocol", header.Protocol)
	if err := writeTransferHeader(targetConn, header); err != nil {
		return err
	}
	return proxyConns(localConn, targetConn)
}

func rsyncTunnelClientArgs(opts SourceSenderOptions, bridgeAddr string) []string {
	args := rsyncArgs(RsyncOptions{Delete: opts.Delete, OneFileSystem: opts.OneFileSystem})
	args = append(args, slash(opts.Source), "rsync://"+bridgeAddr+"/krm/")
	return args
}

func ReceiveRestore(ctx context.Context, opts RestoreWriterOptions) error {
	if opts.Destination == "" || opts.Snapshot == "" || opts.Source == "" || opts.TargetEndpoint == "" {
		return fmt.Errorf("destination, snapshot, source, and target endpoint are required")
	}
	snapshot, err := NormalizeRelativePath(opts.Snapshot)
	if err != nil {
		return fmt.Errorf("invalid restore snapshot: %w", err)
	}
	source, err := NormalizeTargetSubpath(opts.Source)
	if err != nil {
		return fmt.Errorf("invalid restore source: %w", err)
	}
	if opts.DryRun {
		_, err := fmt.Fprintf(opts.Stdout, "krm-restore targetEndpoint=%q snapshot=%q source=%q destination=%q\n", opts.TargetEndpoint, snapshot, source, opts.Destination)
		return err
	}
	if _, err := exec.LookPath("rsync"); err != nil {
		return fmt.Errorf("rsync is required for restore transfers: %w", err)
	}
	return receiveRestoreRsyncTunnel(ctx, opts, snapshot, source)
}

func receiveRestoreRsyncTunnel(ctx context.Context, opts RestoreWriterOptions, snapshot, source string) error {
	config, err := tlsutil.TLSConfig(opts.TLSBundle, opts.ExpectedTarget, "")
	if err != nil {
		return err
	}
	destinationRoot, err := filepath.Abs(opts.Destination)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(destinationRoot, 0o755); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen for local restore rsync bridge: %w", err)
	}
	defer listener.Close()
	logLine(opts.Stdout, "restore writer bridge listening", "address", listener.Addr().String(), "snapshot", snapshot, "source", source, "destination", destinationRoot, "targetEndpoint", opts.TargetEndpoint, "expectedTarget", opts.ExpectedTarget.URI())
	errCh := make(chan error, 1)
	go func() {
		errCh <- bridgeLocalRsyncFromTarget(ctx, listener, opts, config, snapshot, source)
	}()
	args := rsyncRestoreClientArgs(opts, listener.Addr().String())
	logLine(opts.Stdout, "restore writer executing local rsync from bridge", "bridge", listener.Addr().String(), "destination", destinationRoot)
	_, runErr := runRsyncCommand(ctx, args, opts.Stdout, opts.Stderr, opts.Progress)
	_ = listener.Close()
	bridgeErr := <-errCh
	if runErr != nil {
		return runErr
	}
	return bridgeErr
}

func bridgeLocalRsyncFromTarget(ctx context.Context, listener net.Listener, opts RestoreWriterOptions, config *tls.Config, snapshot, source string) error {
	localConn, err := listener.Accept()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	defer localConn.Close()
	dialer := &tls.Dialer{Config: config}
	logLine(opts.Stdout, "restore writer connecting to target", "targetEndpoint", opts.TargetEndpoint, "expectedTarget", opts.ExpectedTarget.URI())
	targetConn, err := dialer.DialContext(ctx, "tcp", opts.TargetEndpoint)
	if err != nil {
		return fmt.Errorf("connect restore target: %w", err)
	}
	defer targetConn.Close()
	header := transferHeader{Snapshot: snapshot, Source: source, Protocol: transferProtocolRsync}
	logLine(opts.Stdout, "restore writer sending transfer header", "snapshot", header.Snapshot, "source", header.Source, "protocol", header.Protocol)
	if err := writeTransferHeader(targetConn, header); err != nil {
		return err
	}
	return proxyConns(localConn, targetConn)
}

func rsyncRestoreClientArgs(opts RestoreWriterOptions, bridgeAddr string) []string {
	args := rsyncArgs(RsyncOptions{Delete: opts.Delete, OneFileSystem: opts.OneFileSystem})
	args = append(args, "rsync://"+bridgeAddr+"/krm/", slash(opts.Destination))
	return args
}

func receiveTransfer(root string, expected map[tlsutil.Identity]string, conn net.Conn, mirror bool, log io.Writer) (tlsutil.Identity, error) {
	defer conn.Close()
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return tlsutil.Identity{}, fmt.Errorf("connection is not TLS")
	}
	if err := tlsConn.Handshake(); err != nil {
		return tlsutil.Identity{}, err
	}
	identity, err := peerIdentity(tlsConn.ConnectionState())
	if err != nil {
		return tlsutil.Identity{}, err
	}
	expectedDestination, ok := expected[identity]
	if !ok {
		return identity, fmt.Errorf("unexpected sender identity %s", identity.URI())
	}
	logLine(log, "target receiver accepted source", "identity", identity.URI(), "expectedDestination", expectedDestination)
	header, err := readTransferHeader(tlsConn)
	if err != nil {
		return identity, err
	}
	destination, err := NormalizeTargetSubpath(header.Destination)
	if err != nil {
		return identity, fmt.Errorf("invalid sender destination: %w", err)
	}
	if destination != expectedDestination {
		return identity, fmt.Errorf("sender %s requested destination %q, expected %q", identity.URI(), destination, expectedDestination)
	}
	destinationRoot := filepath.Join(root, filepath.FromSlash(destination))
	logLine(log, "target receiver preparing destination", "identity", identity.URI(), "destination", destination, "path", destinationRoot)
	seeded, err := prepareTransferDestination(root, destination, mirror)
	if err != nil {
		return identity, err
	}
	logLine(log, "target receiver prepared destination", "identity", identity.URI(), "destination", destination, "path", destinationRoot, "seededFromLatest", fmt.Sprintf("%t", seeded))
	if header.Protocol != transferProtocolRsync {
		return identity, fmt.Errorf("unsupported transfer protocol %q", header.Protocol)
	}
	logLine(log, "target receiver starting rsync daemon for source", "identity", identity.URI(), "destination", destinationRoot)
	if err := receiveRsyncTunnel(tlsConn, destinationRoot); err != nil {
		return identity, err
	}
	return identity, nil
}

func seedPartialDestination(root, destination string) (bool, error) {
	destination, err := NormalizeRelativePath(destination)
	if err != nil {
		return false, fmt.Errorf("invalid destination: %w", err)
	}
	partialPrefix := filepath.ToSlash(filepath.Join(".partial"))
	if destination != partialPrefix && !strings.HasPrefix(destination, partialPrefix+"/") {
		return false, fmt.Errorf("destination %q is not under .partial", destination)
	}
	parts := strings.Split(destination, "/")
	if len(parts) < 3 || parts[0] != ".partial" || parts[1] == "" {
		return false, fmt.Errorf("destination %q must be under .partial/<run>", destination)
	}
	latestSource := filepath.Join(root, "latest", filepath.FromSlash(strings.Join(parts[2:], "/")))
	destinationRoot := filepath.Join(root, filepath.FromSlash(destination))
	if err := ensureInside(root, destinationRoot); err != nil {
		return false, err
	}
	if err := ensureInside(root, latestSource); err != nil {
		return false, err
	}
	if err := os.RemoveAll(destinationRoot); err != nil {
		return false, err
	}
	if info, err := os.Stat(latestSource); err == nil {
		if !info.IsDir() {
			return false, fmt.Errorf("latest source %s is not a directory", latestSource)
		}
		if err := linkTree(latestSource, destinationRoot); err != nil {
			_ = os.RemoveAll(destinationRoot)
			return false, fmt.Errorf("seed destination from latest: %w", err)
		}
		return true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.MkdirAll(destinationRoot, 0o755); err != nil {
		return false, err
	}
	return false, nil
}

func prepareTransferDestination(root, destination string, mirror bool) (bool, error) {
	if !mirror {
		return seedPartialDestination(root, destination)
	}
	destination, err := NormalizeTargetSubpath(destination)
	if err != nil {
		return false, fmt.Errorf("invalid destination: %w", err)
	}
	destinationRoot := filepath.Join(root, filepath.FromSlash(destination))
	if err := ensureInside(root, destinationRoot); err != nil {
		return false, err
	}
	if err := os.MkdirAll(destinationRoot, 0o755); err != nil {
		return false, err
	}
	return false, nil
}

func receiveRsyncTunnel(conn net.Conn, destinationRoot string) error {
	daemon, cleanup, err := startRsyncDaemon(destinationRoot, false)
	if err != nil {
		return err
	}
	defer cleanup()
	daemonConn, err := dialRsyncDaemon(daemon)
	if err != nil {
		return err
	}
	defer daemonConn.Close()
	return proxyConns(conn, daemonConn)
}

func startRsyncDaemon(destinationRoot string, readOnly bool) (string, func(), error) {
	if _, err := exec.LookPath("rsync"); err != nil {
		return "", func() {}, fmt.Errorf("rsync is required for rsync-over-mtls transfers: %w", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", func() {}, err
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	configFile, err := os.CreateTemp("", "krm-rsyncd-*.conf")
	if err != nil {
		return "", func() {}, err
	}
	configPath := configFile.Name()
	config := rsyncDaemonConfig(destinationRoot, readOnly)
	if _, err := configFile.WriteString(config); err != nil {
		_ = configFile.Close()
		_ = os.Remove(configPath)
		return "", func() {}, err
	}
	if err := configFile.Close(); err != nil {
		_ = os.Remove(configPath)
		return "", func() {}, err
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		_ = os.Remove(configPath)
		return "", func() {}, err
	}
	var stderr bytes.Buffer
	cmd := exec.Command("rsync", "--daemon", "--no-detach", "--address="+host, "--port="+port, "--config="+configPath)
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		_ = os.Remove(configPath)
		return "", func() {}, err
	}
	cleanup := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
		_ = os.Remove(configPath)
	}
	return addr, cleanup, nil
}

func rsyncDaemonConfig(destinationRoot string, readOnly bool) string {
	config := "use chroot = false\nnumeric ids = yes\n"
	if os.Geteuid() == 0 {
		config += fmt.Sprintf("uid = 0\ngid = %d\n", os.Getegid())
	}
	return fmt.Sprintf("%sread only = %t\nlist = false\n[krm]\n\tpath = %s\n", config, readOnly, destinationRoot)
}

func dialRsyncDaemon(addr string) (net.Conn, error) {
	var lastErr error
	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	return nil, fmt.Errorf("connect local rsync daemon: %w", lastErr)
}

func proxyConns(a, b net.Conn) error {
	errCh := make(chan error, 2)
	go func() {
		_, err := io.Copy(a, b)
		errCh <- err
		_ = a.Close()
		_ = b.Close()
	}()
	go func() {
		_, err := io.Copy(b, a)
		errCh <- err
		_ = a.Close()
		_ = b.Close()
	}()
	err := <-errCh
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func serveRestoreTransfer(root string, expected ExpectedRestoreWriter, conn net.Conn, log io.Writer) error {
	defer conn.Close()
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return fmt.Errorf("connection is not TLS")
	}
	if err := tlsConn.Handshake(); err != nil {
		return err
	}
	identity, err := peerIdentity(tlsConn.ConnectionState())
	if err != nil {
		return err
	}
	if identity != expected.Identity {
		return fmt.Errorf("unexpected restore writer identity %s", identity.URI())
	}
	logLine(log, "restore target accepted writer", "identity", identity.URI())
	header, err := readTransferHeader(tlsConn)
	if err != nil {
		return err
	}
	snapshot, err := NormalizeRelativePath(header.Snapshot)
	if err != nil {
		return fmt.Errorf("invalid requested restore snapshot: %w", err)
	}
	source, err := NormalizeTargetSubpath(header.Source)
	if err != nil {
		return fmt.Errorf("invalid requested restore source: %w", err)
	}
	if snapshot != expected.Snapshot || source != expected.Source {
		return fmt.Errorf("restore writer requested %q/%q, expected %q/%q", snapshot, source, expected.Snapshot, expected.Source)
	}
	sourceRoot := filepath.Join(root, filepath.FromSlash(snapshot), filepath.FromSlash(source))
	if snapshot == "current" {
		sourceRoot = filepath.Join(root, filepath.FromSlash(source))
	}
	logLine(log, "restore target validating requested source", "snapshot", snapshot, "source", source, "path", sourceRoot)
	if err := ensureInside(root, sourceRoot); err != nil {
		return err
	}
	info, err := os.Stat(sourceRoot)
	if err != nil {
		return fmt.Errorf("restore source %q is not accessible: %w", pathForError(snapshot, source), err)
	}
	if !info.IsDir() {
		return fmt.Errorf("restore source %q is not a directory", pathForError(snapshot, source))
	}
	if header.Protocol != transferProtocolRsync {
		return fmt.Errorf("unsupported restore transfer protocol %q", header.Protocol)
	}
	logLine(log, "restore target starting read-only rsync daemon", "path", sourceRoot)
	daemon, cleanup, err := startRsyncDaemon(sourceRoot, true)
	if err != nil {
		return err
	}
	defer cleanup()
	daemonConn, err := dialRsyncDaemon(daemon)
	if err != nil {
		return err
	}
	defer daemonConn.Close()
	return proxyConns(tlsConn, daemonConn)
}

func peerIdentity(state tls.ConnectionState) (tlsutil.Identity, error) {
	if len(state.PeerCertificates) == 0 {
		return tlsutil.Identity{}, fmt.Errorf("peer certificate is required")
	}
	for _, uri := range state.PeerCertificates[0].URIs {
		identity, err := tlsutil.ParseIdentity(uri.String())
		if err == nil {
			return identity, nil
		}
	}
	return tlsutil.Identity{}, fmt.Errorf("peer certificate does not contain a kube-rsync-machine identity")
}

func pathForError(parts ...string) string {
	return strings.Join(parts, "/")
}

func writeTransferHeader(w io.Writer, header transferHeader) error {
	data, err := json.Marshal(header)
	if err != nil {
		return err
	}
	if len(data) > maxTransferHeaderBytes {
		return fmt.Errorf("transfer header is too large")
	}
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(data)))
	if _, err := w.Write(size[:]); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func readTransferHeader(r io.Reader) (transferHeader, error) {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return transferHeader{}, err
	}
	length := binary.BigEndian.Uint32(size[:])
	if length == 0 || length > maxTransferHeaderBytes {
		return transferHeader{}, fmt.Errorf("invalid transfer header size %d", length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return transferHeader{}, err
	}
	var header transferHeader
	if err := json.Unmarshal(data, &header); err != nil {
		return transferHeader{}, err
	}
	return header, nil
}

func ParseExpectedTransferSources(runNamespace, runName, runID string, values []string) ([]ExpectedTransferSource, []string, error) {
	return ParseExpectedTransferSourcesWithStrategy(runNamespace, runName, runID, false, values)
}

func ParseExpectedTransferSourcesWithStrategy(runNamespace, runName, runID string, mirror bool, values []string) ([]ExpectedTransferSource, []string, error) {
	out := make([]ExpectedTransferSource, 0, len(values))
	finalizeSources := make([]string, 0, len(values))
	for _, value := range values {
		sourceNamespace, sourceName, destination, err := parseSourceSpec(value)
		if err != nil {
			return nil, nil, err
		}
		finalizeDestination, err := NormalizeTargetSubpath(destination)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid source destination %q: %w", destination, err)
		}
		finalizeSources = append(finalizeSources, finalizeDestination)
		if sourceNamespace == "" || sourceName == "" {
			continue
		}
		transferDestination := filepath.ToSlash(filepath.Join(".partial", runID, finalizeDestination))
		if mirror {
			transferDestination = finalizeDestination
			if transferDestination == "" {
				transferDestination = "."
			}
		}
		out = append(out, ExpectedTransferSource{
			Identity:    tlsutil.SourceIdentity(runNamespace, runName, sourceNamespace, sourceName),
			Destination: transferDestination,
		})
	}
	sort.Strings(finalizeSources)
	return out, finalizeSources, nil
}

func parseSourceSpec(value string) (string, string, string, error) {
	left, right, ok := strings.Cut(value, "=")
	if !ok {
		return "", "", value, nil
	}
	namespace, name, ok := strings.Cut(left, "/")
	if !ok || namespace == "" || name == "" {
		return "", "", "", fmt.Errorf("source identity %q must be namespace/name", left)
	}
	if right == "" {
		return "", "", "", fmt.Errorf("source destination is required")
	}
	return namespace, name, right, nil
}
