package dataplane

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

const SnapshotTimestampLayout = "2006-01-02T15-04-05Z"

type RetentionPolicy struct {
	Hourly  int
	Daily   int
	Weekly  int
	Monthly int
}

type RestorePoint struct {
	Snapshot         string
	ResolvesTo       string
	Tier             string
	CreatedAt        time.Time
	BytesTransferred int64
}

type TargetLock struct {
	path  string
	runID string
}

type FinalizeOptions struct {
	TargetRoot string
	RunID      string
	Timestamp  string
	Sources    []string
	Retention  RetentionPolicy
}

func FinalizeBackup(opts FinalizeOptions) ([]string, error) {
	if opts.TargetRoot == "" || opts.RunID == "" || opts.Timestamp == "" {
		return nil, errors.New("target root, run id, and timestamp are required")
	}
	if err := validateRunID(opts.RunID); err != nil {
		return nil, err
	}
	ts, err := time.Parse(SnapshotTimestampLayout, opts.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp %q: %w", opts.Timestamp, err)
	}
	root, err := filepath.Abs(opts.TargetRoot)
	if err != nil {
		return nil, err
	}
	partial := filepath.Join(root, ".partial", opts.RunID)
	if err := ensureInside(root, partial); err != nil {
		return nil, err
	}
	hourlyName := filepath.ToSlash(filepath.Join("hourly", opts.Timestamp))
	hourlyPath := filepath.Join(root, filepath.FromSlash(hourlyName))
	info, err := os.Stat(partial)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if points, finalizeErr := finalizePromotedSnapshot(root, hourlyName, hourlyPath, ts, opts.Sources, opts.Retention); finalizeErr == nil {
				return points, nil
			}
		}
		return nil, fmt.Errorf("partial run is not ready: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("partial run %s is not a directory", partial)
	}
	if err := validateSnapshotSources(partial, opts.Sources); err != nil {
		return nil, err
	}
	if _, err := os.Stat(hourlyPath); err == nil {
		return finalizePromotedSnapshot(root, hourlyName, hourlyPath, ts, opts.Sources, opts.Retention)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(hourlyPath), 0o755); err != nil {
		return nil, err
	}
	if err := os.Rename(partial, hourlyPath); err != nil {
		return nil, fmt.Errorf("promote partial snapshot: %w", err)
	}
	return finalizePromotedSnapshot(root, hourlyName, hourlyPath, ts, opts.Sources, opts.Retention)
}

func finalizePromotedSnapshot(root, hourlyName, hourlyPath string, ts time.Time, sources []string, retention RetentionPolicy) ([]string, error) {
	if err := validateSnapshotSources(hourlyPath, sources); err != nil {
		return nil, err
	}
	points := []string{hourlyName}
	if err := replaceLatestSymlink(root, hourlyName); err != nil {
		return points, fmt.Errorf("refresh latest: %w", err)
	}
	points = append(points, "latest -> "+hourlyName)
	tierNames := tierSnapshotNames(ts)
	for _, name := range tierNames {
		dst := filepath.Join(root, filepath.FromSlash(name))
		if _, err := os.Stat(dst); errors.Is(err, os.ErrNotExist) {
			if err := linkTree(hourlyPath, dst); err != nil {
				return points, fmt.Errorf("create tier snapshot %s: %w", name, err)
			}
			points = append(points, name)
		} else if err != nil {
			return points, err
		}
	}
	if err := applyRetention(root, retention); err != nil {
		return points, err
	}
	return points, nil
}

func validateSnapshotSources(snapshotPath string, sources []string) error {
	for _, source := range sources {
		if _, err := NormalizeRelativePath(source); err != nil {
			return fmt.Errorf("invalid source %q: %w", source, err)
		}
		if _, err := os.Stat(filepath.Join(snapshotPath, filepath.FromSlash(source))); err != nil {
			return fmt.Errorf("expected source %q is missing from snapshot: %w", source, err)
		}
	}
	return nil
}

func NormalizeDestinationPath(namespace, destinationPath string) (string, error) {
	namespace, err := NormalizeRelativePath(namespace)
	if err != nil {
		return "", fmt.Errorf("invalid namespace: %w", err)
	}
	if strings.Contains(namespace, "/") {
		return "", errors.New("namespace must be a single path segment")
	}
	if destinationPath == "" || destinationPath == "/" {
		return namespace, nil
	}
	destinationPath, err = NormalizeRelativePath(destinationPath)
	if err != nil {
		return "", fmt.Errorf("invalid destination path: %w", err)
	}
	return namespace + "/" + destinationPath, nil
}

func NormalizeRelativePath(value string) (string, error) {
	if value == "" {
		return "", errors.New("path is empty")
	}
	if strings.HasPrefix(value, "/") {
		return "", errors.New("absolute paths are not allowed")
	}
	if strings.Contains(value, "\\") {
		return "", errors.New("backslash path separators are not allowed")
	}
	parts := strings.Split(value, "/")
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		switch part {
		case "", ".":
			continue
		case "..":
			return "", errors.New("path must stay below the target root")
		default:
			cleaned = append(cleaned, part)
		}
	}
	if len(cleaned) == 0 {
		return "", errors.New("path must contain at least one segment")
	}
	return strings.Join(cleaned, "/"), nil
}

func NormalizeTargetSubpath(value string) (string, error) {
	if value == "" || value == "." || value == "/" {
		return "", nil
	}
	return NormalizeRelativePath(value)
}

func ScanRestorePoints(root string) ([]RestorePoint, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	points := []RestorePoint{}
	latestTarget, err := latestSnapshotTarget(root)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(root, "latest")); err == nil {
		points = append(points, RestorePoint{
			Snapshot:   "latest",
			ResolvesTo: latestTarget,
			CreatedAt:  parseSnapshotTime(latestTarget),
		})
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for _, tier := range []string{"hourly", "daily", "weekly", "monthly"} {
		dir := filepath.Join(root, tier)
		entries, err := os.ReadDir(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			snapshot := filepath.ToSlash(filepath.Join(tier, entry.Name()))
			points = append(points, RestorePoint{
				Snapshot:  snapshot,
				Tier:      tier,
				CreatedAt: parseSnapshotTime(snapshot),
			})
		}
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].Snapshot == "latest" {
			return true
		}
		if points[j].Snapshot == "latest" {
			return false
		}
		return points[i].Snapshot > points[j].Snapshot
	})
	return points, nil
}

func AcquireTargetLock(root, runID string) (*TargetLock, error) {
	if err := validateRunID(runID); err != nil {
		return nil, err
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(root, ".krm-run.lock")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			data, readErr := os.ReadFile(path)
			if readErr == nil {
				return nil, fmt.Errorf("target is locked: %s", strings.TrimSpace(string(data)))
			}
		}
		return nil, err
	}
	defer file.Close()
	hostname, _ := os.Hostname()
	_, err = fmt.Fprintf(file, "runID=%s\ncreatedAt=%s\nhostname=%s\npid=%d\n", runID, time.Now().UTC().Format(time.RFC3339), hostname, os.Getpid())
	if err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	return &TargetLock{path: path, runID: runID}, nil
}

func (l *TargetLock) Release() error {
	if l == nil {
		return nil
	}
	data, err := os.ReadFile(l.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !lockDataMatchesRunID(data, l.runID) {
		return fmt.Errorf("lock %s is no longer owned by run %s", l.path, l.runID)
	}
	return os.Remove(l.path)
}

func CleanupStalePartials(root string, activeRunIDs []string) ([]string, error) {
	active := map[string]struct{}{}
	for _, runID := range activeRunIDs {
		if runID == "" {
			continue
		}
		if err := validateRunID(runID); err != nil {
			return nil, err
		}
		active[runID] = struct{}{}
	}
	partialRoot := filepath.Join(root, ".partial")
	entries, err := os.ReadDir(partialRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	removed := []string{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, ok := active[entry.Name()]; ok {
			continue
		}
		path := filepath.Join(partialRoot, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			return removed, err
		}
		removed = append(removed, entry.Name())
	}
	sort.Strings(removed)
	return removed, nil
}

func ApplyRetention(root string, policy RetentionPolicy) ([]string, error) {
	removed := []string{}
	tiers := map[string]int{
		"hourly":  policy.Hourly,
		"daily":   policy.Daily,
		"weekly":  policy.Weekly,
		"monthly": policy.Monthly,
	}
	for _, tier := range []string{"hourly", "daily", "weekly", "monthly"} {
		keep := tiers[tier]
		if keep < 0 {
			continue
		}
		dir := filepath.Join(root, tier)
		entries, err := os.ReadDir(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return removed, err
		}
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			if entry.IsDir() {
				names = append(names, entry.Name())
			}
		}
		sort.Strings(names)
		for len(names) > keep {
			name := filepath.ToSlash(filepath.Join(tier, names[0]))
			if err := os.RemoveAll(filepath.Join(root, filepath.FromSlash(name))); err != nil {
				return removed, err
			}
			removed = append(removed, name)
			names = names[1:]
		}
	}
	if err := cleanupDanglingLatest(root); err != nil {
		return removed, err
	}
	return removed, nil
}

func RecoverSpace(root string, minAvailableBytes uint64, protectedSnapshots []string) ([]string, error) {
	available, err := AvailableBytes(root)
	if err != nil {
		return nil, err
	}
	if available >= minAvailableBytes {
		return nil, nil
	}
	protected := map[string]struct{}{}
	for _, snapshot := range protectedSnapshots {
		normalized, err := NormalizeRelativePath(snapshot)
		if err != nil {
			return nil, fmt.Errorf("invalid protected snapshot %q: %w", snapshot, err)
		}
		protected[normalized] = struct{}{}
	}
	candidates, err := removableSnapshots(root)
	if err != nil {
		return nil, err
	}
	removed := []string{}
	for _, candidate := range candidates {
		if _, ok := protected[candidate]; ok {
			continue
		}
		if err := os.RemoveAll(filepath.Join(root, filepath.FromSlash(candidate))); err != nil {
			return removed, err
		}
		removed = append(removed, candidate)
		available, err = AvailableBytes(root)
		if err != nil {
			return removed, err
		}
		if available >= minAvailableBytes {
			return removed, nil
		}
	}
	return removed, fmt.Errorf("available space %d is below requested minimum %d after pruning %d snapshots", available, minAvailableBytes, len(removed))
}

func AvailableBytes(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return uint64(stat.Bavail) * uint64(stat.Bsize), nil
}

func tierSnapshotNames(ts time.Time) []string {
	return []string{
		filepath.ToSlash(filepath.Join("daily", ts.Format("2006-01-02"))),
		filepath.ToSlash(filepath.Join("weekly", weekStart(ts).Format("2006-01-02"))),
		filepath.ToSlash(filepath.Join("monthly", ts.Format("2006-01"))),
	}
}

func weekStart(ts time.Time) time.Time {
	year, month, day := ts.Date()
	start := time.Date(year, month, day, 0, 0, 0, 0, ts.Location())
	offset := (int(start.Weekday()) + 6) % 7
	return start.AddDate(0, 0, -offset)
}

func replaceLatestSymlink(root, target string) error {
	if _, err := NormalizeRelativePath(target); err != nil {
		return fmt.Errorf("invalid latest target %q: %w", target, err)
	}
	targetPath := filepath.Join(root, filepath.FromSlash(target))
	if err := ensureInside(root, targetPath); err != nil {
		return err
	}
	if info, err := os.Stat(targetPath); err != nil {
		return err
	} else if !info.IsDir() {
		return fmt.Errorf("latest target %s is not a directory", targetPath)
	}
	latest := filepath.Join(root, "latest")
	tmp := latest + ".tmp"
	_ = os.RemoveAll(tmp)
	if err := os.Symlink(filepath.ToSlash(target), tmp); err != nil {
		return err
	}
	_ = os.RemoveAll(latest)
	return os.Rename(tmp, latest)
}

func latestSnapshotTarget(root string) (string, error) {
	latest := filepath.Join(root, "latest")
	info, err := os.Lstat(latest)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return newestSnapshot(root, "hourly")
	}
	target, err := os.Readlink(latest)
	if err != nil {
		return "", err
	}
	normalized, err := NormalizeRelativePath(target)
	if err != nil {
		return "", fmt.Errorf("invalid latest target %q: %w", target, err)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(normalized))); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	return normalized, nil
}

func cleanupDanglingLatest(root string) error {
	latest := filepath.Join(root, "latest")
	info, err := os.Lstat(latest)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return nil
	}
	target, err := os.Readlink(latest)
	if err != nil {
		return err
	}
	normalized, err := NormalizeRelativePath(target)
	if err != nil {
		return os.Remove(latest)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(normalized))); errors.Is(err, os.ErrNotExist) {
		return os.Remove(latest)
	} else if err != nil {
		return err
	}
	return nil
}

func linkTree(src, dst string) error {
	src, err := filepath.Abs(src)
	if err != nil {
		return err
	}
	dst, err = filepath.Abs(dst)
	if err != nil {
		return err
	}
	return filepath.WalkDir(src, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if rel == "." {
			return os.MkdirAll(target, 0o755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		mode := info.Mode()
		switch {
		case mode.IsDir():
			return os.MkdirAll(target, mode.Perm())
		case mode.Type() == 0:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			return os.Link(path, target)
		case mode&os.ModeSymlink != 0:
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			return os.Symlink(link, target)
		default:
			return nil
		}
	})
}

func applyRetention(root string, policy RetentionPolicy) error {
	_, err := ApplyRetention(root, policy)
	return err
}

func ensureInside(root, path string) error {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return fmt.Errorf("%s escapes root %s", path, root)
	}
	return nil
}

func newestSnapshot(root, tier string) (string, error) {
	dir := filepath.Join(root, tier)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	names := []string{}
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	if len(names) == 0 {
		return "", nil
	}
	sort.Strings(names)
	return filepath.ToSlash(filepath.Join(tier, names[len(names)-1])), nil
}

func parseSnapshotTime(snapshot string) time.Time {
	parts := strings.Split(snapshot, "/")
	if len(parts) != 2 {
		return time.Time{}
	}
	switch parts[0] {
	case "hourly":
		ts, _ := time.Parse(SnapshotTimestampLayout, parts[1])
		return ts
	case "daily", "weekly":
		ts, _ := time.Parse("2006-01-02", parts[1])
		return ts
	case "monthly":
		ts, _ := time.Parse("2006-01", parts[1])
		return ts
	default:
		return time.Time{}
	}
}

func lockDataMatchesRunID(data []byte, runID string) bool {
	for _, line := range strings.Split(string(data), "\n") {
		if line == "runID="+runID {
			return true
		}
	}
	return false
}

func validateRunID(runID string) error {
	if runID == "" {
		return errors.New("run id is required")
	}
	normalized, err := NormalizeRelativePath(runID)
	if err != nil {
		return fmt.Errorf("invalid run id %q: %w", runID, err)
	}
	if normalized != runID || strings.Contains(runID, "/") || strings.ContainsAny(runID, "\r\n=") {
		return fmt.Errorf("invalid run id %q", runID)
	}
	return nil
}

func removableSnapshots(root string) ([]string, error) {
	type snapshotInfo struct {
		name    string
		modTime time.Time
	}
	infos := []snapshotInfo{}
	for _, tier := range []string{"hourly", "daily", "weekly", "monthly"} {
		dir := filepath.Join(root, tier)
		entries, err := os.ReadDir(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			infos = append(infos, snapshotInfo{
				name:    filepath.ToSlash(filepath.Join(tier, entry.Name())),
				modTime: info.ModTime(),
			})
		}
	}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].modTime.Equal(infos[j].modTime) {
			return infos[i].name < infos[j].name
		}
		return infos[i].modTime.Before(infos[j].modTime)
	})
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		names = append(names, info.name)
	}
	return names, nil
}
