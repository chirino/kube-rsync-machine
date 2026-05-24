package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

type ResolvedSource struct {
	Ref                      types.NamespacedName
	Source                   krmv1alpha1.BackupSource
	EffectiveDestinationPath string
}

type TargetRunSet struct {
	MachineRef types.NamespacedName
	Machines   []krmv1alpha1.ObjectReference
	Sources    []ResolvedSource
	Retention  krmv1alpha1.RetentionPolicy
}

type DestinationPathConflict struct {
	MachineRef      types.NamespacedName
	SourceNamespace string
	DestinationPath string
	Sources         []types.NamespacedName
	Machines        []types.NamespacedName
}

type DestinationPathOverlap struct {
	MachineRef types.NamespacedName
	Path       string
	Sources    []types.NamespacedName
}

func ResolveObjectReference(ref krmv1alpha1.ObjectReference, defaultNamespace string) (types.NamespacedName, error) {
	if ref.Name == "" {
		return types.NamespacedName{}, errors.New("reference name is required")
	}
	namespace := ref.NamespaceOr(defaultNamespace)
	if namespace == "" {
		return types.NamespacedName{}, fmt.Errorf("namespace is required for reference %q", ref.Name)
	}
	return types.NamespacedName{Namespace: namespace, Name: ref.Name}, nil
}

func EffectiveDestinationPath(source krmv1alpha1.BackupSource) (string, error) {
	return effectiveDestinationPath(source.Namespace, source.Spec.DestinationPath)
}

func EffectiveDestinationPathForStrategy(machine krmv1alpha1.RsyncMachine, source krmv1alpha1.BackupSource) (string, error) {
	if machine.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror {
		return effectiveMirrorDestinationPath(source.Spec.DestinationPath)
	}
	return EffectiveDestinationPath(source)
}

func effectiveDestinationPath(sourceNamespace, destinationPath string) (string, error) {
	if sourceNamespace == "" {
		return "", errors.New("source namespace is required")
	}
	if destinationPath == "" || destinationPath == "/" {
		return sourceNamespace, nil
	}
	cleaned, err := cleanRelativePath(destinationPath)
	if err != nil {
		return "", err
	}
	return path.Join(sourceNamespace, cleaned), nil
}

func effectiveMirrorDestinationPath(destinationPath string) (string, error) {
	if destinationPath == "" || destinationPath == "/" {
		return "", nil
	}
	cleaned, err := cleanRelativePath(destinationPath)
	if err != nil {
		return "", err
	}
	return cleaned, nil
}

func PartialDestinationPath(runID string, source krmv1alpha1.BackupSource) (string, error) {
	if runID == "" {
		return "", errors.New("run ID is required")
	}
	effective, err := EffectiveDestinationPath(source)
	if err != nil {
		return "", err
	}
	return path.Join(".partial", runID, effective), nil
}

func TransferDestinationPath(machine krmv1alpha1.RsyncMachine, runID string, source krmv1alpha1.BackupSource) (string, error) {
	if machine.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror {
		effective, err := EffectiveDestinationPathForStrategy(machine, source)
		if err != nil {
			return "", err
		}
		if effective == "" {
			return ".", nil
		}
		return effective, nil
	}
	return PartialDestinationPath(runID, source)
}

func TargetReady(target krmv1alpha1.RsyncMachine) bool {
	for _, condition := range target.Status.Conditions {
		if condition.Type == "Ready" {
			return condition.Status != "False"
		}
	}
	return true
}

func ActiveRunBlocksTargetMutation(phase krmv1alpha1.RunPhase) bool {
	switch phase {
	case krmv1alpha1.RunPhasePreparing, krmv1alpha1.RunPhaseRunning, krmv1alpha1.RunPhaseFinalizing:
		return true
	default:
		return false
	}
}

func TargetGuardLeaseRef(target types.NamespacedName) types.NamespacedName {
	return types.NamespacedName{
		Namespace: target.Namespace,
		Name:      targetGuardLeaseName(target),
	}
}

func targetGuardLeaseName(target types.NamespacedName) string {
	sum := sha256.Sum256([]byte(target.Namespace + "/" + target.Name))
	suffix := hex.EncodeToString(sum[:])[:10]
	stem := dnsLabel("krm-target-guard-" + target.Name)
	maxStem := 63 - len(suffix) - 1
	if len(stem) > maxStem {
		stem = strings.TrimRight(stem[:maxStem], "-")
	}
	if stem == "" {
		stem = "krm-target-guard"
	}
	return stem + "-" + suffix
}

func cleanRelativePath(value string) (string, error) {
	if value == "" || value == "/" {
		return "", nil
	}
	if path.IsAbs(value) {
		return "", fmt.Errorf("destination path %q must be relative", value)
	}
	if strings.Contains(value, "\\") {
		return "", fmt.Errorf("destination path %q must use '/' separators", value)
	}
	for _, part := range strings.Split(value, "/") {
		if part == ".." {
			return "", fmt.Errorf("destination path %q must not contain '..'", value)
		}
	}
	cleaned := path.Clean(value)
	if cleaned == "." {
		return "", nil
	}
	return cleaned, nil
}

func DetectDestinationPathConflicts(machines []krmv1alpha1.RsyncMachine, sources map[types.NamespacedName]krmv1alpha1.BackupSource) ([]DestinationPathConflict, error) {
	type claim struct {
		machine types.NamespacedName
		path    string
		source  types.NamespacedName
	}
	claimsByKey := map[string][]claim{}
	for _, machine := range machines {
		machineRef := types.NamespacedName{Namespace: machine.Namespace, Name: machine.Name}
		for _, source := range sources {
			sourceRef := types.NamespacedName{Namespace: source.Namespace, Name: source.Name}
			resolvedMachineRef, err := ResolveObjectReference(source.Spec.MachineRef, source.Namespace)
			if err != nil {
				continue
			}
			if resolvedMachineRef != machineRef {
				continue
			}
			if machine.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror {
				continue
			}
			cleanedPath, err := cleanRelativePath(source.Spec.DestinationPath)
			if err != nil {
				return nil, fmt.Errorf("source %s destination path: %w", sourceRef.String(), err)
			}
			key := strings.Join([]string{machineRef.String(), source.Namespace, cleanedPath}, "\x00")
			claimsByKey[key] = append(claimsByKey[key], claim{
				machine: machineRef,
				path:    cleanedPath,
				source:  sourceRef,
			})
		}
	}

	var conflicts []DestinationPathConflict
	for _, claims := range claimsByKey {
		sourcesByRef := map[types.NamespacedName]struct{}{}
		machinesByRef := map[types.NamespacedName]struct{}{}
		for _, claim := range claims {
			sourcesByRef[claim.source] = struct{}{}
			machinesByRef[claim.machine] = struct{}{}
		}
		if len(sourcesByRef) <= 1 {
			continue
		}
		conflict := DestinationPathConflict{
			MachineRef:      claims[0].machine,
			SourceNamespace: claims[0].source.Namespace,
			DestinationPath: claims[0].path,
			Sources:         sortedNamespacedNames(sourcesByRef),
			Machines:        sortedNamespacedNames(machinesByRef),
		}
		conflicts = append(conflicts, conflict)
	}
	sort.Slice(conflicts, func(i, j int) bool {
		return conflictKey(conflicts[i]) < conflictKey(conflicts[j])
	})
	return conflicts, nil
}

func DetectMirrorDestinationPathOverlaps(machine krmv1alpha1.RsyncMachine, sources map[types.NamespacedName]krmv1alpha1.BackupSource) ([]DestinationPathOverlap, error) {
	if machine.Spec.Strategy.TypeOrDefault() != krmv1alpha1.BackupStrategyMirror {
		return nil, nil
	}
	machineRef := types.NamespacedName{Namespace: machine.Namespace, Name: machine.Name}
	type claim struct {
		path   string
		source types.NamespacedName
		delete bool
	}
	var claims []claim
	for _, source := range sources {
		sourceRef := types.NamespacedName{Namespace: source.Namespace, Name: source.Name}
		resolvedMachineRef, err := ResolveObjectReference(source.Spec.MachineRef, source.Namespace)
		if err != nil {
			continue
		}
		if resolvedMachineRef != machineRef {
			continue
		}
		effective, err := effectiveMirrorDestinationPath(source.Spec.DestinationPath)
		if err != nil {
			return nil, fmt.Errorf("source %s destination path: %w", sourceRef.String(), err)
		}
		claims = append(claims, claim{path: effective, source: sourceRef, delete: source.Spec.Rsync.DeleteOrDefault()})
	}
	overlapByKey := map[string]DestinationPathOverlap{}
	for i := range claims {
		for j := i + 1; j < len(claims); j++ {
			if !mirrorPathsOverlap(claims[i].path, claims[j].path) || (!claims[i].delete && !claims[j].delete) {
				continue
			}
			pathKey := mirrorOverlapPath(claims[i].path, claims[j].path)
			key := machineRef.String() + "\x00" + pathKey
			overlap := overlapByKey[key]
			overlap.MachineRef = machineRef
			overlap.Path = pathKey
			seen := map[types.NamespacedName]struct{}{}
			for _, ref := range overlap.Sources {
				seen[ref] = struct{}{}
			}
			for _, ref := range []types.NamespacedName{claims[i].source, claims[j].source} {
				if _, ok := seen[ref]; ok {
					continue
				}
				overlap.Sources = append(overlap.Sources, ref)
				seen[ref] = struct{}{}
			}
			sort.Slice(overlap.Sources, func(a, b int) bool {
				return overlap.Sources[a].String() < overlap.Sources[b].String()
			})
			overlapByKey[key] = overlap
		}
	}
	overlaps := make([]DestinationPathOverlap, 0, len(overlapByKey))
	for _, overlap := range overlapByKey {
		overlaps = append(overlaps, overlap)
	}
	sort.Slice(overlaps, func(i, j int) bool {
		if overlaps[i].Path == overlaps[j].Path {
			return overlaps[i].MachineRef.String() < overlaps[j].MachineRef.String()
		}
		return overlaps[i].Path < overlaps[j].Path
	})
	return overlaps, nil
}

func mirrorPathsOverlap(a, b string) bool {
	if a == "" || b == "" {
		return true
	}
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func mirrorOverlapPath(a, b string) string {
	if a == "" || b == "" {
		return "/"
	}
	if len(a) <= len(b) && (a == b || strings.HasPrefix(b, a+"/")) {
		return a
	}
	return b
}

func CalculateTargetRunSet(machine krmv1alpha1.RsyncMachine, sources map[types.NamespacedName]krmv1alpha1.BackupSource) (TargetRunSet, error) {
	machineRef := types.NamespacedName{Namespace: machine.Namespace, Name: machine.Name}
	switch machine.Spec.Strategy.TypeOrDefault() {
	case krmv1alpha1.BackupStrategySnapshot, krmv1alpha1.BackupStrategyMirror:
	default:
		return TargetRunSet{}, fmt.Errorf("spec.strategy.type %q is not supported", machine.Spec.Strategy.Type)
	}
	if machine.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror && !machine.Spec.Retention.Empty() {
		return TargetRunSet{}, fmt.Errorf("spec.retention must be empty when spec.strategy.type is Mirror")
	}
	conflicts, err := DetectDestinationPathConflicts([]krmv1alpha1.RsyncMachine{machine}, sources)
	if err != nil {
		return TargetRunSet{}, err
	}
	if len(conflicts) > 0 {
		return TargetRunSet{}, fmt.Errorf("target path conflict for %s/%s on %s", conflicts[0].SourceNamespace, conflicts[0].DestinationPath, conflicts[0].MachineRef.String())
	}
	overlaps, err := DetectMirrorDestinationPathOverlaps(machine, sources)
	if err != nil {
		return TargetRunSet{}, err
	}
	if len(overlaps) > 0 {
		return TargetRunSet{}, fmt.Errorf("mirror target path overlap at %q on %s with delete enabled for sources %s", overlaps[0].Path, overlaps[0].MachineRef.String(), namespacedNameList(overlaps[0].Sources))
	}

	runSet := TargetRunSet{
		MachineRef: machineRef,
		Retention:  machine.Spec.Retention,
	}
	sourcesByRef := map[types.NamespacedName]ResolvedSource{}
	runSet.Machines = append(runSet.Machines, krmv1alpha1.ObjectReference{Namespace: machine.Namespace, Name: machine.Name})
	for _, source := range sources {
		sourceRef := types.NamespacedName{Namespace: source.Namespace, Name: source.Name}
		resolvedMachineRef, err := ResolveObjectReference(source.Spec.MachineRef, source.Namespace)
		if err != nil {
			continue
		}
		if resolvedMachineRef != machineRef {
			continue
		}
		if _, ok := sourcesByRef[sourceRef]; ok {
			continue
		}
		effectivePath, err := EffectiveDestinationPathForStrategy(machine, source)
		if err != nil {
			return TargetRunSet{}, fmt.Errorf("source %s destination path: %w", sourceRef.String(), err)
		}
		sourcesByRef[sourceRef] = ResolvedSource{
			Ref:                      sourceRef,
			Source:                   source,
			EffectiveDestinationPath: effectivePath,
		}
	}
	sourceRefs := make([]types.NamespacedName, 0, len(sourcesByRef))
	for ref := range sourcesByRef {
		sourceRefs = append(sourceRefs, ref)
	}
	sort.Slice(sourceRefs, func(i, j int) bool {
		return sourceRefs[i].String() < sourceRefs[j].String()
	})
	for _, ref := range sourceRefs {
		runSet.Sources = append(runSet.Sources, sourcesByRef[ref])
	}
	if len(runSet.Sources) == 0 {
		return TargetRunSet{}, fmt.Errorf("RsyncMachine %s/%s has no referencing BackupSource objects", machine.Namespace, machine.Name)
	}
	return runSet, nil
}

func maxRetention(a, b krmv1alpha1.RetentionPolicy) krmv1alpha1.RetentionPolicy {
	return krmv1alpha1.RetentionPolicy{
		Hourly:  maxInt(a.Hourly, b.Hourly),
		Daily:   maxInt(a.Daily, b.Daily),
		Weekly:  maxInt(a.Weekly, b.Weekly),
		Monthly: maxInt(a.Monthly, b.Monthly),
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func sortedNamespacedNames(values map[types.NamespacedName]struct{}) []types.NamespacedName {
	out := make([]types.NamespacedName, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].String() < out[j].String()
	})
	return out
}

func conflictKey(conflict DestinationPathConflict) string {
	return strings.Join([]string{conflict.MachineRef.String(), conflict.SourceNamespace, conflict.DestinationPath}, "\x00")
}

func namespacedNameList(values []types.NamespacedName) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, value.String())
	}
	return strings.Join(parts, ",")
}
