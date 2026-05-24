package controller

import (
	"reflect"
	"testing"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

func TestResolveObjectReferenceDefaultsNamespace(t *testing.T) {
	ref, err := ResolveObjectReference(krmv1alpha1.ObjectReference{Name: "archive"}, "backup")
	if err != nil {
		t.Fatalf("ResolveObjectReference returned error: %v", err)
	}
	if ref != (types.NamespacedName{Namespace: "backup", Name: "archive"}) {
		t.Fatalf("unexpected ref: %s", ref.String())
	}
}

func TestNamespaceAllowedDefaultsToMachineNamespace(t *testing.T) {
	machine := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	machine.Spec.AllowedSourceNamespaces = nil
	if !SourceNamespaceAllowed(machine, "backup") {
		t.Fatal("expected default source namespace allowance to include machine namespace")
	}
	if SourceNamespaceAllowed(machine, "app") {
		t.Fatal("expected default source namespace allowance to reject other namespaces")
	}
}

func TestNamespaceAllowedTokens(t *testing.T) {
	tests := []struct {
		name      string
		allowed   []string
		namespace string
		want      bool
	}{
		{name: "empty list", allowed: []string{}, namespace: "backup", want: true},
		{name: "dot", allowed: []string{"."}, namespace: "backup", want: true},
		{name: "star", allowed: []string{"*"}, namespace: "app", want: true},
		{name: "literal", allowed: []string{"app"}, namespace: "app", want: true},
		{name: "non match", allowed: []string{"app"}, namespace: "backup", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			machine := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
			machine.Spec.AllowedSourceNamespaces = test.allowed
			if got := SourceNamespaceAllowed(machine, test.namespace); got != test.want {
				t.Fatalf("SourceNamespaceAllowed() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestEffectiveDestinationPath(t *testing.T) {
	tests := []struct {
		name            string
		destinationPath string
		want            string
	}{
		{name: "nested path", destinationPath: "sites/demo-app/files", want: "app-prod/sites/demo-app/files"},
		{name: "empty path", destinationPath: "", want: "app-prod"},
		{name: "root path", destinationPath: "/", want: "app-prod"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := backupSource("app-prod", "files", "data", test.destinationPath)
			got, err := EffectiveDestinationPath(source)
			if err != nil {
				t.Fatalf("EffectiveDestinationPath returned error: %v", err)
			}
			if got != test.want {
				t.Fatalf("unexpected path %q", got)
			}
		})
	}
}

func TestEffectiveMirrorDestinationPath(t *testing.T) {
	machine := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	machine.Spec.Strategy.Type = krmv1alpha1.BackupStrategyMirror
	tests := []struct {
		name            string
		destinationPath string
		want            string
	}{
		{name: "nested path", destinationPath: "sites/demo-app/files", want: "sites/demo-app/files"},
		{name: "empty path", destinationPath: "", want: ""},
		{name: "root path", destinationPath: "/", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := backupSource("app-prod", "files", "data", test.destinationPath)
			got, err := EffectiveDestinationPathForStrategy(machine, source)
			if err != nil {
				t.Fatalf("EffectiveDestinationPathForStrategy returned error: %v", err)
			}
			if got != test.want {
				t.Fatalf("unexpected path %q", got)
			}
		})
	}
}

func TestEffectiveDestinationPathRejectsUnsafePath(t *testing.T) {
	tests := []string{"/absolute", "../escape", "nested/../../escape"}
	for _, test := range tests {
		t.Run(test, func(t *testing.T) {
			source := backupSource("app-prod", "files", "data", test)
			if _, err := EffectiveDestinationPath(source); err == nil {
				t.Fatalf("expected error for destination path %q", test)
			}
		})
	}
}

func TestDetectDestinationPathConflicts(t *testing.T) {
	machines := []krmv1alpha1.RsyncMachine{
		rsyncMachine("backup", "archive", ref("backup", "archive"), nil, krmv1alpha1.RetentionPolicy{}),
	}
	sources := map[types.NamespacedName]krmv1alpha1.BackupSource{
		{Namespace: "app-prod", Name: "files"}: backupSourceForMachine("app-prod", "files", "data", "sites/demo-app/files", ref("backup", "archive")),
		{Namespace: "app-prod", Name: "media"}: backupSourceForMachine("app-prod", "media", "media", "sites/demo-app/files", ref("backup", "archive")),
		{Namespace: "app-dev", Name: "files"}:  backupSourceForMachine("app-dev", "files", "data", "sites/demo-app/files", ref("backup", "archive")),
	}

	conflicts, err := DetectDestinationPathConflicts(machines, sources)
	if err != nil {
		t.Fatalf("DetectDestinationPathConflicts returned error: %v", err)
	}
	if len(conflicts) != 1 {
		t.Fatalf("expected 1 conflict, got %d: %#v", len(conflicts), conflicts)
	}
	conflict := conflicts[0]
	if conflict.MachineRef != (types.NamespacedName{Namespace: "backup", Name: "archive"}) {
		t.Fatalf("unexpected target: %s", conflict.MachineRef.String())
	}
	if conflict.SourceNamespace != "app-prod" || conflict.DestinationPath != "sites/demo-app/files" {
		t.Fatalf("unexpected conflict path: %#v", conflict)
	}
	wantSources := []types.NamespacedName{{Namespace: "app-prod", Name: "files"}, {Namespace: "app-prod", Name: "media"}}
	if !reflect.DeepEqual(conflict.Sources, wantSources) {
		t.Fatalf("unexpected conflict sources: %#v", conflict.Sources)
	}
}

func TestDetectDestinationPathConflictsAllowsSameSourceInMultiplePlans(t *testing.T) {
	machines := []krmv1alpha1.RsyncMachine{
		rsyncMachine("backup", "archive", ref("backup", "archive"), nil, krmv1alpha1.RetentionPolicy{}),
	}
	sources := map[types.NamespacedName]krmv1alpha1.BackupSource{
		{Namespace: "app-prod", Name: "files"}: backupSourceForMachine("app-prod", "files", "data", "sites/demo-app/files", ref("backup", "archive")),
	}

	conflicts, err := DetectDestinationPathConflicts(machines, sources)
	if err != nil {
		t.Fatalf("DetectDestinationPathConflicts returned error: %v", err)
	}
	if len(conflicts) != 0 {
		t.Fatalf("expected no conflicts, got %#v", conflicts)
	}
}

func TestDetectMirrorDestinationPathOverlaps(t *testing.T) {
	disabled := false
	tests := []struct {
		name    string
		paths   []string
		delete  []bool
		wantHit bool
	}{
		{name: "root overlaps nested", paths: []string{"/", "app"}, wantHit: true},
		{name: "parent overlaps child", paths: []string{"app", "app/data"}, wantHit: true},
		{name: "prefix does not overlap", paths: []string{"app-a", "app"}, wantHit: false},
		{name: "overlap allowed without delete", paths: []string{"app", "app/data"}, delete: []bool{false, false}, wantHit: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			machine := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
			machine.Spec.Strategy.Type = krmv1alpha1.BackupStrategyMirror
			sources := map[types.NamespacedName]krmv1alpha1.BackupSource{}
			for i, path := range test.paths {
				source := backupSourceForMachine("app-prod", string(rune('a'+i)), "data", path, ref("backup", "archive"))
				if i < len(test.delete) && !test.delete[i] {
					source.Spec.Rsync.Delete = &disabled
				}
				sources[types.NamespacedName{Namespace: source.Namespace, Name: source.Name}] = source
			}
			overlaps, err := DetectMirrorDestinationPathOverlaps(machine, sources)
			if err != nil {
				t.Fatalf("DetectMirrorDestinationPathOverlaps returned error: %v", err)
			}
			if (len(overlaps) > 0) != test.wantHit {
				t.Fatalf("unexpected overlaps: %#v", overlaps)
			}
		})
	}
}

func TestCalculateTargetRunSet(t *testing.T) {
	machine := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24, Daily: 7, Weekly: 8, Monthly: 12})
	sources := map[types.NamespacedName]krmv1alpha1.BackupSource{
		{Namespace: "app-prod", Name: "files"}: backupSourceForMachine("app-prod", "files", "data", "sites/demo-app/files", ref("backup", "archive")),
		{Namespace: "app-prod", Name: "media"}: backupSourceForMachine("app-prod", "media", "media", "sites/demo-app/media", ref("backup", "archive")),
	}

	runSet, err := CalculateTargetRunSet(machine, sources)
	if err != nil {
		t.Fatalf("CalculateTargetRunSet returned error: %v", err)
	}
	if len(runSet.Machines) != 1 {
		t.Fatalf("expected 1 machine, got %#v", runSet.Machines)
	}
	if len(runSet.Sources) != 2 {
		t.Fatalf("expected 2 sources, got %#v", runSet.Sources)
	}
	if runSet.Sources[0].Ref.String() != "app-prod/files" || runSet.Sources[1].Ref.String() != "app-prod/media" {
		t.Fatalf("sources not sorted/deduped: %#v", runSet.Sources)
	}
	wantRetention := krmv1alpha1.RetentionPolicy{Hourly: 24, Daily: 7, Weekly: 8, Monthly: 12}
	if runSet.Retention != wantRetention {
		t.Fatalf("unexpected retention: %#v", runSet.Retention)
	}
}

func TestCalculateTargetRunSetUsesMirrorPaths(t *testing.T) {
	machine := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	machine.Spec.Strategy.Type = krmv1alpha1.BackupStrategyMirror
	deleteDisabled := false
	root := backupSourceForMachine("app-prod", "root", "data", "/", ref("backup", "archive"))
	root.Spec.Rsync.Delete = &deleteDisabled
	app := backupSourceForMachine("app-prod", "app", "data", "app", ref("backup", "archive"))
	app.Spec.Rsync.Delete = &deleteDisabled
	sources := map[types.NamespacedName]krmv1alpha1.BackupSource{
		{Namespace: "app-prod", Name: "root"}: root,
		{Namespace: "app-prod", Name: "app"}:  app,
	}

	runSet, err := CalculateTargetRunSet(machine, sources)
	if err != nil {
		t.Fatalf("CalculateTargetRunSet returned error: %v", err)
	}
	if got := runSet.Sources[0].EffectiveDestinationPath; got != "app" {
		t.Fatalf("unexpected first mirror path %q", got)
	}
	if got := runSet.Sources[1].EffectiveDestinationPath; got != "" {
		t.Fatalf("unexpected root mirror path %q", got)
	}
}

func backupTarget(namespace, name, pvc string, retention krmv1alpha1.RetentionPolicy) krmv1alpha1.RsyncMachine {
	return krmv1alpha1.RsyncMachine{
		ObjectMeta: objectMeta(namespace, name),
		Spec: krmv1alpha1.RsyncMachineSpec{
			PVCName:                  pvc,
			AllowedSourceNamespaces:  []string{"*"},
			AllowedRestoreNamespaces: []string{"*"},
			Retention:                retention,
		},
	}
}
