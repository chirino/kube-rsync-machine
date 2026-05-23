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

func backupTarget(namespace, name, pvc string, retention krmv1alpha1.RetentionPolicy) krmv1alpha1.RsyncMachine {
	return krmv1alpha1.RsyncMachine{
		ObjectMeta: objectMeta(namespace, name),
		Spec: krmv1alpha1.RsyncMachineSpec{
			PVCName:   pvc,
			Retention: retention,
		},
	}
}
