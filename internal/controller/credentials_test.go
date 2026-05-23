package controller

import (
	"testing"
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
)

func TestBuildBackupJobCredentialSecrets(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")

	credentials, err := BuildBackupJobCredentialSecrets(run, target, []krmv1alpha1.BackupSource{source}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(credentials) != 2 {
		t.Fatalf("expected 2 credentials, got %d", len(credentials))
	}
	targetCredential := credentials[0]
	if targetCredential.Namespace != "backup" || targetCredential.Name != "krm-tls-backup-demo-20260520-target-server-backup-archive" {
		t.Fatalf("unexpected target credential: %#v", targetCredential)
	}
	if err := tlsutil.VerifyIdentity(targetCredential.Bundle, tlsutil.TargetIdentity("backup", "demo-20260520", "backup", "archive"), time.Now()); err != nil {
		t.Fatal(err)
	}
	sourceCredential := credentials[1]
	if sourceCredential.Namespace != "app-prod" || sourceCredential.Name != "krm-tls-backup-demo-20260520-source-sender-app-prod-files" {
		t.Fatalf("unexpected source credential: %#v", sourceCredential)
	}
	if err := tlsutil.VerifyIdentity(sourceCredential.Bundle, tlsutil.SourceIdentity("backup", "demo-20260520", "app-prod", "files"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if sourceCredential.Secret().Labels[LabelSource] != "app-prod-files" {
		t.Fatalf("expected source label on secret, got %#v", sourceCredential.Secret().Labels)
	}
}

func TestBuildBackupJobCredentialSecretsPublishesControlCA(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	controlCA := []byte("control-ca")

	credentials, err := BuildBackupJobCredentialSecrets(run, target, []krmv1alpha1.BackupSource{source}, time.Hour, controlCA)
	if err != nil {
		t.Fatal(err)
	}
	for _, credential := range credentials {
		secret := credential.Secret()
		if string(secret.Data[ControlCAFile]) != "control-ca" {
			t.Fatalf("expected control CA in %s/%s secret data, got %#v", secret.Namespace, secret.Name, secret.Data)
		}
	}
}

func TestBuildRestoreJobCredentialSecrets(t *testing.T) {
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("backup", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("app-prod", "files"),
		},
	}
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")

	credentials, err := BuildRestoreJobCredentialSecrets(restore, target, source, "backup", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(credentials) != 2 {
		t.Fatalf("expected 2 credentials, got %d", len(credentials))
	}
	if credentials[1].Name != "krm-tls-backup-restore-files-restore-writer-app-prod-files" {
		t.Fatalf("unexpected writer credential name: %q", credentials[1].Name)
	}
	if credentials[1].Namespace != "backup" {
		t.Fatalf("unexpected writer credential namespace: %q", credentials[1].Namespace)
	}
	if err := tlsutil.VerifyIdentity(credentials[1].Bundle, tlsutil.SourceIdentity("backup", "restore-files", "app-prod", "files"), time.Now()); err != nil {
		t.Fatal(err)
	}
}
