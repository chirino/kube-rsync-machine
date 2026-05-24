package manager

import (
	"encoding/pem"
	"testing"
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/controller"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRunClientCertificateVerifierTrustsControlPlaneCA(t *testing.T) {
	run := krmv1alpha1.BackupJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "demo-20260520"},
	}
	target := krmv1alpha1.RsyncMachine{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "archive"},
	}
	source := krmv1alpha1.BackupSource{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app-prod", Name: "files"},
	}
	controlCA, err := tlsutil.NewCA("test-control-ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := controller.BuildBackupJobCredentialSecretsWithSigner(run, target, []krmv1alpha1.BackupSource{source}, time.Hour, controlCA)
	if err != nil {
		t.Fatal(err)
	}
	sourceCredential := credentials[1]

	block, _ := pem.Decode(sourceCredential.Bundle.CertPEM)
	if block == nil {
		t.Fatal("expected client certificate PEM")
	}
	if err := NewRunClientCertificateVerifier(controlCA.Bundle().CACertPEM)([][]byte{block.Bytes}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestRunClientCertificateVerifierRejectsNonControlPlaneCA(t *testing.T) {
	run := krmv1alpha1.BackupJob{ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "demo-20260520"}}
	target := krmv1alpha1.RsyncMachine{ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "archive"}}
	source := krmv1alpha1.BackupSource{ObjectMeta: metav1.ObjectMeta{Namespace: "app-prod", Name: "files"}}
	credentials, err := controller.BuildBackupJobCredentialSecrets(run, target, []krmv1alpha1.BackupSource{source}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	sourceCredential := credentials[1]
	controlCA, err := tlsutil.NewCA("test-control-ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(sourceCredential.Bundle.CertPEM)
	if block == nil {
		t.Fatal("expected client certificate PEM")
	}
	if err := NewRunClientCertificateVerifier(controlCA.Bundle().CACertPEM)([][]byte{block.Bytes}, nil); err == nil {
		t.Fatal("expected non-control-plane certificate to be rejected")
	}
}
