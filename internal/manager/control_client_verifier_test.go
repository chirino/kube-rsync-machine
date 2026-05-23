package manager

import (
	"encoding/pem"
	"testing"
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/controller"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRunClientCertificateVerifierTrustsGeneratedRunSecretCA(t *testing.T) {
	run := krmv1alpha1.BackupJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "demo-20260520"},
	}
	target := krmv1alpha1.RsyncMachine{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "archive"},
	}
	source := krmv1alpha1.BackupSource{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app-prod", Name: "files"},
	}
	credentials, err := controller.BuildBackupJobCredentialSecrets(run, target, []krmv1alpha1.BackupSource{source}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	sourceCredential := credentials[1]
	client := fake.NewClientBuilder().
		WithScheme(coreOnlyScheme(t)).
		WithObjects(sourceCredential.Secret()).
		Build()

	block, _ := pem.Decode(sourceCredential.Bundle.CertPEM)
	if block == nil {
		t.Fatal("expected client certificate PEM")
	}
	if err := NewRunClientCertificateVerifier(client)([][]byte{block.Bytes}, nil); err != nil {
		t.Fatal(err)
	}
}

func coreOnlyScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}
