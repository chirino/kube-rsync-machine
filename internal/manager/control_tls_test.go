package manager

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEnsureControlGRPCTLSSecretCreatesReusableBundle(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()

	bundle, err := EnsureControlGRPCTLSSecret(ctx, client, "krm-system", "control-tls", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := tlsutil.VerifyIdentity(bundle, ControlGRPCServerIdentity("krm-system"), time.Now()); err != nil {
		t.Fatalf("created bundle does not verify: %v", err)
	}

	var secret corev1.Secret
	if err := client.Get(ctx, types.NamespacedName{Namespace: "krm-system", Name: "control-tls"}, &secret); err != nil {
		t.Fatal(err)
	}
	if secret.Type != corev1.SecretTypeTLS || len(secret.Data[tlsutil.SecretCAFile]) == 0 {
		t.Fatalf("unexpected secret: %#v", secret)
	}

	reused, err := EnsureControlGRPCTLSSecret(ctx, client, "krm-system", "control-tls", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reused.CertPEM, bundle.CertPEM) || !bytes.Equal(reused.KeyPEM, bundle.KeyPEM) {
		t.Fatal("expected valid existing control grpc tls secret to be reused")
	}
}

func TestEnsureControlGRPCTLSSecretRepairsInvalidBundle(t *testing.T) {
	ctx := context.Background()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "krm-system", Name: "control-tls"},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			tlsutil.SecretCAFile:   []byte("not a certificate"),
			tlsutil.SecretCertFile: []byte("not a certificate"),
			tlsutil.SecretKeyFile:  []byte("not a key"),
		},
	}
	client := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(secret).Build()

	bundle, err := EnsureControlGRPCTLSSecret(ctx, client, "krm-system", "control-tls", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := tlsutil.VerifyIdentity(bundle, ControlGRPCServerIdentity("krm-system"), time.Now()); err != nil {
		t.Fatalf("repaired bundle does not verify: %v", err)
	}

	var updated corev1.Secret
	if err := client.Get(ctx, types.NamespacedName{Namespace: "krm-system", Name: "control-tls"}, &updated); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(updated.Data[tlsutil.SecretCertFile], []byte("not a certificate")) {
		t.Fatal("expected invalid serving certificate to be replaced")
	}
}

func coreScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}
