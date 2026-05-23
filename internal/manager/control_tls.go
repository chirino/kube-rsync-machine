package manager

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DefaultNamespace                    = "kube-rsync-machine-operator"
	DefaultControlGRPCTLSSecretName     = "kube-rsync-machine-control-grpc-tls"
	DefaultControlGRPCTLSCertificateTTL = 365 * 24 * time.Hour
	controlTLSRunNamespace              = "operator"
	controlTLSRunName                   = "control-grpc"
	controlTLSServiceName               = "kube-rsync-machine-manager"
)

func DefaultManagerNamespace() string {
	if namespace := os.Getenv("POD_NAMESPACE"); namespace != "" {
		return namespace
	}
	return DefaultNamespace
}

func ControlGRPCServerIdentity(namespace string) tlsutil.Identity {
	return tlsutil.TargetIdentity(controlTLSRunNamespace, controlTLSRunName, namespace, controlTLSServiceName)
}

func EnsureControlGRPCTLSSecret(ctx context.Context, c client.Client, namespace, name string, ttl time.Duration) (tlsutil.Bundle, error) {
	if namespace == "" {
		return tlsutil.Bundle{}, fmt.Errorf("control grpc tls namespace is required")
	}
	if name == "" {
		return tlsutil.Bundle{}, fmt.Errorf("control grpc tls secret name is required")
	}
	if ttl <= 0 {
		return tlsutil.Bundle{}, fmt.Errorf("control grpc tls certificate ttl must be positive")
	}

	var secret corev1.Secret
	key := types.NamespacedName{Namespace: namespace, Name: name}
	if err := c.Get(ctx, key, &secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return tlsutil.Bundle{}, fmt.Errorf("get control grpc tls secret: %w", err)
		}
		bundle, err := mintControlGRPCServerBundle(namespace, ttl)
		if err != nil {
			return tlsutil.Bundle{}, err
		}
		secret = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      name,
				Labels:    controlTLSSecretLabels(),
			},
			Type: corev1.SecretTypeTLS,
			Data: bundle.SecretData(),
		}
		if err := c.Create(ctx, &secret); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return EnsureControlGRPCTLSSecret(ctx, c, namespace, name, ttl)
			}
			return tlsutil.Bundle{}, fmt.Errorf("create control grpc tls secret: %w", err)
		}
		return bundle, nil
	}

	bundle := tlsutil.Bundle{
		CACertPEM: secret.Data[tlsutil.SecretCAFile],
		CertPEM:   secret.Data[tlsutil.SecretCertFile],
		KeyPEM:    secret.Data[tlsutil.SecretKeyFile],
	}
	if err := validControlGRPCBundle(bundle, namespace); err == nil {
		return bundle, nil
	}
	bundle, err := mintControlGRPCServerBundle(namespace, ttl)
	if err != nil {
		return tlsutil.Bundle{}, err
	}
	if secret.Labels == nil {
		secret.Labels = map[string]string{}
	}
	for k, v := range controlTLSSecretLabels() {
		secret.Labels[k] = v
	}
	secret.Type = corev1.SecretTypeTLS
	secret.Data = bundle.SecretData()
	if err := c.Update(ctx, &secret); err != nil {
		return tlsutil.Bundle{}, fmt.Errorf("repair control grpc tls secret: %w", err)
	}
	return bundle, nil
}

func validControlGRPCBundle(bundle tlsutil.Bundle, namespace string) error {
	if _, err := tls.X509KeyPair(bundle.CertPEM, bundle.KeyPEM); err != nil {
		return err
	}
	return tlsutil.VerifyIdentity(bundle, ControlGRPCServerIdentity(namespace), time.Now())
}

func mintControlGRPCServerBundle(namespace string, ttl time.Duration) (tlsutil.Bundle, error) {
	ca, err := tlsutil.NewRunCA(controlTLSRunNamespace, controlTLSRunName, ttl)
	if err != nil {
		return tlsutil.Bundle{}, fmt.Errorf("mint control grpc ca: %w", err)
	}
	bundle, err := ca.Mint(ControlGRPCServerIdentity(namespace), ttl)
	if err != nil {
		return tlsutil.Bundle{}, fmt.Errorf("mint control grpc server certificate: %w", err)
	}
	return bundle, nil
}

func controlTLSSecretLabels() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "kube-rsync-machine",
		"app.kubernetes.io/component": "control-grpc",
	}
}
