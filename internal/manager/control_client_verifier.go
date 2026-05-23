package manager

import (
	"context"
	"crypto/x509"
	"fmt"

	"github.com/chirino/kube-rsync-machine/internal/controlgrpc"
	"github.com/chirino/kube-rsync-machine/internal/controller"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewRunClientCertificateVerifier(c client.Client) controlgrpc.ClientCertificateVerifier {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		identity, err := identityFromRawCertificate(rawCerts)
		if err != nil {
			return err
		}
		secrets, err := candidateRunCredentialSecrets(context.Background(), c, identity)
		if err != nil {
			return err
		}
		var lastErr error
		for _, secret := range secrets {
			caPEM := secret.Data[tlsutil.SecretCAFile]
			if len(caPEM) == 0 {
				continue
			}
			if err := controlgrpc.VerifyClientCertificateWithCA(rawCerts, caPEM, identity); err == nil {
				return nil
			} else {
				lastErr = err
			}
		}
		if lastErr != nil {
			return fmt.Errorf("verify client certificate against run CA: %w", lastErr)
		}
		return fmt.Errorf("no generated run credential secret found for client identity %s", identity.URI())
	}
}

func identityFromRawCertificate(rawCerts [][]byte) (tlsutil.Identity, error) {
	if len(rawCerts) == 0 {
		return tlsutil.Identity{}, fmt.Errorf("client certificate is required")
	}
	cert, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return tlsutil.Identity{}, err
	}
	for _, uri := range cert.URIs {
		identity, err := tlsutil.ParseIdentity(uri.String())
		if err == nil {
			return identity, nil
		}
	}
	return tlsutil.Identity{}, fmt.Errorf("client certificate does not contain a kube-rsync-machine identity")
}

func candidateRunCredentialSecrets(ctx context.Context, c client.Client, identity tlsutil.Identity) ([]corev1.Secret, error) {
	runID := controller.RunIDForRef(client.ObjectKey{Namespace: identity.RunNamespace, Name: identity.RunName})
	names := []string{
		controller.GeneratedTLSSecretName(runID, controller.RoleTargetServer, identity.Namespace, identity.Name),
		controller.GeneratedTLSSecretName(runID, controller.RoleSourceSender, identity.Namespace, identity.Name),
		controller.GeneratedTLSSecretName(runID, controller.RoleRestoreWriter, identity.Namespace, identity.Name),
	}
	var candidates []corev1.Secret
	for _, name := range names {
		var secret corev1.Secret
		if err := c.Get(ctx, client.ObjectKey{Namespace: identity.Namespace, Name: name}, &secret); err == nil {
			candidates = append(candidates, secret)
		}
	}

	var listed corev1.SecretList
	if err := c.List(ctx, &listed, client.MatchingLabels{
		controller.LabelName:         controller.AppName,
		controller.LabelRunNamespace: identity.RunNamespace,
		controller.LabelRun:          identity.RunName,
	}); err != nil {
		return nil, fmt.Errorf("list generated run credential secrets: %w", err)
	}
	seen := map[string]struct{}{}
	for _, secret := range candidates {
		seen[secret.Namespace+"/"+secret.Name] = struct{}{}
	}
	for _, secret := range listed.Items {
		for _, name := range names {
			if secret.Name != name {
				continue
			}
			key := secret.Namespace + "/" + secret.Name
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			candidates = append(candidates, secret)
		}
	}
	return candidates, nil
}
