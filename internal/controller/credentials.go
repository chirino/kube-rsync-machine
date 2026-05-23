package controller

import (
	"fmt"
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	DefaultRunCertificateTTL = 25 * time.Hour
	ControlCAFile            = tlsutil.SecretControlCAFile
)

type CredentialSecret struct {
	Namespace string
	Name      string
	Identity  tlsutil.Identity
	Labels    map[string]string
	Bundle    tlsutil.Bundle
	ExtraData map[string][]byte
}

func BuildBackupJobCredentialSecrets(run krmv1alpha1.BackupJob, target krmv1alpha1.RsyncMachine, sources []krmv1alpha1.BackupSource, ttl time.Duration, controlCAPEM ...[]byte) ([]CredentialSecret, error) {
	if ttl == 0 {
		ttl = DefaultRunCertificateTTL
	}
	ca, err := tlsutil.NewRunCA(run.Namespace, run.Name, ttl)
	if err != nil {
		return nil, err
	}
	runID := RunID(run)
	credentials := make([]CredentialSecret, 0, len(sources)+1)
	targetIdentity := tlsutil.TargetIdentity(run.Namespace, run.Name, target.Namespace, target.Name)
	targetBundle, err := ca.Mint(targetIdentity, ttl)
	if err != nil {
		return nil, fmt.Errorf("mint target certificate: %w", err)
	}
	credentials = append(credentials, CredentialSecret{
		Namespace: target.Namespace,
		Name:      GeneratedTLSSecretName(runID, RoleTargetServer, target.Namespace, target.Name),
		Identity:  targetIdentity,
		Labels:    runLabels(run, runKindBackup, RoleTargetServer),
		Bundle:    targetBundle,
		ExtraData: credentialExtraData(controlCAPEM...),
	})
	for _, source := range sources {
		identity := tlsutil.SourceIdentity(run.Namespace, run.Name, source.Namespace, source.Name)
		bundle, err := ca.Mint(identity, ttl)
		if err != nil {
			return nil, fmt.Errorf("mint source certificate %s/%s: %w", source.Namespace, source.Name, err)
		}
		labels := runLabels(run, runKindBackup, RoleSourceSender)
		labels[LabelSource] = labelValue(source.Namespace, source.Name)
		credentials = append(credentials, CredentialSecret{
			Namespace: source.Namespace,
			Name:      GeneratedTLSSecretName(runID, RoleSourceSender, source.Namespace, source.Name),
			Identity:  identity,
			Labels:    labels,
			Bundle:    bundle,
			ExtraData: credentialExtraData(controlCAPEM...),
		})
	}
	return credentials, nil
}

func BuildRestoreJobCredentialSecrets(restore krmv1alpha1.RestoreJob, target krmv1alpha1.RsyncMachine, source krmv1alpha1.BackupSource, destinationNamespace string, ttl time.Duration, controlCAPEM ...[]byte) ([]CredentialSecret, error) {
	if ttl == 0 {
		ttl = DefaultRunCertificateTTL
	}
	if destinationNamespace == "" {
		destinationNamespace = source.Namespace
	}
	ca, err := tlsutil.NewRunCA(restore.Namespace, restore.Name, ttl)
	if err != nil {
		return nil, err
	}
	runRef := types.NamespacedName{Namespace: restore.Namespace, Name: restore.Name}
	runID := RunIDForRef(runRef)
	targetIdentity := tlsutil.TargetIdentity(restore.Namespace, restore.Name, target.Namespace, target.Name)
	targetBundle, err := ca.Mint(targetIdentity, ttl)
	if err != nil {
		return nil, fmt.Errorf("mint restore target certificate: %w", err)
	}
	writerIdentity := tlsutil.SourceIdentity(restore.Namespace, restore.Name, source.Namespace, source.Name)
	writerBundle, err := ca.Mint(writerIdentity, ttl)
	if err != nil {
		return nil, fmt.Errorf("mint restore writer certificate: %w", err)
	}
	targetLabels := runLabelsForRef(runRef, runKindRestore, RoleTargetServer)
	writerLabels := runLabelsForRef(runRef, runKindRestore, RoleRestoreWriter)
	writerLabels[LabelSource] = labelValue(source.Namespace, source.Name)
	return []CredentialSecret{
		{
			Namespace: target.Namespace,
			Name:      GeneratedTLSSecretName(runID, RoleTargetServer, target.Namespace, target.Name),
			Identity:  targetIdentity,
			Labels:    targetLabels,
			Bundle:    targetBundle,
			ExtraData: credentialExtraData(controlCAPEM...),
		},
		{
			Namespace: destinationNamespace,
			Name:      GeneratedTLSSecretName(runID, RoleRestoreWriter, source.Namespace, source.Name),
			Identity:  writerIdentity,
			Labels:    writerLabels,
			Bundle:    writerBundle,
			ExtraData: credentialExtraData(controlCAPEM...),
		},
	}, nil
}

func (c CredentialSecret) Secret() *corev1.Secret {
	secret := BuildTLSSecret(c.Namespace, c.Name, c.Labels, c.Bundle)
	for key, value := range c.ExtraData {
		secret.Data[key] = append([]byte(nil), value...)
	}
	return secret
}

func credentialExtraData(controlCAPEM ...[]byte) map[string][]byte {
	if len(controlCAPEM) == 0 || len(controlCAPEM[0]) == 0 {
		return nil
	}
	return map[string][]byte{
		ControlCAFile: append([]byte(nil), controlCAPEM[0]...),
	}
}
