package snapshot

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/discovery"
)

type Capabilities interface {
	VolumeSnapshotAvailable(ctx context.Context) bool
}

type StaticCapabilities struct {
	Available bool
}

func (c StaticCapabilities) VolumeSnapshotAvailable(context.Context) bool {
	return c.Available
}

type DiscoveryCapabilities struct {
	Client discovery.DiscoveryInterface
}

func (c DiscoveryCapabilities) VolumeSnapshotAvailable(ctx context.Context) bool {
	if c.Client == nil {
		return false
	}
	_, err := c.Client.ServerResourcesForGroupVersion(SnapshotAPIVersion)
	if err == nil {
		return true
	}
	if apierrors.IsNotFound(err) {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	default:
		return false
	}
}
