package manager

import (
	"context"
	"fmt"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const defaultManagerContainerName = "manager"

func OwnPodName() string {
	if podName := strings.TrimSpace(os.Getenv("POD_NAME")); podName != "" {
		return podName
	}
	if hostname := strings.TrimSpace(os.Getenv("HOSTNAME")); hostname != "" {
		return hostname
	}
	hostname, err := os.Hostname()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(hostname)
}

func ResolveOwnPodImage(ctx context.Context, c client.Client, namespace, podName, containerName, fallback string) (string, error) {
	if namespace == "" || podName == "" {
		return fallback, nil
	}
	var pod corev1.Pod
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return fallback, nil
		}
		return fallback, fmt.Errorf("get own pod %s/%s: %w", namespace, podName, err)
	}
	if image := pullableDigestImage(pod.Status.ContainerStatuses, containerName); image != "" {
		return image, nil
	}
	if image := podSpecContainerImage(pod.Spec.Containers, containerName); image != "" {
		return image, nil
	}
	return fallback, nil
}

func pullableDigestImage(statuses []corev1.ContainerStatus, containerName string) string {
	for _, status := range statuses {
		if containerName != "" && status.Name != containerName {
			continue
		}
		imageID := strings.TrimSpace(status.ImageID)
		if imageID == "" || !strings.Contains(imageID, "@sha256:") {
			continue
		}
		if _, value, ok := strings.Cut(imageID, "://"); ok {
			imageID = value
		}
		return imageID
	}
	return ""
}

func podSpecContainerImage(containers []corev1.Container, containerName string) string {
	for _, container := range containers {
		if containerName == "" || container.Name == containerName {
			return container.Image
		}
	}
	return ""
}
