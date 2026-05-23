package manager

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestResolveOwnPodImagePrefersRunningDigest(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "krm-system", Name: "operator-abc"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:  "manager",
			Image: "registry.example.com/krm:latest",
		}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name:    "manager",
			ImageID: "docker-pullable://registry.example.com/krm@sha256:1234",
		}}},
	}
	client := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(pod).Build()

	image, err := ResolveOwnPodImage(ctx, client, "krm-system", "operator-abc", "manager", "fallback:image")
	if err != nil {
		t.Fatal(err)
	}
	if image != "registry.example.com/krm@sha256:1234" {
		t.Fatalf("unexpected image: %q", image)
	}
}

func TestOwnPodNamePrefersExplicitPodName(t *testing.T) {
	t.Setenv("POD_NAME", "operator-explicit")
	t.Setenv("HOSTNAME", "operator-hostname")

	if got := OwnPodName(); got != "operator-explicit" {
		t.Fatalf("unexpected pod name: %q", got)
	}
}

func TestOwnPodNameFallsBackToHostnameEnv(t *testing.T) {
	t.Setenv("POD_NAME", "")
	t.Setenv("HOSTNAME", "operator-hostname")

	if got := OwnPodName(); got != "operator-hostname" {
		t.Fatalf("unexpected pod name: %q", got)
	}
}

func TestResolveOwnPodImageFallsBackToSpecImageWithoutDigest(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "krm-system", Name: "operator-abc"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:  "manager",
			Image: "registry.example.com/krm:latest",
		}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name:    "manager",
			ImageID: "containerd://sha256:1234",
		}}},
	}
	client := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(pod).Build()

	image, err := ResolveOwnPodImage(ctx, client, "krm-system", "operator-abc", "manager", "fallback:image")
	if err != nil {
		t.Fatal(err)
	}
	if image != "registry.example.com/krm:latest" {
		t.Fatalf("unexpected image: %q", image)
	}
}

func TestResolveOwnPodImageFallsBackWithoutPodCoordinates(t *testing.T) {
	client := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()

	image, err := ResolveOwnPodImage(context.Background(), client, "", "", "manager", "fallback:image")
	if err != nil {
		t.Fatal(err)
	}
	if image != "fallback:image" {
		t.Fatalf("unexpected image: %q", image)
	}
}
