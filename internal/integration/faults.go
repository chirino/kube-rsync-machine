package integration

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Faults struct {
	TargetOutOfSpace     bool
	SourceRsyncExitCode  int32
	DropTargetReadyEvent bool
}

func CompletedJob(namespace, name string, labels map[string]string) *batchv1.Job {
	return jobWithCondition(namespace, name, labels, batchv1.JobComplete)
}

func FailedJob(namespace, name string, labels map[string]string) *batchv1.Job {
	return jobWithCondition(namespace, name, labels, batchv1.JobFailed)
}

func jobWithCondition(namespace, name string, labels map[string]string, conditionType batchv1.JobConditionType) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    labels,
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{
				Type:   conditionType,
				Status: corev1.ConditionTrue,
			}},
		},
	}
}
