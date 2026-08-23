/*
Copyright 2024 The Kubeflow authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workload

import (
	"context"
	"errors"
	"strings"
	"testing"

	schedulingv1alpha2 "k8s.io/api/scheduling/v1alpha2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func newFakeClient() client.Client {
	scheme := runtime.NewScheme()
	_ = v1beta2.AddToScheme(scheme)
	_ = schedulingv1alpha2.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

func newTestApp(name, namespace string, mode v1beta2.DeployMode) *v1beta2.SparkApplication {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID("test-uid"),
		},
		Spec: v1beta2.SparkApplicationSpec{
			Mode:  mode,
			Image: ptr.To("spark:3.5.1"),
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:  ptr.To(int32(1)),
					Memory: ptr.To("512m"),
				},
			},
			Executor: v1beta2.ExecutorSpec{
				Instances: ptr.To(int32(3)),
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:  ptr.To(int32(1)),
					Memory: ptr.To("512m"),
				},
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SubmissionID: "test-submission-1",
		},
	}
	return app
}

func TestScheduler_Name(t *testing.T) {
	s := &Scheduler{}
	if s.Name() != "workload" {
		t.Errorf("Name() = %s, want workload", s.Name())
	}
}

func TestScheduler_ShouldSchedule(t *testing.T) {
	s := &Scheduler{}
	app := newTestApp("test", "default", v1beta2.DeployModeCluster)

	if !s.ShouldSchedule(app) {
		t.Error("ShouldSchedule() = false, want true")
	}
}

func TestSchedule_ClusterMode(t *testing.T) {
	fakeClient := newFakeClient()
	s := &Scheduler{client: fakeClient}

	app := newTestApp("test-app", "default", v1beta2.DeployModeCluster)

	err := s.Schedule(app)
	if err != nil {
		t.Fatalf("Schedule() failed: %v", err)
	}

	// Verify Workload was created
	workload := &schedulingv1alpha2.Workload{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{
		Name:      "test-app",
		Namespace: "default",
	}, workload)
	if err != nil {
		t.Errorf("Workload not created: %v", err)
	}

	// Verify PodGroup was created
	podGroup := &schedulingv1alpha2.PodGroup{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{
		Name:      "test-app-test-submission-1",
		Namespace: "default",
	}, podGroup)
	if err != nil {
		t.Errorf("PodGroup not created: %v", err)
	}

	// Verify minCount (3 executors only, driver not included)
	if podGroup.Spec.SchedulingPolicy.Gang.MinCount != 3 {
		t.Errorf("PodGroup minCount = %d, want 3", podGroup.Spec.SchedulingPolicy.Gang.MinCount)
	}

	// Verify scheduling group stamping - executor only, not driver
	if app.Spec.Driver.SchedulingGroup != nil {
		t.Error("Driver SchedulingGroup should not be set (driver schedules independently)")
	}

	if app.Spec.Executor.SchedulingGroup == nil {
		t.Error("Executor SchedulingGroup not set")
	} else if app.Spec.Executor.SchedulingGroup.PodGroupName != "test-app-test-submission-1" {
		t.Errorf("Executor PodGroupName = %s, want test-app-test-submission-1", app.Spec.Executor.SchedulingGroup.PodGroupName)
	}
}

func TestSchedule_ClientMode(t *testing.T) {
	fakeClient := newFakeClient()
	s := &Scheduler{client: fakeClient}

	app := newTestApp("test-app", "default", v1beta2.DeployModeClient)

	err := s.Schedule(app)
	if err != nil {
		t.Fatalf("Schedule() failed: %v", err)
	}

	// Verify PodGroup minCount (3 executors, no driver)
	podGroup := &schedulingv1alpha2.PodGroup{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{
		Name:      "test-app-test-submission-1",
		Namespace: "default",
	}, podGroup)
	if err != nil {
		t.Fatalf("PodGroup not created: %v", err)
	}

	if podGroup.Spec.SchedulingPolicy.Gang.MinCount != 3 {
		t.Errorf("PodGroup minCount = %d, want 3", podGroup.Spec.SchedulingPolicy.Gang.MinCount)
	}

	// Verify scheduling group stamping - executor only, not driver
	if app.Spec.Driver.SchedulingGroup != nil {
		t.Error("Driver SchedulingGroup should not be set in client mode")
	}

	if app.Spec.Executor.SchedulingGroup == nil {
		t.Error("Executor SchedulingGroup not set")
	}
}

func TestSchedule_ClusterModeDriverNotInPodGroup(t *testing.T) {
	fakeClient := newFakeClient()
	s := &Scheduler{client: fakeClient}

	app := newTestApp("test-app", "default", v1beta2.DeployModeCluster)

	err := s.Schedule(app)
	if err != nil {
		t.Fatalf("Schedule() failed: %v", err)
	}

	// CRITICAL: Verify driver is NOT in the PodGroup to prevent bootstrap deadlock
	if app.Spec.Driver.SchedulingGroup != nil {
		t.Error("Driver SchedulingGroup must be nil to prevent deadlock. " +
			"The cluster-mode driver creates executors and cannot wait for them in the same PodGroup.")
	}

	// Verify executor is in the PodGroup
	if app.Spec.Executor.SchedulingGroup == nil {
		t.Error("Executor SchedulingGroup should be set")
	}

	// Verify PodGroup only counts executors
	podGroup := &schedulingv1alpha2.PodGroup{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{
		Name:      "test-app-test-submission-1",
		Namespace: "default",
	}, podGroup)
	if err != nil {
		t.Fatalf("PodGroup not created: %v", err)
	}

	if podGroup.Spec.SchedulingPolicy.Gang.MinCount != 3 {
		t.Errorf("PodGroup minCount = %d, want 3 (executors only, driver excluded)", podGroup.Spec.SchedulingPolicy.Gang.MinCount)
	}
}

func TestSchedule_MinMemberOverride(t *testing.T) {
	fakeClient := newFakeClient()
	s := &Scheduler{client: fakeClient}

	app := newTestApp("test-app", "default", v1beta2.DeployModeCluster)
	app.Spec.BatchSchedulerOptions = &v1beta2.BatchSchedulerConfiguration{
		MinMember: ptr.To(int32(10)),
	}

	err := s.Schedule(app)
	if err != nil {
		t.Fatalf("Schedule() failed: %v", err)
	}

	// Verify PodGroup uses MinMember override
	podGroup := &schedulingv1alpha2.PodGroup{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{
		Name:      "test-app-test-submission-1",
		Namespace: "default",
	}, podGroup)
	if err != nil {
		t.Fatalf("PodGroup not created: %v", err)
	}

	if podGroup.Spec.SchedulingPolicy.Gang.MinCount != 10 {
		t.Errorf("PodGroup minCount = %d, want 10", podGroup.Spec.SchedulingPolicy.Gang.MinCount)
	}
}

func TestSchedule_DRAEnabled(t *testing.T) {
	fakeClient := newFakeClient()
	s := &Scheduler{client: fakeClient}

	app := newTestApp("test-app", "default", v1beta2.DeployModeCluster)
	app.Spec.DynamicAllocation = &v1beta2.DynamicAllocation{
		Enabled:          true,
		InitialExecutors: ptr.To(int32(5)),
		MinExecutors:     ptr.To(int32(2)),
		MaxExecutors:     ptr.To(int32(10)),
	}

	err := s.Schedule(app)
	if err != nil {
		t.Fatalf("Schedule() failed: %v", err)
	}

	// Verify PodGroup uses DRA initial executor count (5), not instances (3)
	podGroup := &schedulingv1alpha2.PodGroup{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{
		Name:      "test-app-test-submission-1",
		Namespace: "default",
	}, podGroup)
	if err != nil {
		t.Fatalf("PodGroup not created: %v", err)
	}

	// Expected: 5 initial executors from DRA (driver not included)
	expectedMinCount := int32(5)
	if podGroup.Spec.SchedulingPolicy.Gang.MinCount != expectedMinCount {
		t.Errorf("PodGroup minCount = %d, want %d (5 DRA initial executors, driver excluded)",
			podGroup.Spec.SchedulingPolicy.Gang.MinCount, expectedMinCount)
	}
}

func TestSchedule_Idempotent(t *testing.T) {
	fakeClient := newFakeClient()
	s := &Scheduler{client: fakeClient}

	app := newTestApp("test-app", "default", v1beta2.DeployModeCluster)

	// Call Schedule twice
	err := s.Schedule(app)
	if err != nil {
		t.Fatalf("First Schedule() failed: %v", err)
	}

	err = s.Schedule(app)
	if err != nil {
		t.Fatalf("Second Schedule() failed: %v", err)
	}

	// Verify no duplicate objects were created
	workloads := &schedulingv1alpha2.WorkloadList{}
	err = fakeClient.List(context.TODO(), workloads, client.InNamespace("default"))
	if err != nil {
		t.Fatalf("Failed to list Workloads: %v", err)
	}
	if len(workloads.Items) != 1 {
		t.Errorf("Expected 1 Workload, got %d", len(workloads.Items))
	}

	podGroups := &schedulingv1alpha2.PodGroupList{}
	err = fakeClient.List(context.TODO(), podGroups, client.InNamespace("default"))
	if err != nil {
		t.Fatalf("Failed to list PodGroups: %v", err)
	}
	if len(podGroups.Items) != 1 {
		t.Errorf("Expected 1 PodGroup, got %d", len(podGroups.Items))
	}
}

func TestSchedule_PriorityClassName(t *testing.T) {
	fakeClient := newFakeClient()
	s := &Scheduler{client: fakeClient}

	app := newTestApp("test-app", "default", v1beta2.DeployModeCluster)
	app.Spec.BatchSchedulerOptions = &v1beta2.BatchSchedulerConfiguration{
		PriorityClassName: ptr.To("high-priority"),
	}

	err := s.Schedule(app)
	if err != nil {
		t.Fatalf("Schedule() failed: %v", err)
	}

	// Verify Workload has priority
	workload := &schedulingv1alpha2.Workload{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{
		Name:      "test-app",
		Namespace: "default",
	}, workload)
	if err != nil {
		t.Fatalf("Workload not created: %v", err)
	}

	if len(workload.Spec.PodGroupTemplates) == 0 {
		t.Fatal("No PodGroupTemplates in Workload")
	}

	if workload.Spec.PodGroupTemplates[0].PriorityClassName != "high-priority" {
		t.Errorf("Workload PriorityClassName = %s, want high-priority",
			workload.Spec.PodGroupTemplates[0].PriorityClassName)
	}

	// Verify PodGroup has priority
	podGroup := &schedulingv1alpha2.PodGroup{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{
		Name:      "test-app-test-submission-1",
		Namespace: "default",
	}, podGroup)
	if err != nil {
		t.Fatalf("PodGroup not created: %v", err)
	}

	if podGroup.Spec.PriorityClassName != "high-priority" {
		t.Errorf("PodGroup PriorityClassName = %s, want high-priority", podGroup.Spec.PriorityClassName)
	}
}

func TestSchedule_OwnerReferences(t *testing.T) {
	fakeClient := newFakeClient()
	s := &Scheduler{client: fakeClient}

	app := newTestApp("test-app", "default", v1beta2.DeployModeCluster)

	err := s.Schedule(app)
	if err != nil {
		t.Fatalf("Schedule() failed: %v", err)
	}

	// Verify Workload has owner reference
	workload := &schedulingv1alpha2.Workload{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{
		Name:      "test-app",
		Namespace: "default",
	}, workload)
	if err != nil {
		t.Fatalf("Workload not created: %v", err)
	}

	if len(workload.OwnerReferences) != 1 {
		t.Fatalf("Expected 1 owner reference, got %d", len(workload.OwnerReferences))
	}

	ownerRef := workload.OwnerReferences[0]
	if ownerRef.Kind != "SparkApplication" || ownerRef.Name != "test-app" {
		t.Errorf("Workload owner reference = %s/%s, want SparkApplication/test-app", ownerRef.Kind, ownerRef.Name)
	}

	if ownerRef.Controller == nil || !*ownerRef.Controller {
		t.Error("Workload owner reference should have controller=true")
	}

	// Verify PodGroup has owner reference
	podGroup := &schedulingv1alpha2.PodGroup{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{
		Name:      "test-app-test-submission-1",
		Namespace: "default",
	}, podGroup)
	if err != nil {
		t.Fatalf("PodGroup not created: %v", err)
	}

	if len(podGroup.OwnerReferences) != 1 {
		t.Fatalf("Expected 1 owner reference, got %d", len(podGroup.OwnerReferences))
	}

	ownerRef = podGroup.OwnerReferences[0]
	if ownerRef.Kind != "SparkApplication" || ownerRef.Name != "test-app" {
		t.Errorf("PodGroup owner reference = %s/%s, want SparkApplication/test-app", ownerRef.Kind, ownerRef.Name)
	}

	if ownerRef.Controller == nil || !*ownerRef.Controller {
		t.Error("PodGroup owner reference should have controller=true")
	}
}

func TestCleanup_Success(t *testing.T) {
	fakeClient := newFakeClient()
	s := &Scheduler{client: fakeClient}

	app := newTestApp("test-app", "default", v1beta2.DeployModeCluster)

	// Create objects first
	err := s.Schedule(app)
	if err != nil {
		t.Fatalf("Schedule() failed: %v", err)
	}

	// Verify both exist
	workload := &schedulingv1alpha2.Workload{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{Name: "test-app", Namespace: "default"}, workload)
	if err != nil {
		t.Fatal("Workload should exist before cleanup")
	}

	podGroup := &schedulingv1alpha2.PodGroup{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{Name: "test-app-test-submission-1", Namespace: "default"}, podGroup)
	if err != nil {
		t.Fatal("PodGroup should exist before cleanup")
	}

	// Cleanup
	err = s.Cleanup(app)
	if err != nil {
		t.Fatalf("Cleanup() failed: %v", err)
	}

	// Verify PodGroup is deleted
	err = fakeClient.Get(context.TODO(), types.NamespacedName{Name: "test-app-test-submission-1", Namespace: "default"}, podGroup)
	if !apierrors.IsNotFound(err) {
		t.Error("PodGroup should be deleted after cleanup")
	}

	// Verify Workload still exists (not deleted by Cleanup)
	err = fakeClient.Get(context.TODO(), types.NamespacedName{Name: "test-app", Namespace: "default"}, workload)
	if err != nil {
		t.Error("Workload should NOT be deleted by cleanup (GC'd via ownerReference)")
	}
}

func TestCleanup_ToleratesNotFound(t *testing.T) {
	fakeClient := newFakeClient()
	s := &Scheduler{client: fakeClient}

	app := newTestApp("test-app", "default", v1beta2.DeployModeCluster)

	// Cleanup without creating objects first
	err := s.Cleanup(app)
	if err != nil {
		t.Errorf("Cleanup() should tolerate NotFound, got error: %v", err)
	}
}

func TestCalculateMinCount_ClusterMode(t *testing.T) {
	app := newTestApp("test", "default", v1beta2.DeployModeCluster)
	minCount := calculateMinCount(app)

	// Expected: 3 executors (driver not included in PodGroup)
	if minCount != 3 {
		t.Errorf("calculateMinCount() = %d, want 3", minCount)
	}
}

func TestCalculateMinCount_ClientMode(t *testing.T) {
	app := newTestApp("test", "default", v1beta2.DeployModeClient)
	minCount := calculateMinCount(app)

	// Expected: 3 executors (no driver)
	if minCount != 3 {
		t.Errorf("calculateMinCount() = %d, want 3", minCount)
	}
}

func TestGetWorkloadName(t *testing.T) {
	app := newTestApp("my-app", "default", v1beta2.DeployModeCluster)
	name := getWorkloadName(app)

	if name != "my-app" {
		t.Errorf("getWorkloadName() = %s, want my-app", name)
	}
}

func TestGetPodGroupName(t *testing.T) {
	app := newTestApp("my-app", "default", v1beta2.DeployModeCluster)
	app.Status.SubmissionID = "sub-123"

	name := getPodGroupName(app)

	expected := "my-app-sub-123"
	if name != expected {
		t.Errorf("getPodGroupName() = %s, want %s", name, expected)
	}
}

// Mock discovery client for testing
type mockDiscoveryClient struct {
	discovery.DiscoveryInterface
	serverResourcesFunc func(groupVersion string) (*metav1.APIResourceList, error)
}

func (m *mockDiscoveryClient) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	if m.serverResourcesFunc != nil {
		return m.serverResourcesFunc(groupVersion)
	}
	return nil, errors.New("not implemented")
}

// Mock invalid config (not *Config)
type invalidConfig struct{}

func (i *invalidConfig) Validate() error { return nil }

func TestFactory_ValidConfig(t *testing.T) {
	fakeClient := newFakeClient()
	restConfig := &rest.Config{}

	mockDiscovery := &mockDiscoveryClient{
		serverResourcesFunc: func(groupVersion string) (*metav1.APIResourceList, error) {
			if groupVersion == workloadAPIGroupVersion {
				return &metav1.APIResourceList{
					GroupVersion: workloadAPIGroupVersion,
				}, nil
			}
			return nil, errors.New("not found")
		},
	}

	config := &Config{
		RestConfig:      restConfig,
		Client:          fakeClient,
		DiscoveryClient: mockDiscovery,
	}

	scheduler, err := Factory(config)
	if err != nil {
		t.Fatalf("Factory() failed with valid config: %v", err)
	}

	if scheduler == nil {
		t.Fatal("Factory() returned nil scheduler")
	}

	if scheduler.Name() != SchedulerName {
		t.Errorf("Scheduler name = %s, want %s", scheduler.Name(), SchedulerName)
	}
}

func TestFactory_InvalidConfig(t *testing.T) {
	config := &invalidConfig{}

	_, err := Factory(config)
	if err == nil {
		t.Fatal("Factory() should fail with invalid config type")
	}

	if err.Error() != "failed to get workload scheduler config" {
		t.Errorf("Factory() error = %v, want 'failed to get workload scheduler config'", err)
	}
}

func TestFactory_NilClient(t *testing.T) {
	restConfig := &rest.Config{}
	mockDiscovery := &mockDiscoveryClient{
		serverResourcesFunc: func(groupVersion string) (*metav1.APIResourceList, error) {
			return &metav1.APIResourceList{GroupVersion: groupVersion}, nil
		},
	}

	config := &Config{
		RestConfig:      restConfig,
		Client:          nil, // Nil client
		DiscoveryClient: mockDiscovery,
	}

	_, err := Factory(config)
	if err == nil {
		t.Fatal("Factory() should fail when Client is nil")
	}

	if !strings.Contains(err.Error(), "Client is required") {
		t.Errorf("Error should mention Client is required, got: %v", err)
	}
}

func TestFactory_NilRestConfigWithoutDiscoveryClient(t *testing.T) {
	fakeClient := newFakeClient()

	config := &Config{
		RestConfig:      nil, // Nil REST config
		Client:          fakeClient,
		DiscoveryClient: nil, // No discovery client provided
	}

	_, err := Factory(config)
	if err == nil {
		t.Fatal("Factory() should fail when RestConfig is nil and DiscoveryClient is not provided")
	}

	if !strings.Contains(err.Error(), "RestConfig is required") {
		t.Errorf("Error should mention RestConfig is required, got: %v", err)
	}
}

func TestFactory_ValidConfigWithInjectedDiscoveryClient(t *testing.T) {
	fakeClient := newFakeClient()

	// When discovery client is injected, REST config can be nil (for unit testing)
	mockDiscovery := &mockDiscoveryClient{
		serverResourcesFunc: func(groupVersion string) (*metav1.APIResourceList, error) {
			if groupVersion == workloadAPIGroupVersion {
				return &metav1.APIResourceList{GroupVersion: groupVersion}, nil
			}
			return nil, errors.New("not found")
		},
	}

	config := &Config{
		RestConfig:      nil, // Nil is OK when DiscoveryClient is provided
		Client:          fakeClient,
		DiscoveryClient: mockDiscovery,
	}

	scheduler, err := Factory(config)
	if err != nil {
		t.Fatalf("Factory() should succeed with injected discovery client even with nil RestConfig: %v", err)
	}

	if scheduler == nil {
		t.Fatal("Factory() returned nil scheduler")
	}
}

func TestFactory_APINotAvailable(t *testing.T) {
	fakeClient := newFakeClient()
	restConfig := &rest.Config{}

	originalErr := errors.New("API not available")
	mockDiscovery := &mockDiscoveryClient{
		serverResourcesFunc: func(groupVersion string) (*metav1.APIResourceList, error) {
			return nil, originalErr
		},
	}

	config := &Config{
		RestConfig:      restConfig,
		Client:          fakeClient,
		DiscoveryClient: mockDiscovery,
	}

	_, err := Factory(config)
	if err == nil {
		t.Fatal("Factory() should fail when API is not available")
	}

	// Verify error message mentions the requirement
	errMsg := err.Error()
	if !strings.Contains(errMsg, workloadAPIGroupVersion) {
		t.Errorf("Error should mention %s, got: %v", workloadAPIGroupVersion, err)
	}
	if !strings.Contains(errMsg, "v1.36") || !strings.Contains(errMsg, "GenericWorkload") {
		t.Errorf("Error should mention Kubernetes v1.36+ and GenericWorkload feature, got: %v", err)
	}

	// Verify the original error is wrapped
	if !errors.Is(err, originalErr) {
		t.Errorf("Error should wrap the original discovery error")
	}
}

func TestSchedule_WorkloadControllerRef(t *testing.T) {
	fakeClient := newFakeClient()
	s := &Scheduler{client: fakeClient}

	app := newTestApp("test-app", "default", v1beta2.DeployModeCluster)

	err := s.Schedule(app)
	if err != nil {
		t.Fatalf("Schedule() failed: %v", err)
	}

	// Verify Workload ControllerRef
	workload := &schedulingv1alpha2.Workload{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{
		Name:      "test-app",
		Namespace: "default",
	}, workload)
	if err != nil {
		t.Fatalf("Workload not created: %v", err)
	}

	if workload.Spec.ControllerRef == nil {
		t.Fatal("Workload ControllerRef is nil")
	}

	ctrlRef := workload.Spec.ControllerRef
	if ctrlRef.APIGroup != v1beta2.GroupVersion.Group {
		t.Errorf("ControllerRef APIGroup = %s, want %s", ctrlRef.APIGroup, v1beta2.GroupVersion.Group)
	}
	if ctrlRef.Kind != "SparkApplication" {
		t.Errorf("ControllerRef Kind = %s, want SparkApplication", ctrlRef.Kind)
	}
	if ctrlRef.Name != "test-app" {
		t.Errorf("ControllerRef Name = %s, want test-app", ctrlRef.Name)
	}
}

func TestSchedule_PodGroupTemplateRef(t *testing.T) {
	fakeClient := newFakeClient()
	s := &Scheduler{client: fakeClient}

	app := newTestApp("test-app", "default", v1beta2.DeployModeCluster)

	err := s.Schedule(app)
	if err != nil {
		t.Fatalf("Schedule() failed: %v", err)
	}

	// Verify PodGroup template reference
	podGroup := &schedulingv1alpha2.PodGroup{}
	err = fakeClient.Get(context.TODO(), types.NamespacedName{
		Name:      "test-app-test-submission-1",
		Namespace: "default",
	}, podGroup)
	if err != nil {
		t.Fatalf("PodGroup not created: %v", err)
	}

	if podGroup.Spec.PodGroupTemplateRef == nil {
		t.Fatal("PodGroup PodGroupTemplateRef is nil")
	}

	templateRef := podGroup.Spec.PodGroupTemplateRef
	if templateRef.Workload == nil {
		t.Fatal("PodGroupTemplateRef.Workload is nil")
	}

	if templateRef.Workload.WorkloadName != "test-app" {
		t.Errorf("PodGroupTemplateRef WorkloadName = %s, want test-app", templateRef.Workload.WorkloadName)
	}
	if templateRef.Workload.PodGroupTemplateName != podGroupTemplateName {
		t.Errorf("PodGroupTemplateRef PodGroupTemplateName = %s, want %s",
			templateRef.Workload.PodGroupTemplateName, podGroupTemplateName)
	}
}
