/*
Copyright 2026 The Kubeflow authors.

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

package webhook

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// --- helpers ---

func newClient(t *testing.T, namespaces ...*corev1.Namespace) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	builder := fake.NewClientBuilder().WithScheme(scheme)
	for _, ns := range namespaces {
		builder = builder.WithObjects(ns)
	}
	return builder.Build()
}

func newNamespace(name string, labels map[string]string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func newPod(namespace string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: namespace}}
}

func newMatcher(t *testing.T, namespaces []string, selector string) *util.NamespaceMatcher {
	t.Helper()
	m, err := util.NewNamespaceMatcher(namespaces, selector)
	require.NoError(t, err)
	return m
}

// hasNamespace returns a testify argument matcher that succeeds when the
// runtime.Object argument is a namespaced object with the given namespace.
func hasNamespace(ns string) interface{} {
	return mock.MatchedBy(func(o runtime.Object) bool {
		m, ok := o.(metav1.Object)
		return ok && m.GetNamespace() == ns
	})
}

// --- mocks ---

type mockDefaulter struct {
	mock.Mock
}

func (m *mockDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	return m.Called(ctx, obj).Error(0)
}

type mockValidator struct {
	mock.Mock
}

func (m *mockValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	args := m.Called(ctx, obj)
	return warningsFrom(args, 0), args.Error(1)
}

func (m *mockValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	args := m.Called(ctx, oldObj, newObj)
	return warningsFrom(args, 0), args.Error(1)
}

func (m *mockValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	args := m.Called(ctx, obj)
	return warningsFrom(args, 0), args.Error(1)
}

func warningsFrom(args mock.Arguments, idx int) admission.Warnings {
	if v := args.Get(idx); v != nil {
		return v.(admission.Warnings)
	}
	return nil
}

// runtime.Object that intentionally does not implement metav1.Object — used to
// exercise the wrapper's pass-through branch for non-namespaced objects.
type bareRuntimeObject struct{}

func (*bareRuntimeObject) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }
func (*bareRuntimeObject) DeepCopyObject() runtime.Object   { return &bareRuntimeObject{} }

// --- defaulter tests ---

func TestNamespaceFilteringDefaulter_MatchesExplicitNamespace(t *testing.T) {
	inner := &mockDefaulter{}
	inner.On("Default", mock.Anything, hasNamespace("ns-a")).Return(nil).Once()

	w := NewNamespaceFilteringDefaulter(inner, newMatcher(t, []string{"ns-a"}, ""), newClient(t))
	require.NoError(t, w.Default(context.Background(), newPod("ns-a")))

	inner.AssertExpectations(t)
}

func TestNamespaceFilteringDefaulter_SkipsUnmatchedNamespace(t *testing.T) {
	inner := &mockDefaulter{}
	inner.On("Default", mock.Anything, mock.Anything).Return(nil).Maybe()

	w := NewNamespaceFilteringDefaulter(inner, newMatcher(t, []string{"ns-a"}, ""), newClient(t))
	require.NoError(t, w.Default(context.Background(), newPod("ns-b")))

	inner.AssertNotCalled(t, "Default")
}

func TestNamespaceFilteringDefaulter_MatchesByLabelSelector(t *testing.T) {
	c := newClient(t,
		newNamespace("matched", map[string]string{"tenant": "tenantA"}),
		newNamespace("unmatched", map[string]string{"tenant": "tenantB"}),
	)

	inner := &mockDefaulter{}
	inner.On("Default", mock.Anything, hasNamespace("matched")).Return(nil).Once()

	w := NewNamespaceFilteringDefaulter(inner, newMatcher(t, nil, "tenant=tenantA"), c)
	require.NoError(t, w.Default(context.Background(), newPod("matched")))
	require.NoError(t, w.Default(context.Background(), newPod("unmatched")))

	inner.AssertExpectations(t)
	inner.AssertNumberOfCalls(t, "Default", 1)
}

func TestNamespaceFilteringDefaulter_EmptyConfigMatchesEverything(t *testing.T) {
	inner := &mockDefaulter{}
	inner.On("Default", mock.Anything, mock.Anything).Return(nil).Once()

	w := NewNamespaceFilteringDefaulter(inner, newMatcher(t, nil, ""), newClient(t))
	require.NoError(t, w.Default(context.Background(), newPod("any-namespace")))

	inner.AssertExpectations(t)
}

func TestNamespaceFilteringDefaulter_PassesThroughObjectsWithoutMeta(t *testing.T) {
	inner := &mockDefaulter{}
	inner.On("Default", mock.Anything, mock.Anything).Return(nil).Once()

	// Explicit namespace list that would normally reject everything; the wrapper
	// must still pass through objects that don't implement metav1.Object.
	w := NewNamespaceFilteringDefaulter(inner, newMatcher(t, []string{"ns-a"}, ""), newClient(t))
	require.NoError(t, w.Default(context.Background(), &bareRuntimeObject{}))

	inner.AssertExpectations(t)
}

func TestNamespaceFilteringDefaulter_PropagatesInnerError(t *testing.T) {
	inner := &mockDefaulter{}
	inner.On("Default", mock.Anything, mock.Anything).Return(errors.New("boom")).Once()

	w := NewNamespaceFilteringDefaulter(inner, newMatcher(t, []string{"ns-a"}, ""), newClient(t))
	err := w.Default(context.Background(), newPod("ns-a"))

	assert.EqualError(t, err, "boom")
	inner.AssertExpectations(t)
}

// --- validator tests ---

func TestNamespaceFilteringValidator_GatesAllThreeMethods(t *testing.T) {
	inner := &mockValidator{}
	inner.On("ValidateCreate", mock.Anything, hasNamespace("ns-a")).Return(nil, nil).Once()
	inner.On("ValidateUpdate", mock.Anything, mock.Anything, hasNamespace("ns-a")).Return(nil, nil).Once()
	inner.On("ValidateDelete", mock.Anything, hasNamespace("ns-a")).Return(nil, nil).Once()

	w := NewNamespaceFilteringValidator(inner, newMatcher(t, []string{"ns-a"}, ""), newClient(t))
	ctx := context.Background()
	matched := newPod("ns-a")
	unmatched := newPod("ns-b")

	_, err := w.ValidateCreate(ctx, matched)
	require.NoError(t, err)
	_, err = w.ValidateUpdate(ctx, matched, matched)
	require.NoError(t, err)
	_, err = w.ValidateDelete(ctx, matched)
	require.NoError(t, err)

	_, err = w.ValidateCreate(ctx, unmatched)
	require.NoError(t, err)
	_, err = w.ValidateUpdate(ctx, unmatched, unmatched)
	require.NoError(t, err)
	_, err = w.ValidateDelete(ctx, unmatched)
	require.NoError(t, err)

	inner.AssertExpectations(t)
	inner.AssertNumberOfCalls(t, "ValidateCreate", 1)
	inner.AssertNumberOfCalls(t, "ValidateUpdate", 1)
	inner.AssertNumberOfCalls(t, "ValidateDelete", 1)
}

func TestNamespaceFilteringValidator_UpdateUsesNewObjectNamespace(t *testing.T) {
	inner := &mockValidator{}
	// Expectation only succeeds if the wrapper passes newObj (ns-new) to the inner.
	// If it ever used oldObj's namespace (ns-old), the expectation would not match.
	inner.On("ValidateUpdate",
		mock.Anything,
		hasNamespace("ns-old"),
		hasNamespace("ns-new"),
	).Return(nil, nil).Once()

	w := NewNamespaceFilteringValidator(inner, newMatcher(t, []string{"ns-new"}, ""), newClient(t))
	_, err := w.ValidateUpdate(context.Background(), newPod("ns-old"), newPod("ns-new"))
	require.NoError(t, err)

	inner.AssertExpectations(t)
}

func TestNamespaceFilteringValidator_PropagatesInnerError(t *testing.T) {
	inner := &mockValidator{}
	inner.On("ValidateCreate", mock.Anything, mock.Anything).Return(nil, errors.New("boom")).Once()

	w := NewNamespaceFilteringValidator(inner, newMatcher(t, []string{"ns-a"}, ""), newClient(t))
	_, err := w.ValidateCreate(context.Background(), newPod("ns-a"))

	assert.EqualError(t, err, "boom")
	inner.AssertExpectations(t)
}
