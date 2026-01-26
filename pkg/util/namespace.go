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

package util

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NamespaceMatcher determines if a namespace should be watched based on
// explicit namespace list and/or label selector.
type NamespaceMatcher struct {
	explicitNamespaces map[string]bool
	selector           labels.Selector
	hasSelector        bool
}

// NewNamespaceMatcher creates a new NamespaceMatcher.
// Matching behavior:
// - Empty namespace list + empty selector = watch all namespaces (NamespaceAll)
// - Empty namespace list + selector = watch only namespaces matching selector
// - Explicit namespace list + empty selector = watch only explicit namespaces
// - Explicit namespace list + selector = watch explicit namespaces OR matching selector (OR logic)
func NewNamespaceMatcher(explicitNamespaces []string, selectorString string) (*NamespaceMatcher, error) {
	var selector labels.Selector
	var hasSelector bool
	var err error

	if selectorString != "" {
		selector, err = labels.Parse(selectorString)
		if err != nil {
			return nil, fmt.Errorf("invalid namespace selector %q: %v", selectorString, err)
		}
		hasSelector = true
	}

	nsMap := make(map[string]bool)
	if len(explicitNamespaces) == 0 && !hasSelector {
		// Only set NamespaceAll when no explicit namespaces AND no selector
		// This maintains backwards compatibility: empty config = watch all
		nsMap[metav1.NamespaceAll] = true
	} else {
		for _, ns := range explicitNamespaces {
			nsMap[ns] = true
		}
	}

	return &NamespaceMatcher{
		explicitNamespaces: nsMap,
		selector:           selector,
		hasSelector:        hasSelector,
	}, nil
}

// Matches returns true if the given namespace should be watched.
// This is a fast-path method that doesn't require a client lookup.
func (m *NamespaceMatcher) Matches(namespace string, nsLabels map[string]string) bool {
	// Check if watching all namespaces
	if m.explicitNamespaces[metav1.NamespaceAll] {
		return true
	}

	// Check explicit namespace list
	if m.explicitNamespaces[namespace] {
		return true
	}

	// Check label selector
	if m.hasSelector && m.selector.Matches(labels.Set(nsLabels)) {
		return true
	}

	return false
}

// MatchesNamespace is a convenience method that takes a Namespace object.
func (m *NamespaceMatcher) MatchesNamespace(ns *corev1.Namespace) bool {
	return m.Matches(ns.Name, ns.Labels)
}

// MatchesWithClient retrieves the namespace from the cache and checks if it matches.
// This is useful in EventFilters where you only have the namespace name.
func (m *NamespaceMatcher) MatchesWithClient(ctx context.Context, c client.Client, namespaceName string) (bool, error) {
	// Fast-path: if watching all namespaces, always match
	if m.explicitNamespaces[metav1.NamespaceAll] {
		return true, nil
	}

	// Fast-path: if namespace is in explicit list, no need to check labels
	if m.explicitNamespaces[namespaceName] {
		return true, nil
	}

	// If no selector is configured and namespace not in explicit list, no match
	if !m.hasSelector {
		return false, nil
	}

	// Need to retrieve namespace from cache to check labels
	ns := &corev1.Namespace{}
	if err := c.Get(ctx, client.ObjectKey{Name: namespaceName}, ns); err != nil {
		return false, fmt.Errorf("failed to get namespace %s: %v", namespaceName, err)
	}

	return m.selector.Matches(labels.Set(ns.Labels)), nil
}
