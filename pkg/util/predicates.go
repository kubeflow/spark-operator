package util

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// namespacePredicate is a predicate.Predicate
// that filters events based on namespace list and/or label selector.
type namespacePredicate struct {
	client           client.Client
	namespaceMatcher *NamespaceMatcher
}

// NewNamespacePredicate creates a predicate that filters events based on
// namespace list and/or label selector.
//
// If namespaceSelector is empty, only explicit namespaces are matched.
// If namespaces is empty, all namespaces are matched.
func NewNamespacePredicate(c client.Client, namespaces []string, namespaceSelector string) (predicate.Predicate, error) {
	matcher, err := NewNamespaceMatcher(namespaces, namespaceSelector)
	if err != nil {
		return nil, err
	}

	return &namespacePredicate{
		client:           c,
		namespaceMatcher: matcher,
	}, nil
}

// Create implements the predicate.Predicate interface.
func (p *namespacePredicate) Create(e event.CreateEvent) bool {
	matched, err := p.namespaceMatcher.MatchesWithClient(context.TODO(), p.client, e.Object.GetNamespace())
	if err != nil {
		return false
	}
	return matched
}

// Update implements the predicate.Predicate interface.
func (p *namespacePredicate) Update(e event.UpdateEvent) bool {
	matched, err := p.namespaceMatcher.MatchesWithClient(context.TODO(), p.client, e.ObjectNew.GetNamespace())
	if err != nil {
		return false
	}
	return matched
}

// Delete implements the predicate.Predicate interface.
func (p *namespacePredicate) Delete(e event.DeleteEvent) bool {
	matched, err := p.namespaceMatcher.MatchesWithClient(context.TODO(), p.client, e.Object.GetNamespace())
	if err != nil {
		return false
	}
	return matched
}

// Generic implements the predicate.Predicate interface.
func (p *namespacePredicate) Generic(e event.GenericEvent) bool {
	matched, err := p.namespaceMatcher.MatchesWithClient(context.TODO(), p.client, e.Object.GetNamespace())
	if err != nil {
		return false
	}
	return matched
}

// namespacePredicate implements the predicate.Predicate interface.
var _ predicate.Predicate = &namespacePredicate{}

// LabelPredicate is a predicate.Predicate that filters events based on label selectors.
type LabelPredicate struct {
	labels map[string]string
}

// NewLabelPredicate creates a new LabelPredicate instance.
func NewLabelPredicate(labels map[string]string) *LabelPredicate {
	return &LabelPredicate{
		labels: labels,
	}
}

// Create implements the predicate.Predicate interface.
func (p *LabelPredicate) Create(e event.CreateEvent) bool {
	return p.predicate(e.Object)
}

// Update implements the predicate.Predicate interface.
func (p *LabelPredicate) Update(e event.UpdateEvent) bool {
	return p.predicate(e.ObjectNew)
}

// Delete implements the predicate.Predicate interface.
func (p *LabelPredicate) Delete(e event.DeleteEvent) bool {
	return p.predicate(e.Object)
}

// Generic implements the predicate.Predicate interface.
func (p *LabelPredicate) Generic(e event.GenericEvent) bool {
	return p.predicate(e.Object)
}

// LabelPredicate implements the predicate.Predicate interface.
var _ predicate.Predicate = &LabelPredicate{}

// predicate filters events based on labels.
func (p *LabelPredicate) predicate(obj client.Object) bool {
	labels := obj.GetLabels()
	for k, v := range p.labels {
		if labels[k] != v {
			return false
		}
	}
	return true
}
