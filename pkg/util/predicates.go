package util

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// NamespacePredicate is a predicate.Predicate that filters events based on the namespaces.
type NamespacePredicate struct {
	namespaces map[string]bool
}

// NewNamespacePredicate creates a new NamespacePredicate instance.
func NewNamespacePredicate(namespaces []string) predicate.Predicate {
	nsMap := make(map[string]bool)
	if len(namespaces) == 0 {
		nsMap[metav1.NamespaceAll] = true
	} else {
		for _, ns := range namespaces {
			nsMap[ns] = true
		}
	}

	return &NamespacePredicate{
		namespaces: nsMap,
	}
}

// Create implements the predicate.Predicate interface.
func (p *NamespacePredicate) Create(e event.CreateEvent) bool {
	return p.predicate(e.Object)
}

// Update implements the predicate.Predicate interface.
func (p *NamespacePredicate) Update(e event.UpdateEvent) bool {
	return p.predicate(e.ObjectNew)
}

// Delete implements the predicate.Predicate interface.
func (p *NamespacePredicate) Delete(e event.DeleteEvent) bool {
	return p.namespaces[e.Object.GetNamespace()]
}

// Generic implements the predicate.Predicate interface.
func (p *NamespacePredicate) Generic(e event.GenericEvent) bool {
	return p.predicate(e.Object)
}

// predicate filters events by namespace.
func (p *NamespacePredicate) predicate(obj client.Object) bool {
	return p.namespaces[metav1.NamespaceAll] || p.namespaces[obj.GetNamespace()]
}

// NamespacePredicate implements the predicate.Predicate interface.
var _ predicate.Predicate = &NamespacePredicate{}

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
