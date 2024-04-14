package cmd

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/duration"
)

func getSinceTime(timestamp metav1.Time) string {
	if timestamp.IsZero() {
		return "N.A."
	}

	return duration.ShortHumanDuration(time.Since(timestamp.Time))
}

func formatNotAvailable(info string) string {
	if info == "" {
		return "N.A."
	}
	return info
}
