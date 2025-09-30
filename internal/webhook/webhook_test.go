package webhook

import (
	"context"
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Note: The maxAppNameLength constant must also be defined in your main code.
// We define it here for the test. The value 63 is a common length
// limit for labels in Kubernetes.

func TestValidateNameLength(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name        string
		appName     string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "name with valid length",
			appName:     "my-awesome-application",
			expectError: false,
		},
		{
			name:        "empty name",
			appName:     "",
			expectError: false, // Length is 0, which is less than the limit
		},
		{
			name:        "name at max length boundary",
			appName:     strings.Repeat("a", maxAppNameLength),
			expectError: false,
		},
		{
			name:        "name exceeding max length by 1 character",
			appName:     strings.Repeat("a", maxAppNameLength+1),
			expectError: true,
			errorMsg:    fmt.Sprintf("metadata.name: Invalid value: %q: name must be no more than %d characters to allow for resource suffixes", strings.Repeat("a", maxAppNameLength+1), maxAppNameLength),
		},
		{
			name:        "long name significantly over the limit",
			appName:     "this-is-a-very-very-very-long-application-name-that-definitely-exceeds-the-kubernetes-label-length-limit",
			expectError: true,
			errorMsg:    fmt.Sprintf("metadata.name: Invalid value: %q: name must be no more than %d characters to allow for resource suffixes", "this-is-a-very-very-very-long-application-name-that-definitely-exceeds-the-kubernetes-label-length-limit", maxAppNameLength),
		},
	}

	// Run tests in a loop
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Prepare test data
			appMeta := metav1.ObjectMeta{
				Name: tc.appName,
			}

			// Call the function under test
			err := validateNameLength(context.Background(), appMeta)

			// Check the result
			if tc.expectError {
				if err == nil {
					t.Errorf("expected an error but got nil")
					return
				}
				if err.Error() != tc.errorMsg {
					t.Errorf("expected error message:\n%q\ngot:\n%q", tc.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}
