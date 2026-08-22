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
	"strings"
	"testing"
)

func TestParseSchedule(t *testing.T) {
	tests := []struct {
		name           string
		schedule       string
		timezone       string
		wantErr        bool
		wantErrContent string
	}{
		// Valid inputs: documented schedule shapes accepted by the controller.
		{name: "valid 5-field cron", schedule: "*/5 * * * *", timezone: "", wantErr: false},
		{name: "valid descriptor", schedule: "@hourly", timezone: "", wantErr: false},
		{name: "valid every descriptor", schedule: "@every 1h", timezone: "", wantErr: false},
		{name: "valid IANA timezone", schedule: "0 12 * * *", timezone: "America/New_York", wantErr: false},
		{name: "valid UTC timezone", schedule: "0 12 * * *", timezone: "UTC", wantErr: false},
		{name: "embedded CRON_TZ takes precedence over field", schedule: "CRON_TZ=UTC 0 12 * * *", timezone: "America/New_York", wantErr: false},
		{name: "embedded TZ= prefix is honoured", schedule: "TZ=UTC 0 12 * * *", timezone: "", wantErr: false},

		// Failure modes: each must return an error whose content distinguishes
		// the failure category so callers can give users a precise reason.
		{name: "unknown timezone", schedule: "*/5 * * * *", timezone: "Mars/Olympus_Mons", wantErr: true, wantErrContent: "invalid timezone"},
		{name: "garbage schedule", schedule: "not a cron", timezone: "", wantErr: true, wantErrContent: "invalid schedule"},
		{name: "out-of-range field", schedule: "60 * * * *", timezone: "", wantErr: true, wantErrContent: "invalid schedule"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sched, err := ParseSchedule(tt.schedule, tt.timezone)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErrContent)
				}
				if !strings.Contains(err.Error(), tt.wantErrContent) {
					t.Fatalf("expected error containing %q, got %q", tt.wantErrContent, err.Error())
				}
				if sched != nil {
					t.Fatalf("expected nil schedule on error, got %#v", sched)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if sched == nil {
				t.Fatalf("expected non-nil schedule, got nil")
			}
		})
	}
}
