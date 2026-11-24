/*
Copyright The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"errors"
	"fmt"
	"strings"
	"time"

	_ "time/tzdata"

	"github.com/robfig/cron/v3"
)

// ErrInvalidTimeZone is returned when the supplied timezone cannot be resolved.
var ErrInvalidTimeZone = errors.New("invalid timezone")

// ErrInvalidSchedule is returned when the cron expression cannot be parsed.
var ErrInvalidSchedule = errors.New("invalid schedule")

// ParseSchedule converts a ScheduledSparkApplication.Spec.Schedule and
// Spec.TimeZone into a cron.Schedule. The controller and the validating
// webhook both use it, so they accept exactly the same inputs.
//
// An empty timezone means Local; a non-empty one must resolve. An embedded
// CRON_TZ=/TZ= prefix takes precedence over the supplied timezone.
//
// On error the schedule is nil and the error distinguishes a bad timezone from
// a bad schedule.
func ParseSchedule(schedule, timezone string) (cron.Schedule, error) {
	if timezone == "" {
		timezone = "Local"
	} else if _, err := time.LoadLocation(timezone); err != nil {
		return nil, fmt.Errorf("%w %q: %v", ErrInvalidTimeZone, timezone, err)
	}

	cronSchedule := schedule
	if strings.HasPrefix(cronSchedule, "CRON_TZ=") || strings.HasPrefix(cronSchedule, "TZ=") {
		// cron.ParseStandard panics when the embedded timezone prefix is not
		// followed by a space and a schedule, so reject that form here.
		if _, expr, ok := strings.Cut(cronSchedule, " "); !ok || strings.TrimSpace(expr) == "" {
			return nil, fmt.Errorf("%w %q: embedded timezone prefix must be followed by a space and a schedule", ErrInvalidSchedule, schedule)
		}
	} else {
		cronSchedule = fmt.Sprintf("CRON_TZ=%s %s", timezone, cronSchedule)
	}
	parsed, err := cron.ParseStandard(cronSchedule)
	if err != nil {
		return nil, fmt.Errorf("%w %q: %v", ErrInvalidSchedule, schedule, err)
	}
	return parsed, nil
}
