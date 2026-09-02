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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// ErrInvalidTimeZone is returned by ParseSchedule when the supplied timezone
// is non-empty but cannot be resolved by time.LoadLocation. Callers can
// distinguish this from a cron parse failure via errors.Is.
var ErrInvalidTimeZone = errors.New("invalid timezone")

// ErrInvalidSchedule is returned by ParseSchedule when the cron expression
// (with any prepended CRON_TZ= prefix) cannot be parsed by cron.ParseStandard.
var ErrInvalidSchedule = errors.New("invalid schedule")

// ParseSchedule converts a ScheduledSparkApplication.Spec.Schedule and
// Spec.TimeZone into a cron.Schedule. The result is the parsed schedule used
// by the controller to compute next-run times; it is also the same parse the
// validating webhook performs at admission so that admission and reconciliation
// agree on which inputs are accepted.
//
// The construction mirrors the long-standing controller behaviour:
//
//   - An empty timezone is treated as "Local".
//   - A non-empty timezone is validated up front via time.LoadLocation so a
//     bogus IANA name surfaces a precise error rather than a downstream
//     cron parse error.
//   - If the schedule does not already carry an embedded "CRON_TZ=" or "TZ="
//     prefix, the timezone is woven into the schedule via a "CRON_TZ=<tz>"
//     prefix. Schedules that already specify their own embedded timezone are
//     passed through unchanged so the embedded value wins.
//
// On error, the returned cron.Schedule is nil and the error is annotated to
// distinguish a timezone failure from a schedule-parse failure.
func ParseSchedule(schedule, timezone string) (cron.Schedule, error) {
	if timezone == "" {
		timezone = "Local"
	} else if _, err := time.LoadLocation(timezone); err != nil {
		return nil, fmt.Errorf("%w %q: %v", ErrInvalidTimeZone, timezone, err)
	}

	cronSchedule := schedule
	if !strings.HasPrefix(cronSchedule, "CRON_TZ=") && !strings.HasPrefix(cronSchedule, "TZ=") {
		cronSchedule = fmt.Sprintf("CRON_TZ=%s %s", timezone, cronSchedule)
	}
	parsed, err := cron.ParseStandard(cronSchedule)
	if err != nil {
		return nil, fmt.Errorf("%w %q: %v", ErrInvalidSchedule, schedule, err)
	}
	return parsed, nil
}
