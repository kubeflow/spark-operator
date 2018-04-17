/*
Copyright 2017 Google LLC

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

package controller

// Package controller implements the CustomResourceDefinition (CRD) controller for SparkApplications and
// ScheduledSparkApplications.
//
// The ScheduledSparkApplication controller is responsible for watching ScheduledSparkApplications objects
// and scheduling them according to the cron schedule in the ScheduledSparkApplication specification. For
// each ScheduledSparkApplication, the controller creates a new SparkApplication instance when the next run
// of the application is due and the condition for starting the next run is satisfied.
//
// The SparkApplication controller is responsible for watching SparkApplication objects and submitting
// Spark applications described by the specs in the objects on behalf of users. After an application is
// submitted, the controller monitors the application state and updates the status field of the
// SparkApplication object accordingly. The controller uses a sparkSubmitRunner to submit applications
// to run in the Kubernetes cluster where Spark Operator runs. The sparkSubmitRunner maintains a set of
// workers, each of which is a goroutine, for actually running the spark-submit commands. The controller
// also uses a sparkPodMonitor to watch Spark driver and executor pods. The sparkPodMonitor sends driver
// and executor state updates to the controller, which then updates status field of SparkApplication
// objects accordingly.
