/*
Copyright 2024 The Kubeflow authors.

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

package common

// Spark application metric names.
const (
	MetricSparkApplicationCount = "spark_application_count"

	MetricSparkApplicationSubmitCount = "spark_application_submit_count"

	MetricSparkApplicationFailedSubmissionCount = "spark_application_failed_submission_count"

	MetricSparkApplicationRunningCount = "spark_application_running_count"

	MetricSparkApplicationSuccessCount = "spark_application_success_count"

	MetricSparkApplicationFailureCount = "spark_application_failure_count"

	MetricSparkApplicationSuccessExecutionTimeSeconds = "spark_application_success_execution_time_seconds"

	MetricSparkApplicationFailureExecutionTimeSeconds = "spark_application_failure_execution_time_seconds"

	MetricSparkApplicationStartLatencySeconds = "spark_application_start_latency_seconds"

	MetricSparkApplicationStartLatencySecondsHistogram = "spark_application_start_latency_seconds_histogram"
)

// Spark executor metric names.
const (
	MetricSparkExecutorRunningCount = "spark_executor_running_count"

	MetricSparkExecutorSuccessCount = "spark_executor_success_count"

	MetricSparkExecutorFailureCount = "spark_executor_failure_count"
)
