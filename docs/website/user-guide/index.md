# User Guide

*In-depth guides for writing, configuring, scheduling, monitoring, and operating Spark applications with the Kubeflow Spark Operator.*

----

## Working with SparkApplications

:::::{grid} 1 1 2 2
:gutter: 3

::::{grid-item-card} Using SparkApplications
:link: using-sparkapplication
:link-type: doc

Create, list, check the status of, and delete `SparkApplication` objects
::::

::::{grid-item-card} Writing a SparkApplication
:link: writing-sparkapplication
:link-type: doc

The full anatomy of a `SparkApplication` spec — types, deps, pods, volumes, and more
::::

::::{grid-item-card} Working with SparkApplications
:link: working-with-sparkapplication
:link-type: doc

Restart policies, failure handling, and managing running applications
::::

::::{grid-item-card} Running on a Schedule
:link: running-sparkapplication-on-schedule
:link-type: doc

Use `ScheduledSparkApplication` to run Spark jobs on a cron schedule
::::

:::::

## Operating the Operator

:::::{grid} 1 1 2 2
:gutter: 3

::::{grid-item-card} Customizing Spark Operator
:link: customizing-spark-operator
:link-type: doc

Tune operator behavior, flags, and Helm chart values
::::

::::{grid-item-card} Enabling Leader Election
:link: leader-election
:link-type: doc

Run the operator in high-availability mode with leader election
::::

::::{grid-item-card} Running Multiple Instances
:link: running-multiple-instances-of-the-operator
:link-type: doc

Deploy several operator instances scoped to different namespaces
::::

::::{grid-item-card} Resource Quota Enforcement
:link: resource-quota-enforcement
:link-type: doc

Enforce Kubernetes resource quotas on Spark workloads
::::

:::::

## Monitoring & Scheduling

:::::{grid} 1 1 2 2
:gutter: 3

::::{grid-item-card} Monitoring with Prometheus & JMX
:link: monitoring-with-jmx-and-prometheus
:link-type: doc

Export Spark metrics to Prometheus using the JMX exporter
::::

::::{grid-item-card} Volcano Integration
:link: volcano-integration
:link-type: doc

Batch scheduling and gang scheduling with Volcano
::::

::::{grid-item-card} YuniKorn Integration
:link: yunikorn-integration
:link-type: doc

Resource-aware batch scheduling with Apache YuniKorn
::::

:::::

## Integrations

:::::{grid} 1 1 2 2
:gutter: 3

::::{grid-item-card} Google Cloud Storage & BigQuery
:link: gcp
:link-type: doc

Read and write data with GCS and BigQuery on GKE
::::

::::{grid-item-card} Kubeflow Notebooks
:link: notebooks-spark-operator
:link-type: doc

Run PySpark jobs from Kubeflow Notebooks
::::

:::::

----

```{toctree}
:hidden:
:maxdepth: 1

using-sparkapplication
writing-sparkapplication
working-with-sparkapplication
running-sparkapplication-on-schedule
customizing-spark-operator
leader-election
running-multiple-instances-of-the-operator
resource-quota-enforcement
monitoring-with-jmx-and-prometheus
volcano-integration
yunikorn-integration
gcp
notebooks-spark-operator
```
