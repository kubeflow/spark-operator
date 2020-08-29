# Troubleshooting

This file is meant to be consumed as a FAQ.

## Why am I unable to provide a custom `spark.properties` via `sparkConfigMap` in the CRD Spec?
Have a look at `addSparkConfigMap(...)` in `patch.go`. We use SubPath mounts to mount multiple files to the same folder
(`SPARK_CONF_DIR`). Spark itself uses a ConfigMap to mount its auto-generated `spark.properties` to the driver pods 
container. We simply re-mount the VolumeMount as SubPath. There exists no agreed/upstream-compatible solution to provide
your own `spark.properties` because it would override the auto-generated spark defaults.
