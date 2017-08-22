package config

// Package config contains code that deals with mounting Spark and Hadoop configurations
// into the driver and executor Pods as Kubernetes ConfigMaps. This package is used by
// both the cmd and initializer controller. The cmd uses this package to create ConfigMaps
// for Spark and Hadoop configurations from files in user-specified directories in the
// client machine. The initializer controller uses this package to mount the ConfigMaps to
// the driver and executor containers. The SparkApplication controller sets some annotation
// onto the driver and executor Pods so the initializer controller knows which ConfigMap(s)
// to use.
