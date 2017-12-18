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

package config

// Package config contains code that deals with mounting Spark and Hadoop configurations
// into the driver and executor Pods as Kubernetes ConfigMaps as well as mounting general
// ConfigMaps specified SparkApplicationSpec. This package is used by both the cmd and
// initializer controller. The cmd uses this package to create ConfigMaps for Spark and
// Hadoop configurations from files in user-specified directories in the client machine.
// The initializer controller uses this package to mount the ConfigMaps to the driver and
// executor containers. The SparkApplication controller sets some annotation onto the
// driver and executor Pods so the initializer controller knows which ConfigMap(s) to use.
// This package is also the place where all custom annotations and labels like the ones
// added for the initializer are defined.
