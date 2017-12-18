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

package initializer

// Package initializer implements the initializer for Spark pods. It looks for certain annotations on
// the Spark driver and executor pods and modifies the pod spec based on the annotations. For example,
// it is able to inject user-specified secrets and ConfigMaps into the Spark pods. It can also set
// user-defined environment variables. It removes its name from the list of pending initializers from
// pods it has processed.
