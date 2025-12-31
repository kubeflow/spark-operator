/*
Copyright 2025 The Kubeflow authors.

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

package version

import (
	"fmt"

	"k8s.io/component-base/version"
)

// PrintVersion prints version information to stdout.
func PrintVersion(short bool) {
	v := version.Get()
	fmt.Printf("Git Version: %s\n", v.GitVersion)
	if short {
		return
	}
	fmt.Printf("Git Commit: %s\n", v.GitCommit)
	fmt.Printf("Git Tree State: %s\n", v.GitTreeState)
	fmt.Printf("Build Date: %s\n", v.BuildDate)
	fmt.Printf("Go Version: %s\n", v.GoVersion)
	fmt.Printf("Compiler: %s\n", v.Compiler)
	fmt.Printf("Platform: %s\n", v.Platform)
}
