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

package sparkoperator

import (
	"fmt"
	"runtime"
)

type VersionInfo struct {
	Version      string
	BuildDate    string
	GitCommit    string
	GitTag       string
	GitTreeState string
	GoVersion    string
	Compiler     string
	Platform     string
}

var (
	version      = "0.0.0"                // value from VERSION file
	buildDate    = "1970-01-01T00:00:00Z" // output from `date -u +'%Y-%m-%dT%H:%M:%SZ'`
	gitCommit    = ""                     // output from `git rev-parse HEAD`
	gitTag       = ""                     // output from `git describe --exact-match --tags HEAD` (if clean tree state)
	gitTreeState = ""                     // determined from `git status --porcelain`. either 'clean' or 'dirty'
)

func getVersion() VersionInfo {
	var versionStr string
	if gitCommit != "" && gitTag != "" && gitTreeState == "clean" {
		// if we have a clean tree state and the current commit is tagged,
		// this is an official release.
		versionStr = gitTag
	} else {
		// otherwise formulate a query version string based on as much metadata
		// information we have available.
		versionStr = version
		if len(gitCommit) >= 7 {
			versionStr += "+" + gitCommit[0:7]
			if gitTreeState != "clean" {
				versionStr += ".dirty"
			}
		} else {
			versionStr += "+unknown"
		}
	}
	return VersionInfo{
		Version:      versionStr,
		BuildDate:    buildDate,
		GitCommit:    gitCommit,
		GitTag:       gitTag,
		GitTreeState: gitTreeState,
		GoVersion:    runtime.Version(),
		Compiler:     runtime.Compiler,
		Platform:     fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

// PrintVersion info directly by command
func PrintVersion(short bool) {
	v := getVersion()
	fmt.Printf("Spark Operator Version: %s\n", v.Version)
	if short {
		return
	}
	fmt.Printf("Build Date: %s\n", v.BuildDate)
	fmt.Printf("Git Commit ID: %s\n", v.GitCommit)
	if v.GitTag != "" {
		fmt.Printf("Git Tag: %s\n", v.GitTag)
	}
	fmt.Printf("Git Tree State: %s\n", v.GitTreeState)
	fmt.Printf("Go Version: %s\n", v.GoVersion)
	fmt.Printf("Compiler: %s\n", v.Compiler)
	fmt.Printf("Platform: %s\n", v.Platform)
}
