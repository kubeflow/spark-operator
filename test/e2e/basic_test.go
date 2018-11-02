/*
Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func convertStdoutToString(stdout io.ReadCloser) string {
	buf := new(bytes.Buffer)
	buf.ReadFrom(stdout)
	return buf.String()
}

func runCmdAndReturnStdout(t *testing.T, cmd string, cmdArgs ...string) string {
	statusCmd := exec.Command(cmd, cmdArgs...)
	stdout, err := statusCmd.StdoutPipe()
	assert.Equal(t, err, nil)
	err = statusCmd.Start()
	assert.Equal(t, err, nil)
	return convertStdoutToString(stdout)
}

func getJobStatus(t *testing.T, statusStr string, result map[string]interface{}) string {
	json.Unmarshal([]byte(statusStr), &result)
	statusField := result["status"].(map[string]interface{})["applicationState"]
	errMsg := statusField.(map[string]interface{})["errorMessage"].(string)
	assert.Equal(t, errMsg, "")
	return statusField.(map[string]interface{})["state"].(string)
}

func TestSubmitSparkPiYaml(t *testing.T) {
	t.Parallel()

	// Wait for test job to finish. Time out after 90 seconds.
	timeout := 90

	yamlPath := "../../examples/spark-pi.yaml"
	submitResult := runCmdAndReturnStdout(t, "kubectl", "apply", "-f", yamlPath)
	assert.Equal(t, "sparkapplication.sparkoperator.k8s.io/spark-pi created\n", submitResult)

	statusStr := runCmdAndReturnStdout(t, "kubectl", "get", "sparkapplication", "spark-pi", "-o", "json")
	var result map[string]interface{}
	status := getJobStatus(t, statusStr, result)

	timePassed := 0
	// Update job status every 5 seconds until job is done or timeout threshold is reached.
	for status != "COMPLETED" && timePassed <= timeout {
		log.Print("Waiting for the Spark job to finish...")
		time.Sleep(5 * time.Second)
		timePassed += 5
		statusStr = runCmdAndReturnStdout(t, "kubectl", "get", "sparkapplication", "spark-pi", "-o", "json")
		status = getJobStatus(t, statusStr, result)
	}
	if timePassed > timeout {
		log.Fatalf("Time out waiting for Spark job to finish!")
	}

	json.Unmarshal([]byte(statusStr), &result)
	driverInfo := result["status"].(map[string]interface{})["driverInfo"]
	podName := driverInfo.(map[string]interface{})["podName"].(string)
	logStr := runCmdAndReturnStdout(t, "kubectl", "logs", podName)
	assert.NotEqual(t, -1, strings.Index(logStr, "Pi is roughly 3"))

	logStr = runCmdAndReturnStdout(t, "kubectl", "delete", "sparkapplication", "spark-pi")
	assert.Equal(t, "sparkapplication.sparkoperator.k8s.io \"spark-pi\" deleted\n", logStr)
}
