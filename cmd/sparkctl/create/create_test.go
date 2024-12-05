/*
Copyright 2018 Google LLC

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

package create

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandleHadoopConfiguration(t *testing.T) {
	configMap, err := buildHadoopConfigMap("test", "default", "testdata/hadoop-conf")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "test-hadoop-conf", configMap.Name)
	assert.Len(t, configMap.BinaryData, 1)
	assert.Len(t, configMap.Data, 1)
	assert.True(t, strings.Contains(configMap.Data["core-site.xml"], "fs.gs.impl"))
}
