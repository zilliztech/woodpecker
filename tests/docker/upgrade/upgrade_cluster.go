// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package upgrade

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/zilliztech/woodpecker/tests/docker/framework"
)

// UpgradeCluster manages a Docker Compose-based Woodpecker cluster for upgrade
// testing. It embeds the shared framework.DockerCluster and adds upgrade-specific
// configuration. The image-swap operation itself lives in the framework as
// DockerCluster.RecreateAllNodes (it needs the unexported composeArgs), so this
// type is a thin wrapper that only supplies suite-specific paths/config.
type UpgradeCluster struct {
	*framework.DockerCluster
}

// upgradeDir returns the absolute path to the upgrade suite directory, derived
// from this source file's location so it works regardless of the test binary's
// working directory.
func upgradeDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Dir(thisFile)
}

// NewUpgradeCluster creates an UpgradeCluster with upgrade test configuration.
func NewUpgradeCluster(t *testing.T) *UpgradeCluster {
	t.Helper()
	base := framework.NewDockerCluster(t, framework.ClusterConfig{
		TestDir:      upgradeDir(),
		ProjectName:  "woodpecker-upgrade",
		OverrideFile: "docker-compose.upgrade.yaml",
		NetworkName:  "woodpecker-upgrade_woodpecker",
		GossipWait:   10 * time.Second,
	})
	return &UpgradeCluster{DockerCluster: base}
}
