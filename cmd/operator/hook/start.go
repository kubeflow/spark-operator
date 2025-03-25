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

package hook

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logzap "sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/kubeflow/spark-operator/pkg/util"
)

var (
	scheme = runtime.NewScheme()
	logger = ctrl.Log.WithName("")
)

var (
	upgradeCrds bool
	crdsPath    string

	development bool
	zapOptions  = logzap.Options{}
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(apiextensionsv1.AddToScheme(scheme))
}

func NewStartCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start hook",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if crdsPath == "" {
				return fmt.Errorf("--crds-path is required")
			}
			return nil
		},
		RunE: func(_ *cobra.Command, args []string) error {
			return start()
		},
	}

	cmd.Flags().BoolVar(&upgradeCrds, "upgrade-crds", false, "Upgrade SparkApplication and ScheduledSparkApplication CRDs")
	cmd.Flags().StringVar(&crdsPath, "crds-path", "", "Path to the CRDs directory")

	flagSet := flag.NewFlagSet("hook", flag.ExitOnError)
	ctrl.RegisterFlags(flagSet)
	zapOptions.BindFlags(flagSet)
	cmd.Flags().AddGoFlagSet(flagSet)

	return cmd
}

func start() error {
	setupLog()

	if upgradeCrds {
		if err := upgradeCRDs(); err != nil {
			return fmt.Errorf("failed to upgrade CRDs: %v", err)
		}
	}

	return nil
}

func upgradeCRDs() error {
	// Create the client rest config. Use kubeConfig if given, otherwise assume in-cluster.
	cfg, err := ctrl.GetConfig()
	if err != nil {
		logger.Error(err, "failed to get kube config")
		os.Exit(1)
	}

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("failed to create k8s client: %v", err)
	}

	// Find all CRD files.
	crdFiles := []string{}
	if err := filepath.Walk(crdsPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".yaml" {
			crdFiles = append(crdFiles, path)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to walk crds path: %v", err)
	}

	// Loop through each CRD file and update the CRD if it has changed.
	for _, crdFile := range crdFiles {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := util.ReadObjectFromFile(crd, crdFile); err != nil {
			logger.Error(err, "Failed to read CRD from file", "file", crdFile)
			continue
		}
		apiextensionsv1.SetDefaults_CustomResourceDefinition(crd)

		key := client.ObjectKey{Name: crd.Name}
		oldCrd := &apiextensionsv1.CustomResourceDefinition{}
		if err := k8sClient.Get(context.TODO(), key, oldCrd); err != nil {
			logger.Error(err, "Failed to get CRD", "name", oldCrd.Name)
			continue
		}

		if equality.Semantic.DeepEqual(oldCrd.Spec, crd.Spec) {
			logger.Info("Skip updating CRD as its specification does not change", "name", crd.Name)
			continue
		}

		newCrd := oldCrd.DeepCopy()
		newCrd.Spec = crd.Spec
		if err := k8sClient.Update(context.TODO(), newCrd); err != nil {
			logger.Error(err, "Failed to update CRD", "name", crd.Name)
			continue
		}
		logger.Info("Updated CRD", "name", crd.Name)
	}

	return nil
}

// setupLog Configures the logging system
func setupLog() {
	ctrl.SetLogger(logzap.New(
		logzap.UseFlagOptions(&zapOptions),
		func(o *logzap.Options) {
			o.Development = development
		}, func(o *logzap.Options) {
			o.ZapOpts = append(o.ZapOpts, zap.AddCaller())
		}, func(o *logzap.Options) {
			var config zapcore.EncoderConfig
			if !development {
				config = zap.NewProductionEncoderConfig()
			} else {
				config = zap.NewDevelopmentEncoderConfig()
			}
			config.EncodeLevel = zapcore.CapitalColorLevelEncoder
			config.EncodeTime = zapcore.ISO8601TimeEncoder
			config.EncodeCaller = zapcore.ShortCallerEncoder
			o.Encoder = zapcore.NewConsoleEncoder(config)
		}),
	)
}
