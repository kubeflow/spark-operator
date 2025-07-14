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

package sparkapplication

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"sigs.k8s.io/controller-runtime/pkg/log"

	proto "github.com/kubeflow/spark-operator/v2/proto"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// Patch point for grpc.DialContext (for testing)
var grpcDialContext = grpc.DialContext

// GRPCSparkSubmitter submits a SparkApplication using gRPC.
type GRPCSparkSubmitter struct {
	serverAddress string
	timeout       time.Duration
}

// NewGRPCSparkSubmitter creates a new GRPCSparkSubmitter instance.
func NewGRPCSparkSubmitter(serverAddress string, timeout time.Duration) *GRPCSparkSubmitter {
	return &GRPCSparkSubmitter{
		serverAddress: serverAddress,
		timeout:       timeout,
	}
}

// GRPCSparkSubmitter implements SparkApplicationSubmitter interface.
var _ SparkApplicationSubmitter = &GRPCSparkSubmitter{}

// Submit implements SparkApplicationSubmitter interface.
func (g *GRPCSparkSubmitter) Submit(ctx context.Context, app *v1beta2.SparkApplication) error {
	logger := log.FromContext(ctx)

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	// Connect to the gRPC server
	conn, err := grpcDialContext(ctx, g.serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to gRPC server at %s: %v", g.serverAddress, err)
	}
	defer conn.Close()

	// Create gRPC client
	client := proto.NewSparkSubmitServiceClient(conn)

	// Convert v1beta2.SparkApplication to protobuf SparkApplication
	protoApp, err := g.convertToProto(app)
	if err != nil {
		return fmt.Errorf("failed to convert SparkApplication to protobuf: %v", err)
	}

	// Create the request
	request := &proto.RunAltSparkSubmitRequest{
		SparkApplication: protoApp,
		SubmissionId:     app.Status.SubmissionID,
	}

	// Call the gRPC service
	logger.Info("Submitting SparkApplication via gRPC", "server", g.serverAddress, "submissionId", app.Status.SubmissionID)
	response, err := client.RunAltSparkSubmit(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to submit SparkApplication via gRPC: %v", err)
	}

	// Check the response
	if !response.Success {
		return fmt.Errorf("gRPC submission failed: %s", response.ErrorMessage)
	}

	logger.Info("Successfully submitted SparkApplication via gRPC", "submissionId", app.Status.SubmissionID)
	return nil
}

// convertToProto converts a v1beta2.SparkApplication to a protobuf SparkApplication.
func (g *GRPCSparkSubmitter) convertToProto(app *v1beta2.SparkApplication) (*proto.SparkApplication, error) {
	// Convert metadata
	metadata := &proto.ObjectMeta{
		Name:        app.Name,
		Namespace:   app.Namespace,
		Labels:      app.Labels,
		Annotations: app.Annotations,
	}

	// Convert spec
	spec, err := g.convertSpecToProto(&app.Spec)
	if err != nil {
		return nil, fmt.Errorf("failed to convert spec: %v", err)
	}

	// Convert status
	status := &proto.SparkApplicationStatus{
		ApplicationState:   string(app.Status.AppState.State),
		SparkApplicationId: app.Status.SparkApplicationID,
		SubmissionId:       app.Status.SubmissionID,
	}

	return &proto.SparkApplication{
		Metadata: metadata,
		Spec:     spec,
		Status:   status,
	}, nil
}

// convertSpecToProto converts a v1beta2.SparkApplicationSpec to a protobuf SparkApplicationSpec.
func (g *GRPCSparkSubmitter) convertSpecToProto(spec *v1beta2.SparkApplicationSpec) (*proto.SparkApplicationSpec, error) {
	protoSpec := &proto.SparkApplicationSpec{
		Type:         g.convertApplicationType(spec.Type),
		Mode:         g.convertDeployMode(spec.Mode),
		SparkConf:    spec.SparkConf,
		HadoopConf:   spec.HadoopConf,
		Arguments:    spec.Arguments,
		SparkVersion: spec.SparkVersion,
		PythonVersion: func() string {
			if spec.PythonVersion != nil {
				return *spec.PythonVersion
			}
			return ""
		}(),
	}

	// Convert optional string fields
	if spec.Image != nil {
		protoSpec.Image = wrapperspb.String(*spec.Image)
	}
	if spec.ImagePullPolicy != nil {
		protoSpec.ImagePullPolicy = wrapperspb.String(*spec.ImagePullPolicy)
	}
	if spec.SparkConfigMap != nil {
		protoSpec.SparkConfigMap = wrapperspb.String(*spec.SparkConfigMap)
	}
	if spec.HadoopConfigMap != nil {
		protoSpec.HadoopConfigMap = wrapperspb.String(*spec.HadoopConfigMap)
	}
	if spec.MainClass != nil {
		protoSpec.MainClass = wrapperspb.String(*spec.MainClass)
	}
	if spec.MainApplicationFile != nil {
		protoSpec.MainApplicationFile = wrapperspb.String(*spec.MainApplicationFile)
	}
	if spec.ProxyUser != nil {
		protoSpec.ProxyUser = wrapperspb.String(*spec.ProxyUser)
	}
	if spec.MemoryOverheadFactor != nil {
		protoSpec.MemoryOverheadFactor = wrapperspb.String(*spec.MemoryOverheadFactor)
	}
	if spec.BatchScheduler != nil {
		protoSpec.BatchScheduler = wrapperspb.String(*spec.BatchScheduler)
	}
	if spec.TimeToLiveSeconds != nil {
		protoSpec.TimeToLiveSeconds = wrapperspb.Int64(*spec.TimeToLiveSeconds)
	}
	if spec.FailureRetries != nil {
		protoSpec.FailureRetries = wrapperspb.Int32(*spec.FailureRetries)
	}
	if spec.RetryInterval != nil {
		protoSpec.RetryInterval = wrapperspb.Int64(*spec.RetryInterval)
	}

	// Convert image pull secrets
	if len(spec.ImagePullSecrets) > 0 {
		protoSpec.ImagePullSecrets = spec.ImagePullSecrets
	}

	// Convert dependencies
	if spec.Deps.Jars != nil || spec.Deps.Files != nil || spec.Deps.PyFiles != nil ||
		spec.Deps.Packages != nil || spec.Deps.ExcludePackages != nil ||
		spec.Deps.Repositories != nil || spec.Deps.Archives != nil {
		protoSpec.Deps = &proto.Dependencies{
			Jars:            spec.Deps.Jars,
			Files:           spec.Deps.Files,
			PyFiles:         spec.Deps.PyFiles,
			Packages:        spec.Deps.Packages,
			ExcludePackages: spec.Deps.ExcludePackages,
			Repositories:    spec.Deps.Repositories,
			Archives:        spec.Deps.Archives,
		}
	}

	// Convert dynamic allocation
	if spec.DynamicAllocation != nil {
		protoSpec.DynamicAllocation = &proto.DynamicAllocation{
			Enabled:          spec.DynamicAllocation.Enabled,
			InitialExecutors: int32(0),
			MinExecutors:     int32(0),
			MaxExecutors:     int32(0),
			ShuffleTrackingTimeout: func() int64 {
				if spec.DynamicAllocation.ShuffleTrackingTimeout != nil {
					return *spec.DynamicAllocation.ShuffleTrackingTimeout
				}
				return 0
			}(),
		}
		if spec.DynamicAllocation.InitialExecutors != nil {
			protoSpec.DynamicAllocation.InitialExecutors = *spec.DynamicAllocation.InitialExecutors
		}
		if spec.DynamicAllocation.MinExecutors != nil {
			protoSpec.DynamicAllocation.MinExecutors = *spec.DynamicAllocation.MinExecutors
		}
		if spec.DynamicAllocation.MaxExecutors != nil {
			protoSpec.DynamicAllocation.MaxExecutors = *spec.DynamicAllocation.MaxExecutors
		}
	}

	// Convert restart policy
	if spec.RestartPolicy.Type != "" {
		protoSpec.RestartPolicy = &proto.RestartPolicy{
			Type: string(spec.RestartPolicy.Type),
		}
	}

	// Convert driver spec
	if spec.Driver.Cores != nil || spec.Driver.Memory != nil || spec.Driver.ServiceAccount != nil {
		protoSpec.Driver = &proto.DriverSpec{}

		if spec.Driver.Cores != nil {
			protoSpec.Driver.SparkPodSpec = &proto.SparkPodSpec{
				Cores: wrapperspb.Int32(*spec.Driver.Cores),
			}
		}
		if spec.Driver.Memory != nil {
			if protoSpec.Driver.SparkPodSpec == nil {
				protoSpec.Driver.SparkPodSpec = &proto.SparkPodSpec{}
			}
			protoSpec.Driver.SparkPodSpec.Memory = *spec.Driver.Memory
		}
		if spec.Driver.ServiceAccount != nil {
			if protoSpec.Driver.SparkPodSpec == nil {
				protoSpec.Driver.SparkPodSpec = &proto.SparkPodSpec{}
			}
			protoSpec.Driver.SparkPodSpec.ServiceAccount = wrapperspb.String(*spec.Driver.ServiceAccount)
		}
	}

	// Convert executor spec
	if spec.Executor.Instances != nil || spec.Executor.Cores != nil || spec.Executor.Memory != nil {
		protoSpec.Executor = &proto.ExecutorSpec{}

		if spec.Executor.Instances != nil {
			protoSpec.Executor.Instances = wrapperspb.Int32(*spec.Executor.Instances)
		}
		if spec.Executor.Cores != nil || spec.Executor.Memory != nil {
			protoSpec.Executor.SparkPodSpec = &proto.SparkPodSpec{}
			if spec.Executor.Cores != nil {
				protoSpec.Executor.SparkPodSpec.Cores = wrapperspb.Int32(*spec.Executor.Cores)
			}
			if spec.Executor.Memory != nil {
				protoSpec.Executor.SparkPodSpec.Memory = *spec.Executor.Memory
			}
		}
	}

	return protoSpec, nil
}

// convertApplicationType converts v1beta2.SparkApplicationType to protobuf SparkApplicationType.
func (g *GRPCSparkSubmitter) convertApplicationType(appType v1beta2.SparkApplicationType) proto.SparkApplicationType {
	switch appType {
	case v1beta2.SparkApplicationTypeJava:
		return proto.SparkApplicationType_SPARK_APPLICATION_TYPE_JAVA
	case v1beta2.SparkApplicationTypeScala:
		return proto.SparkApplicationType_SPARK_APPLICATION_TYPE_SCALA
	case v1beta2.SparkApplicationTypePython:
		return proto.SparkApplicationType_SPARK_APPLICATION_TYPE_PYTHON
	case v1beta2.SparkApplicationTypeR:
		return proto.SparkApplicationType_SPARK_APPLICATION_TYPE_R
	default:
		return proto.SparkApplicationType_SPARK_APPLICATION_TYPE_UNSPECIFIED
	}
}

// convertDeployMode converts v1beta2.DeployMode to protobuf DeployMode.
func (g *GRPCSparkSubmitter) convertDeployMode(mode v1beta2.DeployMode) proto.DeployMode {
	switch mode {
	case v1beta2.DeployModeCluster:
		return proto.DeployMode_DEPLOY_MODE_CLUSTER
	case v1beta2.DeployModeClient:
		return proto.DeployMode_DEPLOY_MODE_CLIENT
	case v1beta2.DeployModeInClusterClient:
		return proto.DeployMode_DEPLOY_MODE_IN_CLUSTER_CLIENT
	default:
		return proto.DeployMode_DEPLOY_MODE_UNSPECIFIED
	}
}
