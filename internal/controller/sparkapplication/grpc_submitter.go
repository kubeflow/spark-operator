package sparkapplication

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/proto/sparksubmit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCSubmitter implements SparkApplicationSubmitter using a gRPC service.
type GRPCSubmitter struct {
	endpoint    string
	dialOpts    []grpc.DialOption
	dialTimeout time.Duration
}

// NewGRPCSubmitter creates a new GRPCSubmitter.
// If insecureConn is true, plaintext connection is used. Otherwise TLS is used (system roots).
func NewGRPCSubmitter(endpoint string, insecureConn bool, dialTimeout time.Duration) *GRPCSubmitter {
	var dialOpts []grpc.DialOption
	if insecureConn {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		// Use system root CA - this requires that the default TLS setup is ok for your cluster.
		creds := credentials.NewClientTLSFromCert(nil, "")
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	}
	// Add WithBlock? We will handle dial context timeout when dialing.
	return &GRPCSubmitter{
		endpoint:    endpoint,
		dialOpts:    dialOpts,
		dialTimeout: dialTimeout,
	}
}

// Submit sends the SparkApplication over gRPC.
func (s *GRPCSubmitter) Submit(ctx context.Context, app *v1beta2.SparkApplication) error {
	// serialize to JSON for portability
	j, err := json.Marshal(app)
	if err != nil {
		return fmt.Errorf("failed to marshal SparkApplication to JSON: %w", err)
	}

	ctxDial, cancel := context.WithTimeout(ctx, s.dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctxDial, s.endpoint, s.dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to dial gRPC endpoint %s: %w", s.endpoint, err)
	}
	defer conn.Close()

	client := sparksubmit.NewSparkSubmitServiceClient(conn)

	// Provide reasonable submit timeout
	ctxSubmit, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	defer cancel2()

	req := &sparksubmit.SubmitRequest{
		Name:            app.Name,
		Namespace:       app.Namespace,
		ApplicationJson: string(j),
	}

	resp, err := client.Submit(ctxSubmit, req)
	if err != nil {
		return fmt.Errorf("grpc submit error: %w", err)
	}
	if !resp.Ok {
		return fmt.Errorf("submission failed: %s", resp.Message)
	}
	return nil
}
