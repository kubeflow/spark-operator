package sparkapplication

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	proto "github.com/kubeflow/spark-operator/v2/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const bufSize = 1024 * 1024

// mockServer implements the proto.SparkSubmitServiceServer interface
// and records the last request received.
type mockServer struct {
	proto.UnimplementedSparkSubmitServiceServer
	lastRequest *proto.RunAltSparkSubmitRequest
	response    *proto.RunAltSparkSubmitResponse
}

func (m *mockServer) RunAltSparkSubmit(ctx context.Context, req *proto.RunAltSparkSubmitRequest) (*proto.RunAltSparkSubmitResponse, error) {
	m.lastRequest = req
	return m.response, nil
}

func startMockGRPCServer(t *testing.T, response *proto.RunAltSparkSubmitResponse) (*grpc.ClientConn, *mockServer, func()) {
	listener := bufconn.Listen(bufSize)
	server := grpc.NewServer()
	mock := &mockServer{response: response}
	proto.RegisterSparkSubmitServiceServer(server, mock)
	go func() {
		_ = server.Serve(listener)
	}()
	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(dialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	cleanup := func() {
		server.Stop()
		conn.Close()
	}
	return conn, mock, cleanup
}

func TestGRPCSparkSubmitter_Submit_Success(t *testing.T) {
	conn, mock, cleanup := startMockGRPCServer(t, &proto.RunAltSparkSubmitResponse{Success: true})
	defer cleanup()

	submitter := &GRPCSparkSubmitter{
		serverAddress: "bufnet:12345", // Add a port to make it valid
		timeout:       2 * time.Second,
	}

	// Patch grpc.DialContext to use our bufconn
	oldDialContext := grpcDialContext
	grpcDialContext = func(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
		return conn, nil
	}
	defer func() { grpcDialContext = oldDialContext }()

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "default",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type: v1beta2.SparkApplicationTypeJava,
			Mode: v1beta2.DeployModeCluster,
		},
		Status: v1beta2.SparkApplicationStatus{
			SubmissionID: "sub-123",
		},
	}

	err := submitter.Submit(context.Background(), app)
	assert.NoError(t, err)
	assert.NotNil(t, mock.lastRequest)
	assert.Equal(t, "sub-123", mock.lastRequest.SubmissionId)
	assert.Equal(t, "test-app", mock.lastRequest.SparkApplication.Metadata.Name)
}
