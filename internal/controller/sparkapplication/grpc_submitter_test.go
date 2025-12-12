package sparkapplication

import (
	"context"
	"net"
	"testing"

	"github.com/kubeflow/spark-operator/v2/proto/sparksubmit"
	"google.golang.org/grpc"
)

// Fake server implementing proto service
type fakeSparkSubmitServer struct {
	sparksubmit.UnimplementedSparkSubmitServiceServer
	resp *sparksubmit.SubmitResponse
}

func (s *fakeSparkSubmitServer) Submit(ctx context.Context, req *sparksubmit.SubmitRequest) (*sparksubmit.SubmitResponse, error) {
	return s.resp, nil
}

// Standard Go test (NO GINKGO)
func TestGRPCSubmitter(t *testing.T) {

	// start local grpc server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer()
	fakeResp := &sparksubmit.SubmitResponse{
		Ok:      true,
		Message: "ok",
	}
	srvImpl := &fakeSparkSubmitServer{resp: fakeResp}
	sparksubmit.RegisterSparkSubmitServiceServer(srv, srvImpl)

	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	// dial client
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := sparksubmit.NewSparkSubmitServiceClient(conn)

	req := &sparksubmit.SubmitRequest{
		Name:            "test",
		ApplicationJson: "{}",
	}

	res, err := client.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	if !res.Ok {
		t.Fatalf("expected Ok=true, got false")
	}

	if res.Message != "ok" {
		t.Fatalf("expected message 'ok', got %q", res.Message)
	}
}
