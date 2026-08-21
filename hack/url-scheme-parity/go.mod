// Standalone module: isolates this verification harness from the operator module
// so `go build ./...` / `go vet ./...` at the repo root never pull it in.
module urlschemeparity

go 1.25
