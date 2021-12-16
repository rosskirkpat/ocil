package store

import (
	"context"
	"os"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/random"

	"github.com/rancherfederal/oci-artifacts/pkg/artifact"
)

var (
	ctx  context.Context
	root string
)

func TestNewOCI(t *testing.T) {
	teardown := setup(t)
	defer teardown()

	type args struct {
		ref string
	}
	tests := []struct {
		name    string
		args    args
		want    *OCI
		wantErr bool
	}{
		{
			name: "",
			args: args{
				ref: "hello/world:v1",
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewOCI(root)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewOCI() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			moci := genArtifact(t, tt.args.ref)

			if _, err := s.Add(ctx, moci, tt.args.ref); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func setup(t *testing.T) func() error {
	tmpdir, err := os.MkdirTemp("", "oci-artifacts")
	if err != nil {
		t.Fatal(err)
	}
	root = tmpdir

	ctx = context.Background()

	return func() error {
		os.RemoveAll(tmpdir)
		return nil
	}
}

type mockArtifact struct {
	v1.Image
}

func (m mockArtifact) MediaType() string {
	mt, err := m.Image.MediaType()
	if err != nil {
		return ""
	}
	return string(mt)
}

func (m mockArtifact) RawConfig() ([]byte, error) {
	return m.RawConfigFile()
}

func genArtifact(t *testing.T, ref string) artifact.OCI {
	img, err := random.Image(1024, 3)
	if err != nil {
		t.Fatal(err)
	}

	return &mockArtifact{
		img,
	}
}
