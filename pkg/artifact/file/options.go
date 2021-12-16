package file

import (
	"github.com/rancherfederal/oci-artifacts/pkg/artifact"
	"github.com/rancherfederal/oci-artifacts/pkg/artifact/file/getter"
)

type Option func(*File)

func WithClient(c *getter.Client) Option {
	return func(f *File) {
		f.client = c
	}
}

func WithConfig(obj interface{}, mediaType string) Option {
	return func(f *File) {
		f.config = artifact.ToConfig(obj, artifact.WithConfigMediaType(mediaType))
	}
}

func WithAnnotations(m map[string]string) Option {
	return func(f *File) {
		f.annotations = m
	}
}
