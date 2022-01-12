package store

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
	"oras.land/oras-go/pkg/oras"
	"oras.land/oras-go/pkg/target"

	"github.com/rancherfederal/ocil/pkg/artifacts"
	"github.com/rancherfederal/ocil/pkg/consts"
	"github.com/rancherfederal/ocil/pkg/content"
	"github.com/rancherfederal/ocil/pkg/layer"
)

type Layout struct {
	Root  string
	store *content.OCI
	cache layer.Cache
}

type Options func(*Layout)

func WithCache(c layer.Cache) Options {
	return func(l *Layout) {
		l.cache = c
	}
}

func NewLayout(rootdir string, opts ...Options) (*Layout, error) {
	ociStore, err := content.NewOCI(rootdir)
	if err != nil {
		return nil, err
	}

	if err := ociStore.LoadIndex(); err != nil {
		return nil, err
	}

	l := &Layout{
		Root:  rootdir,
		store: ociStore,
	}

	for _, opt := range opts {
		opt(l)
	}

	return l, nil
}

// AddOCI adds an artifacts.OCI to the store
//  The method to achieve this is to save artifact.OCI to a temporary directory in an OCI layout compatible form.  Once
//  saved, the entirety of the layout is copied to the store (which is just a registry).  This allows us to not only use
//  strict types to define generic content, but provides a processing pipeline suitable for extensibility.  In the
//  future we'll allow users to define their own content that must adhere either by artifact.OCI or simply an OCI layout.
func (l *Layout) AddOCI(ctx context.Context, oci artifacts.OCI, ref string) (ocispec.Descriptor, error) {
	if l.cache != nil {
		cached := layer.OCICache(oci, l.cache)
		oci = cached
	}

	// Write manifest blob
	m, err := oci.Manifest()
	if err != nil {
		return ocispec.Descriptor{}, err
	}

	mdata, err := json.Marshal(m)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	if err := l.writeBytes(ctx, mdata); err != nil {
		return ocispec.Descriptor{}, err
	}

	// Write config blob
	cdata, err := oci.RawConfig()
	if err != nil {
		return ocispec.Descriptor{}, err
	}

	if err := l.writeBytes(ctx, cdata); err != nil {
		return ocispec.Descriptor{}, err
	}

	// write blob layers concurrently
	layers, err := oci.Layers()
	if err != nil {
		return ocispec.Descriptor{}, err
	}

	var g errgroup.Group
	for _, layer := range layers {
		layer := layer
		g.Go(func() error {
			h, err := layer.Digest()
			if err != nil {
				return err
			}

			w, err := l.writerAt(h.Algorithm, h.Hex)
			if err != nil {
				return err
			}
			defer w.Close()

			// Skip the layer if there's already something there
			// NOTE: We're implicitly relying on CAS without actually validating, might want to change this
			if s, _ := w.Stat(); s.Size() != 0 {
				return nil
			}

			rc, err := layer.Compressed()
			if err != nil {
				return err
			}

			if _, err := io.Copy(w, rc); err != nil {
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return ocispec.Descriptor{}, err
	}

	// Build index
	idx := ocispec.Descriptor{
		MediaType: string(m.MediaType),
		Digest:    digest.FromBytes(mdata),
		Size:      int64(len(mdata)),
		Annotations: map[string]string{
			ocispec.AnnotationRefName: ref,
		},
		URLs:     nil,
		Platform: nil,
	}

	return idx, l.store.AddIndex(idx)
}

// AddOCICollection .
func (l *Layout) AddOCICollection(ctx context.Context, collection artifacts.OCICollection) ([]ocispec.Descriptor, error) {
	cnts, err := collection.Contents()
	if err != nil {
		return nil, err
	}

	var descs []ocispec.Descriptor
	for ref, oci := range cnts {
		desc, err := l.AddOCI(ctx, oci, ref)
		if err != nil {
			return nil, err
		}
		descs = append(descs, desc)
	}
	return descs, nil
}

// Flush is a fancy name for delete-all-the-things, in this case it's as trivial as deleting oci-layout content
// 	This can be a highly destructive operation if the store's directory happens to be inline with other non-store contents
// 	To reduce the blast radius and likelihood of deleting things we don't own, Flush explicitly deletes oci-layout content only
func (l *Layout) Flush(ctx context.Context) error {
	blobs := filepath.Join(l.Root, "blobs")
	if err := os.RemoveAll(blobs); err != nil {
		return err
	}

	index := filepath.Join(l.Root, "index.json")
	if err := os.RemoveAll(index); err != nil {
		return err
	}

	layout := filepath.Join(l.Root, "oci-layout")
	if err := os.RemoveAll(layout); err != nil {
		return err
	}

	return nil
}

// Copy will copy a given reference to a given target.Target
// 		This is essentially a wrapper around oras.Copy, but locked to this content store
func (l *Layout) Copy(ctx context.Context, ref string, to target.Target, toRef string) (ocispec.Descriptor, error) {
	return oras.Copy(ctx, l.store, ref, to, toRef,
		oras.WithAdditionalCachedMediaTypes(consts.DockerManifestSchema2))
}

// CopyAll performs bulk copy operations on the stores oci layout to a provided target.Target
func (l *Layout) CopyAll(ctx context.Context, to target.Target, toMapper func(string) (string, error)) ([]ocispec.Descriptor, error) {
	var descs []ocispec.Descriptor
	err := l.store.Walk(func(reference string, desc ocispec.Descriptor) error {
		toRef := ""
		if toMapper != nil {
			tr, err := toMapper(reference)
			if err != nil {
				return err
			}
			toRef = tr
		}

		desc, err := l.Copy(ctx, reference, to, toRef)
		if err != nil {
			return err
		}

		descs = append(descs, desc)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return descs, nil
}

// Identify is a helper function that will identify a human-readable content type given a descriptor
func (l *Layout) Identify(ctx context.Context, desc ocispec.Descriptor) string {
	rc, err := l.store.Fetch(ctx, desc)
	if err != nil {
		return ""
	}
	defer rc.Close()

	m := struct {
		Config struct {
			MediaType string `json:"mediaType"`
		} `json:"config"`
	}{}
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return ""
	}

	return m.Config.MediaType
}

// NOTES: Should really just properly use oras to do this, but we'll be lazy and wait for oras v2

func (l *Layout) writerAt(alg string, hex string) (*os.File, error) {
	dir := filepath.Join(l.Root, "blobs", alg)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil && !os.IsExist(err) {
		return nil, err
	}

	blobPath := filepath.Join(dir, hex)

	// Skip entirely if something exists, assume layer is present already
	if _, err := os.Stat(blobPath); err == nil {
		return nil, nil
	}
	return os.Create(blobPath)
}

func (l *Layout) writeBytes(ctx context.Context, data []byte) error {
	d := digest.FromBytes(data)

	w, err := l.writerAt(d.Algorithm().String(), d.Hex())
	if err != nil {
		return err
	}
	defer w.Close()

	if _, err := w.Write(data); err != nil {
		return err
	}
	return nil
}
