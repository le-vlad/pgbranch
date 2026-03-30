package remote

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type mockGCSBucket struct {
	objects  map[string]*mockGCSObject
	listObjs []*storage.ObjectAttrs
	listErr  error
}

func (b *mockGCSBucket) Object(name string) gcsObjectAPI {
	if obj, ok := b.objects[name]; ok {
		return obj
	}
	return &mockGCSObject{err: storage.ErrObjectNotExist}
}

func (b *mockGCSBucket) Objects(ctx context.Context, q *storage.Query) gcsObjectIteratorAPI {
	return &mockGCSIterator{items: b.listObjs, err: b.listErr}
}

type mockWriteCloser struct {
	buf      *bytes.Buffer
	closeErr error
}

func (w *mockWriteCloser) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *mockWriteCloser) Close() error {
	return w.closeErr
}

type mockGCSObject struct {
	data    []byte
	attrs   *storage.ObjectAttrs
	err     error
	written *bytes.Buffer
	deleted bool

	writeCloseErr error
}

func (o *mockGCSObject) NewWriter(ctx context.Context) io.WriteCloser {
	if o.written == nil {
		o.written = &bytes.Buffer{}
	}
	return &mockWriteCloser{buf: o.written, closeErr: o.writeCloseErr}
}

func (o *mockGCSObject) NewReader(ctx context.Context) (io.ReadCloser, error) {
	if o.err != nil {
		return nil, o.err
	}
	return io.NopCloser(bytes.NewReader(o.data)), nil
}

func (o *mockGCSObject) Attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	if o.err != nil {
		return nil, o.err
	}
	return o.attrs, nil
}

func (o *mockGCSObject) Delete(ctx context.Context) error {
	if o.err != nil {
		return o.err
	}
	o.deleted = true
	return nil
}

type mockGCSIterator struct {
	items []*storage.ObjectAttrs
	index int
	err   error
}

func (it *mockGCSIterator) Next() (*storage.ObjectAttrs, error) {
	if it.err != nil {
		return nil, it.err
	}
	if it.index >= len(it.items) {
		return nil, iterator.Done
	}
	obj := it.items[it.index]
	it.index++
	return obj, nil
}

func TestGCSRemote_ObjectKey(t *testing.T) {
	r := &GCSRemote{name: "test", prefix: "", client: &mockGCSBucket{}}
	got := r.objectKey("feature/branch")
	want := "feature_branch.pgbranch"
	if got != want {
		t.Errorf("objectKey() without prefix = %q, want %q", got, want)
	}

	r.prefix = "backups/pg"
	got = r.objectKey("main")
	want = "backups/pg/main.pgbranch"
	if got != want {
		t.Errorf("objectKey() with prefix = %q, want %q", got, want)
	}
}

func TestGCSRemote_Push_Success(t *testing.T) {
	obj := &mockGCSObject{written: &bytes.Buffer{}}
	bucket := &mockGCSBucket{
		objects: map[string]*mockGCSObject{"main.pgbranch": obj},
	}
	r := &GCSRemote{name: "test", client: bucket}

	data := []byte("snapshot-contents")
	err := r.Push(context.Background(), "main", bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("Push() error: %v", err)
	}
	if !bytes.Equal(obj.written.Bytes(), data) {
		t.Errorf("written data = %q, want %q", obj.written.Bytes(), data)
	}
}

func TestGCSRemote_Push_WriteError(t *testing.T) {
	obj := &mockGCSObject{
		written:       &bytes.Buffer{},
		writeCloseErr: errors.New("upload failed"),
	}
	bucket := &mockGCSBucket{
		objects: map[string]*mockGCSObject{"main.pgbranch": obj},
	}
	r := &GCSRemote{name: "test", client: bucket}

	data := []byte("snapshot-contents")
	err := r.Push(context.Background(), "main", bytes.NewReader(data), int64(len(data)))
	if err == nil {
		t.Fatalf("Push() expected error, got nil")
	}
}

func TestGCSRemote_Pull_Success(t *testing.T) {
	data := []byte("pulled-data")
	obj := &mockGCSObject{
		data:  data,
		attrs: &storage.ObjectAttrs{Size: int64(len(data))},
	}
	bucket := &mockGCSBucket{
		objects: map[string]*mockGCSObject{"main.pgbranch": obj},
	}
	r := &GCSRemote{name: "test", client: bucket}

	rc, size, err := r.Pull(context.Background(), "main")
	if err != nil {
		t.Fatalf("Pull() error: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Pull data = %q, want %q", got, data)
	}
	if size != int64(len(data)) {
		t.Errorf("Pull size = %d, want %d", size, len(data))
	}
}

func TestGCSRemote_Pull_AttrsError(t *testing.T) {
	obj := &mockGCSObject{err: errors.New("attrs failure")}
	bucket := &mockGCSBucket{
		objects: map[string]*mockGCSObject{"main.pgbranch": obj},
	}
	r := &GCSRemote{name: "test", client: bucket}

	_, _, err := r.Pull(context.Background(), "main")
	if err == nil {
		t.Fatalf("Pull() expected error, got nil")
	}
}

func TestGCSRemote_List_WithObjects(t *testing.T) {
	now := time.Now()
	bucket := &mockGCSBucket{
		listObjs: []*storage.ObjectAttrs{
			{Name: "main.pgbranch", Size: 100, Updated: now},
			{Name: "readme.txt", Size: 50, Updated: now},
			{Name: "dev.pgbranch", Size: 200, Updated: now},
		},
	}
	r := &GCSRemote{name: "test", client: bucket}

	branches, err := r.List(context.Background())
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(branches) != 2 {
		t.Fatalf("List() returned %d branches, want 2", len(branches))
	}
	if branches[0].Name != "main" {
		t.Errorf("branches[0].Name = %q, want %q", branches[0].Name, "main")
	}
	if branches[0].Size != 100 {
		t.Errorf("branches[0].Size = %d, want 100", branches[0].Size)
	}
	if branches[1].Name != "dev" {
		t.Errorf("branches[1].Name = %q, want %q", branches[1].Name, "dev")
	}
	if branches[1].Size != 200 {
		t.Errorf("branches[1].Size = %d, want 200", branches[1].Size)
	}
}

func TestGCSRemote_List_Empty(t *testing.T) {
	bucket := &mockGCSBucket{}
	r := &GCSRemote{name: "test", client: bucket}

	branches, err := r.List(context.Background())
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(branches) != 0 {
		t.Errorf("List() returned %d branches, want 0", len(branches))
	}
}

func TestGCSRemote_List_Error(t *testing.T) {
	bucket := &mockGCSBucket{listErr: errors.New("iteration failed")}
	r := &GCSRemote{name: "test", client: bucket}

	_, err := r.List(context.Background())
	if err == nil {
		t.Fatalf("List() expected error, got nil")
	}
}

func TestGCSRemote_Delete_Success(t *testing.T) {
	obj := &mockGCSObject{}
	bucket := &mockGCSBucket{
		objects: map[string]*mockGCSObject{"main.pgbranch": obj},
	}
	r := &GCSRemote{name: "test", client: bucket}

	err := r.Delete(context.Background(), "main")
	if err != nil {
		t.Fatalf("Delete() error: %v", err)
	}
	if !obj.deleted {
		t.Errorf("expected object to be deleted")
	}
}

func TestGCSRemote_Delete_Error(t *testing.T) {
	obj := &mockGCSObject{err: errors.New("delete failed")}
	bucket := &mockGCSBucket{
		objects: map[string]*mockGCSObject{"main.pgbranch": obj},
	}
	r := &GCSRemote{name: "test", client: bucket}

	err := r.Delete(context.Background(), "main")
	if err == nil {
		t.Fatalf("Delete() expected error, got nil")
	}
}

func TestGCSRemote_Exists_True(t *testing.T) {
	obj := &mockGCSObject{
		attrs: &storage.ObjectAttrs{Size: 42},
	}
	bucket := &mockGCSBucket{
		objects: map[string]*mockGCSObject{"main.pgbranch": obj},
	}
	r := &GCSRemote{name: "test", client: bucket}

	exists, err := r.Exists(context.Background(), "main")
	if err != nil {
		t.Fatalf("Exists() error: %v", err)
	}
	if !exists {
		t.Errorf("Exists() = false, want true")
	}
}

func TestGCSRemote_Exists_False(t *testing.T) {
	bucket := &mockGCSBucket{
		objects: map[string]*mockGCSObject{},
	}
	r := &GCSRemote{name: "test", client: bucket}

	exists, err := r.Exists(context.Background(), "main")
	if err != nil {
		t.Fatalf("Exists() error: %v", err)
	}
	if exists {
		t.Errorf("Exists() = true, want false")
	}
}

func TestGCSRemote_Exists_OtherError(t *testing.T) {
	obj := &mockGCSObject{err: errors.New("network error")}
	bucket := &mockGCSBucket{
		objects: map[string]*mockGCSObject{"main.pgbranch": obj},
	}
	r := &GCSRemote{name: "test", client: bucket}

	exists, err := r.Exists(context.Background(), "main")
	if err == nil {
		t.Fatalf("Exists() expected error, got nil")
	}
	if exists {
		t.Errorf("Exists() = true, want false on error")
	}
}
