package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestRangeHeader(t *testing.T) {
	h, err := rangeHeader(10, 5)
	if err != nil || h != "bytes=10-14" {
		t.Fatalf("got %q, %v", h, err)
	}
	h, _ = rangeHeader(0, 1)
	if h != "bytes=0-0" {
		t.Fatalf("got %q", h)
	}
	for _, c := range [][2]int64{{-1, 1}, {0, 0}, {0, -3}} {
		if _, err := rangeHeader(c[0], c[1]); err == nil {
			t.Errorf("off=%d n=%d: expected error", c[0], c[1])
		}
	}
}

func httpErr(code int, inner error) error {
	return &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: code}},
			Err:      inner,
		},
	}
}

func TestIsNotFound(t *testing.T) {
	yes := []error{
		&s3types.NotFound{},
		&s3types.NoSuchKey{},
		&smithy.GenericAPIError{Code: "NotFound"},
		&smithy.GenericAPIError{Code: "NoSuchKey"},
		httpErr(404, errors.New("x")),
		fmt.Errorf("wrapped: %w", &s3types.NoSuchKey{}),
	}
	for i, e := range yes {
		if !isNotFound(e) {
			t.Errorf("yes[%d] %v: expected true", i, e)
		}
	}
	for i, e := range []error{nil, errors.New("boom"), httpErr(500, errors.New("x")), &smithy.GenericAPIError{Code: "AccessDenied"}} {
		if isNotFound(e) {
			t.Errorf("no[%d]: expected false", i)
		}
	}
}

func TestIsPreconditionFailed(t *testing.T) {
	if !isPreconditionFailed(httpErr(412, errors.New("x"))) {
		t.Error("412 should match")
	}
	if !isPreconditionFailed(&smithy.GenericAPIError{Code: "PreconditionFailed"}) {
		t.Error("code should match")
	}
	if isPreconditionFailed(nil) || isPreconditionFailed(httpErr(409, errors.New("x"))) {
		t.Error("false positive")
	}
}

// fakeS3 is an in-memory s3API. condOK controls whether If-None-Match is honoured.
type fakeS3 struct {
	objs    map[string][]byte
	heads   int
	puts    int
	condSet int
	// getOverride, if set, answers every GetObject.
	getOverride func() (*s3.GetObjectOutput, error)
	// meta records the headers of the last PutObject per key.
	meta map[string]PutOptions
	// listPage is the page size of ListObjectsV2; lists counts its calls.
	listPage, lists int
	// deleteErr, if set, answers every DeleteObject.
	deleteErr error
}

// DeleteObject answers 204 whether or not the key exists, as S3 and R2 do.
func (f *fakeS3) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	delete(f.objs, *in.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.puts++
	b, _ := io.ReadAll(in.Body)
	if in.IfNoneMatch != nil {
		f.condSet++
		if _, ok := f.objs[*in.Key]; ok {
			return nil, httpErr(412, &smithy.GenericAPIError{Code: "PreconditionFailed"})
		}
	}
	f.objs[*in.Key] = b
	if f.meta == nil {
		f.meta = map[string]PutOptions{}
	}
	f.meta[*in.Key] = PutOptions{CacheControl: aws.ToString(in.CacheControl), ContentType: aws.ToString(in.ContentType)}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.getOverride != nil {
		return f.getOverride()
	}
	b, ok := f.objs[*in.Key]
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b))}, nil
}

func (f *fakeS3) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.heads++
	if _, ok := f.objs[*in.Key]; !ok {
		return nil, &s3types.NotFound{}
	}
	return &s3.HeadObjectOutput{}, nil
}

// ListObjectsV2 pages the sorted keys under Prefix listPage at a time; the
// continuation token is the last key of the previous page.
func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.lists++
	var keys []string
	for k := range f.objs {
		if strings.HasPrefix(k, aws.ToString(in.Prefix)) && k > aws.ToString(in.ContinuationToken) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	out := &s3.ListObjectsV2Output{}
	if len(keys) > f.listPage {
		keys = keys[:f.listPage]
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = aws.String(keys[len(keys)-1])
	}
	for _, k := range keys {
		out.Contents = append(out.Contents, s3types.Object{Key: aws.String(k)})
	}
	out.KeyCount = aws.Int32(int32(len(keys)))
	return out, nil
}

func TestS3ListPaginates(t *testing.T) {
	f := &fakeS3{objs: map[string][]byte{"manifest.json": nil}, listPage: 2}
	var want []string
	for i := range 5 {
		k := fmt.Sprintf("segments/%d.seg", i)
		f.objs[k] = nil
		want = append(want, k)
	}
	s := &s3Store{api: f, bucket: "b"}
	got, err := s.List(context.Background(), "segments/")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("List = %v, want %v", got, want)
	}
	if f.lists != 3 {
		t.Fatalf("%d ListObjectsV2 calls, want 3 pages", f.lists)
	}
}

func TestS3PutIfAbsent(t *testing.T) {
	for _, cond := range []bool{false, true} {
		t.Run(fmt.Sprintf("conditional=%v", cond), func(t *testing.T) {
			f := &fakeS3{objs: map[string][]byte{}}
			s := &s3Store{api: f, bucket: "b", opts: Options{UseConditionalPut: cond}}
			ctx := context.Background()
			if err := s.PutIfAbsent(ctx, "k", []byte("one")); err != nil {
				t.Fatal(err)
			}
			if err := s.PutIfAbsent(ctx, "k", []byte("one")); err != nil {
				t.Fatalf("identical: %v", err)
			}
			if err := s.PutIfAbsent(ctx, "k", []byte("two")); !errors.Is(err, ErrExists) {
				t.Fatalf("different: %v", err)
			}
			if string(f.objs["k"]) != "one" {
				t.Fatalf("overwritten: %q", f.objs["k"])
			}
			if cond && (f.heads != 0 || f.condSet == 0) {
				t.Fatalf("conditional mode must not HEAD: heads=%d cond=%d", f.heads, f.condSet)
			}
			if !cond && f.condSet != 0 {
				t.Fatal("default mode must not send If-None-Match")
			}
		})
	}
}

func TestS3GetMissingAndExists(t *testing.T) {
	f := &fakeS3{objs: map[string][]byte{}}
	s := &s3Store{api: f, bucket: "b"}
	ctx := context.Background()
	if _, err := s.Get(ctx, "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
	if ok, err := s.Exists(ctx, "x"); ok || err != nil {
		t.Fatalf("exists: %v %v", ok, err)
	}
}

func TestS3DeleteMissingIsNil(t *testing.T) {
	f := &fakeS3{objs: map[string][]byte{"k": []byte("x")}}
	s := &s3Store{api: f, bucket: "b"}
	ctx := context.Background()
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.objs["k"]; ok {
		t.Fatal("object not deleted")
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("204 on missing: %v", err)
	}
	f.deleteErr = &s3types.NoSuchKey{}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("NoSuchKey: %v", err)
	}
	f.deleteErr = httpErr(500, errors.New("boom"))
	if err := s.Delete(ctx, "k"); err == nil || !strings.Contains(err.Error(), `"k"`) {
		t.Fatalf("500: %v", err)
	}
}

func TestNewR2Validates(t *testing.T) {
	if _, err := NewR2("", "k", "s", "b"); err == nil {
		t.Fatal("expected error")
	}
	if _, err := NewR2("acc", "k", "s", "b"); err != nil {
		t.Fatal(err)
	}
}

func TestS3GetRangeOutOfRange(t *testing.T) {
	ctx := context.Background()
	for name, get := range map[string]func() (*s3.GetObjectOutput, error){
		"416": func() (*s3.GetObjectOutput, error) {
			return nil, httpErr(416, &smithy.GenericAPIError{Code: "InvalidRange"})
		},
		"InvalidRange code": func() (*s3.GetObjectOutput, error) {
			return nil, &smithy.GenericAPIError{Code: "InvalidRange"}
		},
		"short body": func() (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader([]byte("89")))}, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeS3{objs: map[string][]byte{"k": []byte("0123456789")}, getOverride: get}
			s := &s3Store{api: f, bucket: "b"}
			if _, err := s.GetRange(ctx, "k", 8, 5); !errors.Is(err, ErrOutOfRange) {
				t.Fatalf("got %v, want ErrOutOfRange", err)
			}
		})
	}
	// A generic server error is not a range problem.
	f := &fakeS3{objs: map[string][]byte{}, getOverride: func() (*s3.GetObjectOutput, error) {
		return nil, httpErr(500, errors.New("x"))
	}}
	s := &s3Store{api: f, bucket: "b"}
	if _, err := s.GetRange(ctx, "k", 0, 1); err == nil || errors.Is(err, ErrOutOfRange) {
		t.Fatalf("500: %v", err)
	}
}

func TestS3PutSendsObjectHeaders(t *testing.T) {
	ctx := context.Background()
	imm := PutOptions{CacheControl: CacheImmutable, ContentType: TypeBinary}
	mut := PutOptions{CacheControl: CacheNoCache, ContentType: TypeJSON}
	for _, cond := range []bool{false, true} {
		f := &fakeS3{objs: map[string][]byte{}}
		s := &s3Store{api: f, bucket: "b", opts: Options{UseConditionalPut: cond}}
		if err := s.PutIfAbsent(ctx, "segments/a.seg", []byte("x"), imm); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, "manifest.json", []byte("{}"), mut); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, "plain", []byte("p")); err != nil {
			t.Fatal(err)
		}
		if got := f.meta["segments/a.seg"]; got != imm {
			t.Errorf("cond=%v seg headers %+v", cond, got)
		}
		if got := f.meta["manifest.json"]; got != mut {
			t.Errorf("cond=%v manifest headers %+v", cond, got)
		}
		if got := f.meta["plain"]; got != (PutOptions{}) {
			t.Errorf("cond=%v no options sent %+v", cond, got)
		}
	}
}
