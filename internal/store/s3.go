package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Options tunes the R2 store.
type Options struct {
	// UseConditionalPut makes PutIfAbsent use PutObject with
	// "If-None-Match: *" instead of HEAD + PUT. Enable only after verifying
	// that the bucket honours conditional PUT.
	UseConditionalPut bool
}

// s3API is the subset of *s3.Client the store uses (fakeable in tests).
type s3API interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

type s3Store struct {
	api    s3API
	bucket string
	opts   Options
}

// NewR2 returns a Store backed by a Cloudflare R2 bucket.
func NewR2(accountID, keyID, secret, bucket string, opts ...Options) (Store, error) {
	if accountID == "" || keyID == "" || secret == "" || bucket == "" {
		return nil, errors.New("store: R2 account id, key id, secret and bucket are required")
	}
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	client := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID)),
		Credentials:  credentials.NewStaticCredentialsProvider(keyID, secret, ""),
		UsePathStyle: true,
		// R2 does not support the SDK's default trailing checksums.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	return &s3Store{api: client, bucket: bucket, opts: o}, nil
}

// putInput builds a PutObjectInput carrying the object headers of opts.
func (s *s3Store) putInput(key string, body []byte, opts []PutOptions) *s3.PutObjectInput {
	o := firstOpts(opts)
	in := &s3.PutObjectInput{
		Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
	}
	if o.CacheControl != "" {
		in.CacheControl = aws.String(o.CacheControl)
	}
	if o.ContentType != "" {
		in.ContentType = aws.String(o.ContentType)
	}
	return in
}

func (s *s3Store) Put(ctx context.Context, key string, body []byte, opts ...PutOptions) error {
	_, err := s.api.PutObject(ctx, s.putInput(key, body, opts))
	if err != nil {
		return fmt.Errorf("store: put %q: %w", key, err)
	}
	return nil
}

// PutIfAbsent is idempotent for identical bytes and ErrExists otherwise.
//
// Without UseConditionalPut it is HEAD followed by PUT, which is not atomic.
// That is acceptable: the archive has a single writer per key range, so two
// writers never race on the same key.
func (s *s3Store) PutIfAbsent(ctx context.Context, key string, body []byte, opts ...PutOptions) error {
	if s.opts.UseConditionalPut {
		in := s.putInput(key, body, opts)
		in.IfNoneMatch = aws.String("*")
		_, err := s.api.PutObject(ctx, in)
		if err == nil {
			return nil
		}
		if !isPreconditionFailed(err) {
			return fmt.Errorf("store: put %q: %w", key, err)
		}
		return s.compareExisting(ctx, key, body)
	}
	ok, err := s.Exists(ctx, key)
	if err != nil {
		return err
	}
	if ok {
		return s.compareExisting(ctx, key, body)
	}
	return s.Put(ctx, key, body, opts...)
}

func (s *s3Store) compareExisting(ctx context.Context, key string, body []byte) error {
	existing, err := s.Get(ctx, key)
	if err != nil {
		return err
	}
	if bytes.Equal(existing, body) {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrExists, key)
}

func (s *s3Store) Get(ctx context.Context, key string) ([]byte, error) {
	return s.get(ctx, key, nil)
}

func (s *s3Store) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	h, err := rangeHeader(off, n)
	if err != nil {
		return nil, err
	}
	b, err := s.get(ctx, key, &h)
	if err != nil {
		if isInvalidRange(err) {
			return nil, fmt.Errorf("%w: %w", ErrOutOfRange, err)
		}
		return nil, err
	}
	if int64(len(b)) != n {
		return nil, fmt.Errorf("%w: get range %q [%d,+%d): got %d bytes", ErrOutOfRange, key, off, n, len(b))
	}
	return b, nil
}

func (s *s3Store) get(ctx context.Context, key string, rng *string) ([]byte, error) {
	out, err := s.api.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key, Range: rng})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil, fmt.Errorf("store: get %q: %w", key, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("store: read %q: %w", key, err)
	}
	return b, nil
}

func (s *s3Store) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.api.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: &key})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("store: head %q: %w", key, err)
}

// Delete treats NoSuchKey like the usual 204 for a missing key.
func (s *s3Store) Delete(ctx context.Context, key string) error {
	_, err := s.api.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("store: delete %q: %w", key, err)
	}
	return nil
}

// List pages through ListObjectsV2 until the listing is complete.
func (s *s3Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	p := s3.NewListObjectsV2Paginator(s.api, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("store: list %q: %w", prefix, err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	slices.Sort(keys)
	return keys, nil
}

// rangeHeader formats an HTTP Range header value for n bytes from off.
func rangeHeader(off, n int64) (string, error) {
	if off < 0 || n <= 0 {
		return "", fmt.Errorf("%w: invalid range off=%d n=%d", ErrOutOfRange, off, n)
	}
	return fmt.Sprintf("bytes=%d-%d", off, off+n-1), nil
}

func httpStatus(err error) int {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

func apiCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nf *s3types.NotFound
	var nk *s3types.NoSuchKey
	if errors.As(err, &nf) || errors.As(err, &nk) {
		return true
	}
	if c := apiCode(err); c == "NotFound" || c == "NoSuchKey" {
		return true
	}
	return httpStatus(err) == http.StatusNotFound
}

// isInvalidRange reports a 416 / InvalidRange answer to a range request.
func isInvalidRange(err error) bool {
	return apiCode(err) == "InvalidRange" || httpStatus(err) == http.StatusRequestedRangeNotSatisfiable
}

func isPreconditionFailed(err error) bool {
	if err == nil {
		return false
	}
	return apiCode(err) == "PreconditionFailed" || httpStatus(err) == http.StatusPreconditionFailed
}
