// Command apt-publish syncs the cs-agent apt repository to an S3-compatible
// object store, for the GitHub Actions release pipeline.
//
// It deliberately mirrors the exact S3-compatible client options proven in
// s3upload/s3upload.go — path-style addressing, static credentials, and
// request/response checksums calculated only when required (some S3-compatible
// stores reject the SDK's default CRC32 checksums with XAmzContentSHA256Mismatch).
// That is why we build a plain s3.Client here rather than reaching for the AWS CLI
// or deb-s3, whose checksum behaviour against such stores is the unknown we avoid.
//
// Usage:
//
//	apt-publish pull <local-dir>   # download <prefix>pool/** into <local-dir>/pool/**
//	apt-publish push <local-dir>   # upload <local-dir>/** to <prefix>**, signed Release files LAST
//
// Config via env: APT_S3_ENDPOINT, APT_S3_REGION, APT_S3_BUCKET, APT_S3_PREFIX,
// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type config struct {
	endpoint string
	region   string
	bucket   string
	prefix   string
}

func loadConfig() config {
	c := config{
		endpoint: os.Getenv("APT_S3_ENDPOINT"),
		region:   os.Getenv("APT_S3_REGION"),
		bucket:   os.Getenv("APT_S3_BUCKET"),
		prefix:   os.Getenv("APT_S3_PREFIX"),
	}
	if c.bucket == "" {
		log.Fatal("apt-publish: APT_S3_BUCKET is required")
	}
	if c.region == "" {
		c.region = "us-east-1"
	}
	// Normalize prefix: no leading slash; single trailing slash when non-empty.
	c.prefix = strings.TrimPrefix(c.prefix, "/")
	if c.prefix != "" && !strings.HasSuffix(c.prefix, "/") {
		c.prefix += "/"
	}
	return c
}

func newClient(c config) *s3.Client {
	awsCfg := aws.Config{
		Region: c.region,
		Credentials: credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), ""),
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if c.endpoint != "" {
			o.BaseEndpoint = aws.String(c.endpoint)
		}
		o.UsePathStyle = true
		// Mirror s3upload.New: some S3-compatible stores reject the SDK's default CRC32 request checksums.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
}

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("usage: %s <pull|push> <local-dir>", filepath.Base(os.Args[0]))
	}
	cmd, dir := os.Args[1], os.Args[2]
	c := loadConfig()
	client := newClient(c)
	ctx := context.Background()

	switch cmd {
	case "pull":
		if err := pull(ctx, client, c, dir); err != nil {
			log.Fatalf("apt-publish pull: %v", err)
		}
	case "push":
		if err := push(ctx, client, c, dir); err != nil {
			log.Fatalf("apt-publish push: %v", err)
		}
	default:
		log.Fatalf("apt-publish: unknown command %q (want pull|push)", cmd)
	}
}

// pull downloads <prefix>pool/** into <dir>/pool/** (the full version history).
func pull(ctx context.Context, client *s3.Client, c config, dir string) error {
	poolPrefix := c.prefix + "pool/"
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(poolPrefix),
	})
	n := 0
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			rel := strings.TrimPrefix(key, c.prefix) // e.g. pool/main/c/cs-agent/...
			dst := filepath.Join(dir, filepath.FromSlash(rel))
			if err := downloadOne(ctx, client, c.bucket, key, dst); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
			n++
		}
	}
	log.Printf("apt-publish: pulled %d object(s) from s3://%s/%s", n, c.bucket, poolPrefix)
	return nil
}

func downloadOne(ctx context.Context, client *s3.Client, bucket, key, dst string) error {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	defer out.Body.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, out.Body)
	return err
}

// push uploads <dir>/** to <prefix>**. The signed Release files (Release,
// Release.gpg, InRelease) go LAST so a client mid-update never sees a Release
// referencing a Packages index that hasn't been uploaded yet. (For fully atomic
// updates, enable by-hash in build-apt-repo.sh — tracked follow-up.)
func push(ctx context.Context, client *s3.Client, c config, dir string) error {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return err
	}

	isReleaseMeta := func(p string) bool {
		switch filepath.Base(p) {
		case "Release", "Release.gpg", "InRelease":
			return true
		}
		return false
	}
	// Non-Release-meta first; Release-meta last. Stable + lexical within each group.
	sort.SliceStable(files, func(i, j int) bool {
		ri, rj := isReleaseMeta(files[i]), isReleaseMeta(files[j])
		if ri != rj {
			return !ri
		}
		return files[i] < files[j]
	})

	for _, path := range files {
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		key := c.prefix + filepath.ToSlash(rel)
		if err := uploadOne(ctx, client, c.bucket, key, path); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
	}
	log.Printf("apt-publish: pushed %d file(s) to s3://%s/%s", len(files), c.bucket, c.prefix)
	return nil
}

func uploadOne(ctx context.Context, client *s3.Client, bucket, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   f, // *os.File is seekable, so the SDK sets Content-Length without buffering
	})
	return err
}
