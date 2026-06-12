package s3upload

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	base := Config{Bucket: "b", PartSizeMB: 64, DefaultTTL: 6 * time.Hour, MaxTTL: 24 * time.Hour}
	if err := base.Validate(50 << 30); err != nil { // 50 GB fits in 64MB*10000
		t.Errorf("valid config rejected: %v", err)
	}

	c := base
	c.Bucket = ""
	if err := c.Validate(0); err == nil {
		t.Error("expected error when bucket is empty")
	}

	c = base
	c.Endpoint = "http://minio:9000"
	if err := c.Validate(0); err == nil {
		t.Error("expected error for non-https endpoint")
	}
	c.Endpoint = "https://minio:9000"
	if err := c.Validate(0); err != nil {
		t.Errorf("https endpoint should be accepted: %v", err)
	}

	c = base
	c.PartSizeMB = 5 // 5MiB*10000 ≈ 48.8GiB < 60GiB
	if err := c.Validate(60 << 30); err == nil {
		t.Error("expected part-size-floor error for a 60GB archive at 5MB parts")
	}

	c = base
	c.PartSizeMB = 4
	if err := c.Validate(0); err == nil {
		t.Error("expected error for part_size_mb < 5")
	}

	c = base
	c.DefaultTTL = 48 * time.Hour
	if err := c.Validate(0); err == nil {
		t.Error("expected error when default_ttl > max_ttl")
	}
}

func TestClampTTL(t *testing.T) {
	c := Config{DefaultTTL: 6 * time.Hour, MaxTTL: 24 * time.Hour}
	if got := c.ClampTTL(0); got != 6*time.Hour {
		t.Errorf("unset ttl: got %s, want 6h", got)
	}
	if got := c.ClampTTL(-time.Second); got != 6*time.Hour {
		t.Errorf("negative ttl: got %s, want 6h", got)
	}
	if got := c.ClampTTL(2 * time.Hour); got != 2*time.Hour {
		t.Errorf("in-range ttl: got %s, want 2h", got)
	}
	if got := c.ClampTTL(48 * time.Hour); got != 24*time.Hour {
		t.Errorf("over-max ttl: got %s, want 24h (capped)", got)
	}
}

// PresignGet signs locally (no network), so we can assert the URL shape.
func TestPresignGet(t *testing.T) {
	u, err := New(Config{
		Endpoint:       "https://s3.example.com",
		Region:         "us-east-1",
		Bucket:         "mybucket",
		Prefix:         "exports/",
		AccessKey:      "AKIAEXAMPLE",
		SecretKey:      "secret",
		ForcePathStyle: true,
		PartSizeMB:     64,
		Concurrency:    4,
		DefaultTTL:     6 * time.Hour,
		MaxTTL:         24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	url, expiry, err := u.PresignGet(context.Background(), "vol-1/archive.tar", time.Hour)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	if !strings.Contains(url, "mybucket") || !strings.Contains(url, "exports/vol-1/archive.tar") {
		t.Errorf("presigned URL missing bucket/key: %s", url)
	}
	if !strings.Contains(url, "X-Amz-Expires=3600") {
		t.Errorf("presigned URL missing 1h expiry: %s", url)
	}
	if expiry.IsZero() {
		t.Error("expected a non-zero expiry time")
	}
}
