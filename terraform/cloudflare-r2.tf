resource "cloudflare_r2_bucket" "opentimes-data" {
  account_id = var.cloudflare_account_id
  name       = "opentimes-data"
  location   = "ENAM"
}

resource "aws_s3_bucket_lifecycle_configuration" "opentimes-data-lifecycle" {
  bucket = cloudflare_r2_bucket.opentimes-data.name

  rule {
    id     = "abort-multipart-upload"
    status = "Enabled"
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "cloudflare_r2_bucket" "opentimes-dvc" {
  account_id = var.cloudflare_account_id
  name       = "opentimes-dvc"
  location   = "ENAM"
}

resource "aws_s3_bucket_lifecycle_configuration" "opentimes-dvc-lifecycle" {
  bucket = cloudflare_r2_bucket.opentimes-dvc.name

  rule {
    id     = "abort-multipart-upload"
    status = "Enabled"
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "cloudflare_r2_bucket" "opentimes-public" {
  account_id = var.cloudflare_account_id
  name       = "opentimes-public"
  location   = "ENAM"
}

resource "aws_s3_bucket_lifecycle_configuration" "opentimes-public-lifecycle" {
  bucket = cloudflare_r2_bucket.opentimes-public.name

  rule {
    id     = "abort-multipart-upload"
    status = "Enabled"
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_cors_configuration" "opentimes-public-cors" {
  bucket = cloudflare_r2_bucket.opentimes-public.name

  cors_rule {
    allowed_headers = ["Content-Range"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["https://shell.duckdb.org", "https://opentimes.org", "*"]
    expose_headers  = ["Content-Range"]
  }
}

resource "cloudflare_r2_bucket" "opentimes-resources" {
  account_id = var.cloudflare_account_id
  name       = "opentimes-resources"
  location   = "ENAM"
}

resource "aws_s3_bucket_lifecycle_configuration" "opentimes-resources-lifecycle" {
  bucket = cloudflare_r2_bucket.opentimes-resources.name

  rule {
    id     = "expire-cache-items"
    status = "Enabled"
    filter {
      prefix = "cache/"
    }
    expiration {
      days = 3
    }
  }

  rule {
    id     = "abort-multipart-upload"
    status = "Enabled"
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
