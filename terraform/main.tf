terraform {
  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 4.45.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.85.0"
    }
  }

  required_version = ">= 1.6.6"

  backend "s3" {
    bucket = "opentimes-resources"
    key    = "terraform/terraform.tfstate"

    region                      = "auto"
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    skip_region_validation      = true
    skip_requesting_account_id  = true
    skip_s3_checksum            = true
    use_path_style              = true
    endpoint                    = "https://${var.cloudflare_account_id}.r2.cloudflarestorage.com"

    /* Set AWS_PROFILE to "cloudflare" before running commands */
  }
}

provider "aws" {
  region = "us-east-1"

  skip_credentials_validation = true
  skip_region_validation      = true
  skip_requesting_account_id  = true

  endpoints {
    s3 = "https://${var.cloudflare_account_id}.r2.cloudflarestorage.com"
  }
}

provider "cloudflare" {
  api_token = var.cloudflare_api_token
}
