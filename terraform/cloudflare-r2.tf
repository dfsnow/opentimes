resource "cloudflare_r2_bucket" "opentimes-data" {
  account_id = var.cloudflare_account_id
  name       = "opentimes-data"
  location   = "ENAM"
}

resource "cloudflare_r2_bucket" "opentimes-dvc" {
  account_id = var.cloudflare_account_id
  name       = "opentimes-dvc"
  location   = "ENAM"
}

resource "cloudflare_r2_bucket" "opentimes-public" {
  account_id = var.cloudflare_account_id
  name       = "opentimes-public"
  location   = "ENAM"

  /* Need to manually enable public bucket access via UI and add CORS
  policy for https://opentimes.org */
}

resource "cloudflare_r2_bucket" "opentimes-resources" {
  account_id = var.cloudflare_account_id
  name       = "opentimes-resources"
  location   = "ENAM"
}
