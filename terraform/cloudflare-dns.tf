resource "cloudflare_zone" "opentimes-org" {
  account_id = var.cloudflare_account_id
  zone       = "opentimes.org"
}

resource "cloudflare_zone_settings_override" "opentimes-org" {
  zone_id = cloudflare_zone.opentimes-org.id

  settings {
    tls_1_3                  = "on"
    automatic_https_rewrites = "on"
    ssl                      = "strict"
  }
}
