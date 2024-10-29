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

/* Need to manually add an R2 CNAME record via the UI since those aren't 
   managed by terraform. Likewise with the Cloudflare Pages CNAME records */

resource "cloudflare_record" "opentimes-org-mx1-root" {
  zone_id = cloudflare_zone.opentimes-org.id
  name    = "opentimes.org"
  type    = "MX"
  content = "in1-smtp.messagingengine.com"
  priority = 10
}

resource "cloudflare_record" "opentimes-org-mx2-root" {
  zone_id = cloudflare_zone.opentimes-org.id
  name    = "opentimes.org"
  type    = "MX"
  content = "in2-smtp.messagingengine.com"
  priority = 20
}

resource "cloudflare_record" "opentimes-org-fm1" {
  zone_id = cloudflare_zone.opentimes-org.id
  name    = "fm1._domainkey"
  type    = "CNAME"
  content = "fm1.opentimes.org.dkim.fmhosted.com"
}

resource "cloudflare_record" "opentimes-org-fm2" {
  zone_id = cloudflare_zone.opentimes-org.id
  name    = "fm2._domainkey"
  type    = "CNAME"
  content = "fm2.opentimes.org.dkim.fmhosted.com"
}

resource "cloudflare_record" "opentimes-org-fm3" {
  zone_id = cloudflare_zone.opentimes-org.id
  name    = "fm3._domainkey"
  type    = "CNAME"
  content = "fm3.opentimes.org.dkim.fmhosted.com"
}

resource "cloudflare_record" "opentimes-org-spf" {
  zone_id = cloudflare_zone.opentimes-org.id
  name    = "opentimes.org"
  type    = "TXT"
  content = "v=spf1 include:spf.messagingengine.com ?all"
}

resource "cloudflare_record" "opentimes-org-dmarc" {
  zone_id = cloudflare_zone.opentimes-org.id
  name    = "_dmarc"
  type    = "TXT"
  content = "v=DMARC1;p=none;rua=mailto:1ccc182bc464478ebe689a3da8739e1a@dmarc-reports.cloudflare.net"
}
