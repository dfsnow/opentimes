resource "cloudflare_ruleset" "cache_data_subdomain" {
  zone_id     = cloudflare_zone.opentimes-org.id
  name        = "Cache data files for data subdomain"
  description = "Cache all data files for data.opentimes.org"
  kind        = "zone"
  phase       = "http_request_cache_settings"

  rules {
    action = "set_cache_settings"
    action_parameters {
      edge_ttl {
        mode    = "override_origin"
        default = 1209600
      }
      browser_ttl {
        mode = "bypass"
      }
      serve_stale {
        disable_stale_while_updating = true
      }
      respect_strong_etags = false
      origin_error_page_passthru = false
    }
    expression  = "(http.request.full_uri wildcard \"https://data.opentimes.org/*\" and http.request.uri.path.extension in {\"parquet\" \"json\" \"html\"})"
    description = "Cache data files for data subdomain"
    enabled     = true
  }
}
