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
      cache = true
    }
    expression  = "(http.request.full_uri wildcard \"https://data.opentimes.org/*\")"
    description = "Cache data files for data subdomain"
    enabled     = true
  }
}

resource "cloudflare_ruleset" "serve_index_data_subdomain" {
  zone_id     = cloudflare_zone.opentimes-org.id
  name        = "Serve index.html to /"
  description = "Replace / with /index.html in data subdomain"
  kind        = "zone"
  phase       = "http_request_transform"

  rules {
    action = "rewrite"
      action_parameters {
        uri {
          path {
            expression = "concat(http.request.uri.path, \"index.html\")"
          }
        }
      }
    expression = "starts_with(http.host, \"data\") and (ends_with(http.request.uri.path, \"/\") or http.request.uri.path == \"\") and not (ends_with(http.request.uri.path, \".json\") or ends_with(http.request.uri.path, \".parquet\"))"
    description = "Rewrite all / to index.html in data subdomain" 
    enabled = true
  }
}
