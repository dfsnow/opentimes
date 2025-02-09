resource "cloudflare_ruleset" "cache_all" {
  zone_id     = cloudflare_zone.opentimes-org.id
  name        = "Cache all files"
  description = "Cache all data files"
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
        mode    = "override_origin"
        default = 7200
      }
      serve_stale {
        disable_stale_while_updating = true
      }
      respect_strong_etags       = false
      origin_error_page_passthru = false
      cache                      = true
    }
    expression = true
    description = "Cache all files"
    enabled     = true
  }
}

resource "cloudflare_ruleset" "compress_all_but_parquet" {
  zone_id     = cloudflare_zone.opentimes-org.id
  name        = "Compress all but Parquet"
  description = "Parquet requires HEAD requests"
  kind        = "zone"
  phase       = "http_response_compression"
  rules {
    action = "compress_response"
    action_parameters {
      algorithms {
        name = "zstd"
      }
      algorithms {
        name = "brotli"
      }
      algorithms {
        name = "gzip"
      }
    }
    expression  = "(http.request.full_uri wildcard \"https://*opentimes.org/*\" and http.request.method ne \"HEAD\")"
    description = "Compress all except HEAD"
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
    expression  = "starts_with(http.host, \"data\") and (ends_with(http.request.uri.path, \"/\") or http.request.uri.path == \"\") and not (ends_with(http.request.uri.path, \".json\") or ends_with(http.request.uri.path, \".parquet\"))"
    description = "Rewrite all / to index.html in data subdomain"
    enabled     = true
  }
}
