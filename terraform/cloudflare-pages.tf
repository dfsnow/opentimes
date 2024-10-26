resource "cloudflare_pages_project" "opentimes-org" {
  account_id        = var.cloudflare_account_id
  name              = "opentimes-org"
  production_branch = "main"

  source {
    type = "github"
    config {
      owner                         = "dfsnow"
      repo_name                     = "opentimes"
      production_branch             = "main"
      pr_comments_enabled           = true
      deployments_enabled           = true
      production_deployment_enabled = true
      preview_deployment_setting    = "custom"
      preview_branch_includes       = ["dev", "preview"]
      preview_branch_excludes       = ["main"]
    }
  }

  build_config {
    build_command       = "hugo --gc --minify"
    destination_dir     = "build"
    root_dir            = "site"
  }

  deployment_configs {
    preview {
      environment_variables = {
        ENVIRONMENT = "preview"
      }
    }
    production {
      environment_variables = {
        ENVIRONMENT = "production"
      }
    }
  }
}
