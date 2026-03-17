use axum::{
    extract::{Path, State},
    http::Uri,
    response::{Html, IntoResponse},
};
use tera::{Context, Tera};

use super::utils::AppState;
use crate::web::subdomain::Subdomain;

fn site_root_for(site_slug: &str, uri_path: &str, subdomain: &str) -> String {
    if site_slug == "paxos" {
        return String::new();
    }

    if subdomain == site_slug {
        return String::new();
    }

    let papers_prefix = format!("/papers/{}", site_slug);
    if uri_path.starts_with(&papers_prefix) {
        return papers_prefix;
    }

    let slug_prefix = format!("/{}", site_slug);
    if uri_path.starts_with(&slug_prefix) {
        return slug_prefix;
    }

    papers_prefix
}

fn build_site_context(site_slug: &str, uri: &Uri, subdomain: &str) -> Context {
    let mut context = Context::new();
    let site_root = site_root_for(site_slug, uri.path(), subdomain);
    context.insert("site_slug", site_slug);
    context.insert("site_root", &site_root);
    context
}

/// Generic handler for Tera templates.
/// This matches any path and tries to find a corresponding template.
/// For example, access to /foo/bar will try to render templates/foo/bar.html.
pub async fn tera_handler(
    uri: Uri,
    Subdomain(subdomain): Subdomain,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Default to index.html if path ends in /
    let template_name = if path.is_empty() {
        "paxos/index.html".to_string()
    } else if path.ends_with('/') {
        format!("{}index.html", path)
    } else {
        format!("{}.html", path)
    };

    // Try to render the template
    let context = build_site_context("paxos", &uri, &subdomain);
    match state.tera.render(&template_name, &context) {
        Ok(html) => Html(html).into_response(),
        Err(e) => {
            tracing::error!("Template rendering error for {}: {}", template_name, e);
            // In a real app, you might want a custom 404 page
            (
                axum::http::StatusCode::NOT_FOUND,
                format!("Template not found: {}", e),
            )
                .into_response()
        }
    }
}

/// Handler that prepends a prefix to the template path.
/// Useful for nested routers where the URI path is relative.
/// Usage: .route("/*path", get(|p, s| tera_prefix_handler(p, s, "paxos_made_simple")))
/// But closure in route is tricky with typing, so we might need a specific handler per section
/// or a wrapper struct.
/// For now, let's just make specific handlers for the known apps.

pub async fn paxos_made_simple_handler(
    uri: Uri,
    path: Option<Path<String>>,
    Subdomain(subdomain): Subdomain,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let path = path.map(|p| p.0).unwrap_or_else(|| "index".to_string());
    let path = path.trim_end_matches('/'); // Remove trailing slash

    // Check if it's a directory-like path (index)
    let template_name = if path.is_empty() || path == "index" {
        "paxos-made-simple/index.html".to_string()
    } else {
        format!("paxos-made-simple/{}.html", path)
    };

    let context = build_site_context("paxos-made-simple", &uri, &subdomain);
    render_template(&state.tera, &template_name, context)
}

pub async fn paxos_made_moderately_complex_handler(
    uri: Uri,
    path: Option<Path<String>>,
    Subdomain(subdomain): Subdomain,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let path = path.map(|p| p.0).unwrap_or_else(|| "index".to_string());
    let path = path.trim_end_matches('/'); // Remove trailing slash

    // Check if it's a directory-like path (index)
    let template_name = if path.is_empty() || path == "index" {
        "paxos-made-moderately-complex/index.html".to_string()
    } else {
        format!("paxos-made-moderately-complex/{}.html", path)
    };

    let context = build_site_context("paxos-made-moderately-complex", &uri, &subdomain);
    render_template(&state.tera, &template_name, context)
}

pub async fn reconfiguring_a_state_machine_handler(
    uri: Uri,
    path: Option<Path<String>>,
    Subdomain(subdomain): Subdomain,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let path = path.map(|p| p.0).unwrap_or_else(|| "index".to_string());
    let path = path.trim_end_matches('/');

    let template_name = if path.is_empty() || path == "index" {
        "reconfiguring-a-state-machine/index.html".to_string()
    } else {
        format!("reconfiguring-a-state-machine/{}.html", path)
    };

    let context = build_site_context("reconfiguring-a-state-machine", &uri, &subdomain);
    render_template(&state.tera, &template_name, context)
}

pub async fn vertical_paxos_handler(
    uri: Uri,
    path: Option<Path<String>>,
    Subdomain(subdomain): Subdomain,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let path = path.map(|p| p.0).unwrap_or_else(|| "index".to_string());
    let path = path.trim_end_matches('/');

    let template_name = if path.is_empty() || path == "index" {
        "vertical-paxos/index.html".to_string()
    } else {
        format!("vertical-paxos/{}.html", path)
    };

    let context = build_site_context("vertical-paxos", &uri, &subdomain);
    render_template(&state.tera, &template_name, context)
}

pub async fn paxos_handler(
    uri: Uri,
    Subdomain(subdomain): Subdomain,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    let (site_slug, template_name) = match subdomain.as_str() {
        "paxos-made-simple" => {
            let template_name = if path.is_empty() || path == "/" {
                "paxos-made-simple/index.html".to_string()
            } else if path.ends_with('/') {
                format!("paxos-made-simple/{}index.html", path)
            } else {
                format!("paxos-made-simple/{}.html", path)
            };
            ("paxos-made-simple", template_name)
        }
        "paxos-made-moderately-complex" => {
            let template_name = if path.is_empty() || path == "/" {
                "paxos-made-moderately-complex/index.html".to_string()
            } else if path.ends_with('/') {
                format!("paxos-made-moderately-complex/{}index.html", path)
            } else {
                format!("paxos-made-moderately-complex/{}.html", path)
            };
            ("paxos-made-moderately-complex", template_name)
        }
        "reconfiguring-a-state-machine" => {
            let template_name = if path.is_empty() || path == "/" {
                "reconfiguring-a-state-machine/index.html".to_string()
            } else if path.ends_with('/') {
                format!("reconfiguring-a-state-machine/{}index.html", path)
            } else {
                format!("reconfiguring-a-state-machine/{}.html", path)
            };
            ("reconfiguring-a-state-machine", template_name)
        }
        "distributed-design" => {
            let template_name = if path.is_empty() || path == "/" {
                "distributed-design/index.html".to_string()
            } else if path.ends_with('/') {
                format!("distributed-design/{}index.html", path)
            } else {
                format!("distributed-design/{}.html", path)
            };
            ("distributed-design", template_name)
        }
        "vertical-paxos" => {
            let template_name = if path.is_empty() || path == "/" {
                "vertical-paxos/index.html".to_string()
            } else if path.ends_with('/') {
                format!("vertical-paxos/{}index.html", path)
            } else {
                format!("vertical-paxos/{}.html", path)
            };
            ("vertical-paxos", template_name)
        }
        _ => {
            let template_name = if path.is_empty() {
                "paxos/index.html".to_string()
            } else if path.ends_with('/') {
                format!("paxos/{}index.html", path)
            } else {
                format!("paxos/{}.html", path)
            };
            ("paxos", template_name)
        }
    };

    let context = build_site_context(site_slug, &uri, &subdomain);
    render_template(&state.tera, &template_name, context)
}

fn render_template(tera: &Tera, name: &str, context: Context) -> axum::response::Response {
    match tera.render(name, &context) {
        Ok(html) => Html(html).into_response(),
        Err(e) => {
            tracing::error!("Template rendering error for {}: {}", name, e);
            (
                axum::http::StatusCode::NOT_FOUND,
                format!("Template not found: {}", e),
            )
                .into_response()
        }
    }
}
