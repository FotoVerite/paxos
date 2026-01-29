use axum::{
    extract::{Path, State},
    response::{Html, IntoResponse},
};
use tera::{Context, Tera};

use super::utils::AppState;

/// Generic handler for Tera templates.
/// This matches any path and tries to find a corresponding template.
/// For example, access to /foo/bar will try to render templates/foo/bar.html.
pub async fn tera_handler(
    uri: axum::http::Uri,
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
    match state.tera.render(&template_name, &Context::new()) {
        Ok(html) => Html(html).into_response(),
        Err(e) => {
            tracing::error!("Template rendering error for {}: {}", template_name, e);
            // In a real app, you might want a custom 404 page
            (axum::http::StatusCode::NOT_FOUND, format!("Template not found: {}", e)).into_response()
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
    path: Option<Path<String>>,
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
    
    render_template(&state.tera, &template_name)
}

pub async fn paxos_handler(
    uri: axum::http::Uri,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
     let template_name = if path.is_empty() {
        "paxos/index.html".to_string()
    } else if path.ends_with('/') {
        format!("paxos/{}index.html", path)
    } else {
        format!("paxos/{}.html", path)
    };
    
    render_template(&state.tera, &template_name)
}


fn render_template(tera: &Tera, name: &str) -> axum::response::Response {
     match tera.render(name, &Context::new()) {
        Ok(html) => Html(html).into_response(),
        Err(e) => {
            tracing::error!("Template rendering error for {}: {}", name, e);
            (axum::http::StatusCode::NOT_FOUND, format!("Template not found: {}", e)).into_response()
        }
    }
}
