use crate::web::handlers::AppState;
use crate::web::handlers::tera_handler::vertical_paxos_handler;
use crate::web::subdomain::Subdomain;
use axum::{Router, http::Uri, routing::get};

/// Vertical Paxos paper router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/*path", get(vertical_paxos_handler))
        .route(
            "/",
            get(|uri: Uri, subdomain: Subdomain, state| {
                vertical_paxos_handler(uri, None, subdomain, state)
            }),
        )
}
