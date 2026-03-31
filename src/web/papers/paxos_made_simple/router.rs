use crate::web::handlers::AppState;
use crate::web::handlers::tera_handler::paxos_made_simple_handler;
use crate::web::subdomain::Subdomain;
use axum::{Router, http::Uri, routing::get};

/// Paxos Made Simple paper router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/*path", get(paxos_made_simple_handler))
        .route(
            "/",
            get(|uri: Uri, subdomain: Subdomain, state| {
                paxos_made_simple_handler(uri, None, subdomain, state)
            }),
        )
}
