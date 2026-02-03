use axum::{Router, routing::get, http::Uri};
use crate::web::handlers::tera_handler::paxos_made_moderately_complex_handler;
use crate::web::handlers::AppState;
use crate::web::subdomain::Subdomain;

/// Paxos Made Moderately Complex paper router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/*path", get(paxos_made_moderately_complex_handler))
        .route("/", get(|uri: Uri, subdomain: Subdomain, state| {
            paxos_made_moderately_complex_handler(uri, None, subdomain, state)
        }))
}
