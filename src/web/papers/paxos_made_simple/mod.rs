use axum::{Router, routing::get};
use crate::web::handlers::tera_handler::paxos_made_simple_handler;
use crate::web::handlers::AppState;

/// Paxos Made Simple paper router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/*path", get(paxos_made_simple_handler))
        .route("/", get(paxos_made_simple_handler))
}
