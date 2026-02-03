use axum::{Router, routing::get};
use crate::web::handlers::tera_handler::paxos_made_moderately_complex_handler;
use crate::web::handlers::AppState;

/// Paxos Made Moderately Complex paper router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/*path", get(paxos_made_moderately_complex_handler))
        .route("/", get(paxos_made_moderately_complex_handler))
}
