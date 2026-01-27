use axum::{Router, routing::get, response::Redirect};
use crate::web::handlers::AppState;

pub mod paxos_made_simple;

// Redirect trailing slash versions to non-trailing
async fn redirect_paxos_made_simple_trailing() -> Redirect {
    Redirect::permanent("/papers/paxos-made-simple")
}

/// Main papers router - nests all paper submodules
pub fn router() -> Router<AppState> {
    Router::new()
        .nest("/paxos-made-simple", paxos_made_simple::router())
        .route("/paxos-made-simple/", get(redirect_paxos_made_simple_trailing))
}
