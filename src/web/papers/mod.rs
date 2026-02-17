use crate::web::handlers::AppState;
use axum::{Router, response::Redirect, routing::get};

pub mod paxos_made_moderately_complex;
pub mod paxos_made_simple;

// Redirect trailing slash versions to non-trailing
async fn redirect_paxos_made_simple_trailing() -> Redirect {
    Redirect::permanent("/papers/paxos-made-simple")
}

async fn redirect_paxos_made_moderately_complex_trailing() -> Redirect {
    Redirect::permanent("/papers/paxos-made-moderately-complex")
}

/// Main papers router - nests all paper submodules
pub fn router() -> Router<AppState> {
    Router::new()
        .nest("/paxos-made-simple", paxos_made_simple::router())
        .route(
            "/paxos-made-simple/",
            get(redirect_paxos_made_simple_trailing),
        )
        .nest(
            "/paxos-made-moderately-complex",
            paxos_made_moderately_complex::router(),
        )
        .route(
            "/paxos-made-moderately-complex/",
            get(redirect_paxos_made_moderately_complex_trailing),
        )
}
