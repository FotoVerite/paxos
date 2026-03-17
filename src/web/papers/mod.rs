use crate::web::handlers::AppState;
use axum::{Router, extract::Path, response::Redirect, routing::get};

pub mod paxos_made_moderately_complex;
pub mod paxos_made_simple;
pub mod reconfiguring_a_state_machine;
pub mod vertical_paxos;

// Redirect trailing slash versions to non-trailing
async fn redirect_paxos_made_simple_trailing() -> Redirect {
    Redirect::permanent("/papers/paxos-made-simple")
}

async fn redirect_paxos_made_moderately_complex_trailing() -> Redirect {
    Redirect::permanent("/papers/paxos-made-moderately-complex")
}

async fn redirect_reconfiguring_a_state_machine_trailing() -> Redirect {
    Redirect::permanent("/papers/reconfiguring-a-state-machine")
}

async fn redirect_reconfiguring_state_machine_underscore() -> Redirect {
    Redirect::permanent("/papers/reconfiguring-a-state-machine")
}

async fn redirect_reconfiguring_state_machine_underscore_path(
    Path(path): Path<String>,
) -> Redirect {
    Redirect::permanent(&format!("/papers/reconfiguring-a-state-machine/{}", path))
}

async fn redirect_reconfiguring_state_machine_no_article() -> Redirect {
    Redirect::permanent("/papers/reconfiguring-a-state-machine")
}

async fn redirect_reconfiguring_state_machine_no_article_path(
    Path(path): Path<String>,
) -> Redirect {
    Redirect::permanent(&format!("/papers/reconfiguring-a-state-machine/{}", path))
}

async fn redirect_vertical_paxos_trailing() -> Redirect {
    Redirect::permanent("/papers/vertical-paxos")
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
        .nest(
            "/reconfiguring-a-state-machine",
            reconfiguring_a_state_machine::router(),
        )
        .route(
            "/reconfiguring-a-state-machine/",
            get(redirect_reconfiguring_a_state_machine_trailing),
        )
        .route(
            "/reconfiguring_state_machine",
            get(redirect_reconfiguring_state_machine_underscore),
        )
        .route(
            "/reconfiguring_state_machine/",
            get(redirect_reconfiguring_state_machine_underscore),
        )
        .route(
            "/reconfiguring_state_machine/*path",
            get(redirect_reconfiguring_state_machine_underscore_path),
        )
        .route(
            "/reconfiguring-state-machine",
            get(redirect_reconfiguring_state_machine_no_article),
        )
        .route(
            "/reconfiguring-state-machine/",
            get(redirect_reconfiguring_state_machine_no_article),
        )
        .route(
            "/reconfiguring-state-machine/*path",
            get(redirect_reconfiguring_state_machine_no_article_path),
        )
        .nest("/vertical-paxos", vertical_paxos::router())
        .route("/vertical-paxos/", get(redirect_vertical_paxos_trailing))
}
