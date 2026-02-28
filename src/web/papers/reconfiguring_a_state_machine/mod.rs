use crate::web::handlers::AppState;
use crate::web::handlers::tera_handler::reconfiguring_a_state_machine_handler;
use crate::web::subdomain::Subdomain;
use axum::{Router, http::Uri, routing::get};

/// Reconfiguring a State Machine tutorial router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/*path", get(reconfiguring_a_state_machine_handler))
        .route(
            "/",
            get(|uri: Uri, subdomain: Subdomain, state| {
                reconfiguring_a_state_machine_handler(uri, None, subdomain, state)
            }),
        )
}
