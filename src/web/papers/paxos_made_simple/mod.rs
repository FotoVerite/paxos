use axum::{
    Router,
    routing::get,
};
use crate::web::handlers::AppState;

mod handlers;

use handlers::*;

/// Paxos Made Simple paper router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(index_handler))
        .route("/abstract", get(abstract_handler))
        .route("/introduction", get(introduction_handler))
        .route("/consensus", get(consensus_handler))
        .route("/the-problem", get(the_problem_handler))
        .route("/choosing-value", get(choosing_value_handler))
        .route("/learning-value", get(learning_value_handler))
        .route("/progress", get(progress_handler))
        .route("/implementation", get(implementation_handler))
        .route("/state-machine", get(state_machine_handler))
}
