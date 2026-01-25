use axum::{
    Router,
    routing::{get, post},
};
use std::net::SocketAddr;
use tower_http::services::ServeDir;
use tracing::info;

use super::handlers::{
    AppState, multi_paxos::*, page::*, scenario::*, websocket::websocket_handler,
};

/// Run the web server on 0.0.0.0:3001
pub async fn run_web_server() {
    // Serve static files from the "static" directory
    let static_files_service = ServeDir::new("static");
    let app_state = AppState::new();

    let app = Router::new()
        .route("/", get(landing_handler))
        .route("/whitepapers", get(whitepapers_handler))
        .route("/leslie-lamport-biography", get(leslie_handler))
        .route("/publishing-history", get(setting_of_paper_handler))
        .route("/visualizer", get(visualizer_handler))
        .route("/senate", get(senate_handler))
        .route("/terms", get(terms_handler))
        .route("/overview", get(overview_handler))
        //section-1
        .route("/the-problem/the-island-of-paxos", get(tiop_handler))
        .route("/the-problem/requirements", get(requirements_handler))
        .route("/the-problem/assumptions", get(assumptions_handler))
        //section-2
        .route("/synod/overview", get(synod_overview_handler))
        .route("/synod/decree-example", get(synod_decree_handler))
        .route(
            "/synod/ballot-definitions",
            get(synod_ballot_definition_handler),
        )
        .route("/synod/math-terms", get(synod_math_terms_handler))
        .route("/synod/ballot", get(synod_ballot_handler))
        .route("/synod/ballot-number", get(synod_ballot_number_handler))
        .route("/synod/votes-function-b", get(synod_votes_function_handler))
        .route("/synod/max-vote", get(synod_max_vote_handler))
        .route("/synod/max-vote-quorum", get(synod_max_vote_quorum_handler))
        .route("/synod/theorem-one", get(synod_theorem_one_handler))
        .route("/synod/theorem-two", get(synod_theorem_two_handler))
        .route("/synod/recap", get(synod_recap_handler))
        //section 2.1-2.4
        .route(
            "/protocols/preliminary-protocol",
            get(protocol_preliminary_handler),
        )
        .route(
            "/protocols/preliminary-protocol-demo",
            get(preliminary_protocol_demo_handler),
        )
        .route("/protocols/basic-protocol", get(protocol_basic_handler))
        .route(
            "/protocols/basic-protocol-demo",
            get(basic_protocol_demo_handler),
        )
        .route(
            "/protocols/complete-protocol",
            get(complete_protocol_handler),
        )
        //section 3
        .route("/multi-decree-parliament/overview", get(m_p_overview))
        .route("/multi-decree-parliament/protocol", get(m_p_protocol))
        .route(
            "/multi-decree-parliament/ordering-of-decrees",
            get(m_p_ordering_decrees),
        )
        .route(
            "/multi-decree-parliament/optimizations",
            get(m_p_optimizations),
        )
        .route(
            "/further-developments/picking-a-president",
            get(f_d_picking_a_president),
        )
        .route("/further-developments/long-ledgers", get(f_d_long_ledgers))
        .route("/further-developments/bureaucrats", get(f_d_bureaucrats))
        .route(
            "/further-developments/learning-the-law",
            get(f_d_learning_the_law),
        )
        .route(
            "/further-developments/dishonest-legislators",
            get(f_d_dishonest_legislators),
        )
        .route(
            "/further-developments/choosing-new-legislators",
            get(f_d_choosing_new_legislators),
        )
        .route("/the_problem", get(the_problem_handler))
        .route("/ws", get(websocket_handler))
        .route("/api/start-scenario", post(start_scenario_handler))
        .route("/api/stop-scenario", post(stop_scenario_handler))
        //synod
        .route("/synod/constraints", get(synod_constraints_handler))
        .route("/synod/lemma", get(synod_lemma_handler))
        .route("/synod/quorum", get(synod_quorum_handler))
        .route("/api/propose", post(propose_handler))
        .fallback_service(static_files_service)
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    info!("Web server listening on http://0.0.0.0:3001");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}
