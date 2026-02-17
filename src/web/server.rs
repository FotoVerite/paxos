use axum::{
    Router,
    routing::{get, post},
};
use std::net::SocketAddr;
use tower_http::services::{ServeDir, ServeFile};
use tracing::info;

use super::handlers::{
    AppState, paxos_handler, paxos_made_simple_handler, scenario::*, websocket::websocket_handler,
};
use super::papers;

// Redirect / to the paxos paper
// async fn root_redirect() -> Redirect {
//     Redirect::permanent("/papers/paxos-made-simple")
// }

/// Run the web server on 0.0.0.0:3001
pub async fn run_web_server() {
    let app_state = AppState::new();

    // Main app router
    let app = Router::new()
        // API Routes (Specific)
        .route("/api/start-scenario", post(start_scenario_handler))
        .route("/api/stop-scenario", post(stop_scenario_handler))
        .route("/api/propose", post(propose_handler))
        .route("/ws", get(websocket_handler))
        // Papers Sub-router
        .nest("/papers", papers::router())
        // Static Files (Explicit Priority)
        .nest_service("/css", ServeDir::new("static/css"))
        .nest_service("/js", ServeDir::new("static/js"))
        .nest_service("/svgs", ServeDir::new("static/svgs"))
        .nest_service("/scenarios", ServeDir::new("static/scenarios"))
        // Root Static Files
        .route_service("/favicon.svg", ServeFile::new("static/favicon.svg"))
        .route_service("/logo.svg", ServeFile::new("static/logo.svg"))
        .route_service("/style.css", ServeFile::new("static/style.css"))
        // Legacy JS files at root (should ideally be in /js)
        .route_service(
            "/basic-protocol-demo.js",
            ServeFile::new("static/basic-protocol-demo.js"),
        )
        .route_service(
            "/decree-simulator.js",
            ServeFile::new("static/decree-simulator.js"),
        )
        .route_service(
            "/paxos-visualizer.js",
            ServeFile::new("static/paxos-visualizer.js"),
        )
        .route_service(
            "/websocket-helper.js",
            ServeFile::new("static/websocket-helper.js"),
        )
        .route_service(
            "/scenario-loader.js",
            ServeFile::new("static/scenario-loader.js"),
        )
        .route_service(
            "/animated-nodes.js",
            ServeFile::new("static/animated-nodes.js"),
        )
        // Refactored modules
        .route_service("/demo-state.js", ServeFile::new("static/demo-state.js"))
        .route_service(
            "/event-visualizers.js",
            ServeFile::new("static/event-visualizers.js"),
        )
        .route_service(
            "/scenario-helpers.js",
            ServeFile::new("static/scenario-helpers.js"),
        )
        .route_service("/event-queue.js", ServeFile::new("static/event-queue.js"))
        // Pms Handler
        .route(
            "/paxos-made-simple",
            get(|uri, subdomain, state| paxos_made_simple_handler(uri, None, subdomain, state)),
        )
        .route(
            "/paxos-made-simple/",
            get(|uri, subdomain, state| paxos_made_simple_handler(uri, None, subdomain, state)),
        )
        .route("/paxos-made-simple/*path", get(paxos_made_simple_handler))
        // Fallback to Tera Handler for everything else (Pages)
        // This catches /, /overview, /whitepapers, etc.
        .fallback(paxos_handler)
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
