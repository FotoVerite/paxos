use paxos::{
    cluster::cluster::Cluster,
    console_observer::ConsoleObserver,
    scenario::ScenarioBuilder,
    scenario_loader::ScenarioLoader,
    scenario_runner::ScenarioRunner,
    web::server::run_web_server,
};
use std::{net::IpAddr, sync::Arc};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create logs directory
    std::fs::create_dir_all(".paxos/logs").ok();
    
    // Setup file appender (daily rotation)
    let file_appender = tracing_appender::rolling::daily(".paxos/logs", "server.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    
    // Initialize tracing with console and file output
    // Enable DEBUG logging with: RUST_LOG=debug cargo run web
    let level = if cfg!(debug_assertions) {
        std::env::var("RUST_LOG")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(tracing::Level::INFO)
    } else {
        tracing::Level::INFO
    };
    
    tracing_subscriber::fmt()
        .with_ansi(true)
        .with_max_level(level)
        .with_writer(non_blocking)
        .init();
    
    // Keep guard alive for the entire program
    let _guard = _guard;
    
    let args: Vec<String> = std::env::args().collect();

    if args.len() > 1 && args[1] == "web" {
        run_with_web_server().await
    } else if args.len() > 1 && args[1] == "json" {
        run_json_scenario().await
    } else {
        run_builtin_scenario().await
    }
}

async fn run_json_scenario() -> anyhow::Result<()> {
    println!("Loading scenarios from scenarios/ directory...\n");

    let scenarios = ScenarioLoader::load_all("scenarios").await?;

    if scenarios.is_empty() {
        println!("No scenarios found in scenarios/ directory");
        return Ok(());
    }

    for (filename, scenario) in scenarios {
        println!("Loaded: {}", filename);
        let ip = IpAddr::V4([127, 0, 0, 1].into());

        let node_count = scenario.node_count;
        let observer = Arc::new(ConsoleObserver);
        let mut cluster = Cluster::new(0, ip, node_count, observer).await?;

        for i in 0..node_count {
            cluster.nodes[i].start();
        }

        sleep(Duration::from_millis(100)).await;

        ScenarioRunner::run(&mut cluster, &scenario).await?;

        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}

async fn run_with_web_server() -> anyhow::Result<()> {
    println!("Starting Paxos with Web Visualizer...\n");
    println!("Open http://localhost:3000 in your browser\n");
    
    // Start web server in a background task
    tokio::spawn(async move {
        run_web_server().await;
    });

    // Give web server time to start
    sleep(Duration::from_millis(100)).await;

    // Keep the server running, waiting for clients to start scenarios
    println!("Waiting for client connections...");
    std::future::pending().await
}

async fn run_builtin_scenario() -> anyhow::Result<()> {
     println!("Starting Paxos cluster with programmatic scenario...\n");
         let ip = IpAddr::V4([127, 0, 0, 1].into());

     let node_count = 5;
     let observer = Arc::new(ConsoleObserver);
     let mut cluster = Cluster::new(0, ip, node_count, observer).await?;

    for i in 0..node_count {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;

    // Build scenario programmatically
    let scenario = ScenarioBuilder::new("Partition Recovery", node_count)
        .description("Tests Paxos recovery from partition")
        .phase("initialization")
        .enable_failures()
        .wait(Duration::from_millis(100))
        .phase("normal_operation")
        .propose(paxos::paxos_command::PaxosCommand::EnactDecree {
            author: "Socrates".to_string(),
            law: "Knowledge is virtue".to_string(),
        })
        .wait(Duration::from_millis(500))
        .phase("create_partition")
        .partition(0, 1)
        .partition(0, 2)
        .partition(0, 3)
        .partition(0, 4)
        .wait(Duration::from_millis(100))
        .phase("during_partition")
        .propose(paxos::paxos_command::PaxosCommand::AppointArchon {
            name: "Plato".to_string(),
            term_length_years: 5,
        })
        .wait(Duration::from_millis(500))
        .phase("heal_partition")
        .heal_partition(0, 1)
        .heal_partition(0, 2)
        .heal_partition(0, 3)
        .heal_partition(0, 4)
        .wait(Duration::from_millis(200))
        .phase("recovery")
        .propose(paxos::paxos_command::PaxosCommand::BuildAcropolis {
            stones_required: 1000,
            architect: "Ictinus".to_string(),
        })
        .wait(Duration::from_millis(500))
        .build();

    ScenarioRunner::run(&mut cluster, &scenario).await?;

    sleep(Duration::from_secs(1)).await;

    Ok(())
}