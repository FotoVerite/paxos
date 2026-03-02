use paxos::{
    cluster::{cluster::Cluster, cluster_runtime::ClusterRuntime},
    console_observer::ConsoleObserver,
    node::config::{PmmcNodeConfig, Roles},
    scenario::ScenarioBuilder,
    scenario_loader::ScenarioLoader,
    scenario_runner::ScenarioRunner,
    web::server::run_web_server,
};
use std::{net::IpAddr, sync::Arc};
use tokio::time::{Duration, sleep};
use uuid::Uuid;

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
    } else if args.len() > 1 && args[1] == "pmmc" {
        run_pmmc_builtin_scenario().await
    } else if args.len() > 1 && args[1] == "pmmc-role-split" {
        run_pmmc_role_split_debug_scenario(true).await
    } else if args.len() > 1 && args[1] == "pmmc-role-split-sticky" {
        run_pmmc_role_split_debug_scenario(false).await
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
    println!("Open http://localhost:3001 in your browser\n");

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

async fn run_pmmc_builtin_scenario() -> anyhow::Result<()> {
    println!("Starting PMMC cluster with basic console scenario...\n");
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let observer = Arc::new(ConsoleObserver);
    let cluster = ClusterRuntime::new(ip, 5, observer).await?;

    cluster.start_all().await;
    sleep(Duration::from_millis(250)).await;

    println!("--- PMMC Phase: single client proposal ---");
    cluster
        .propose_from(
            0,
            paxos::paxos_command::PaxosCommand::PUT {
                key: "alpha".to_string(),
                version: 1,
                value: 1,
            },
        )
        .await;
    sleep(Duration::from_secs(1)).await;

    println!("--- PMMC Phase: partition and recovery ---");
    cluster.enable_failures().await;
    cluster.partition(0, 1).await;
    cluster.partition(0, 2).await;
    sleep(Duration::from_millis(250)).await;
    cluster
        .propose_from(
            0,
            paxos::paxos_command::PaxosCommand::PUT {
                key: "beta".to_string(),
                version: 1,
                value: 2,
            },
        )
        .await;
    sleep(Duration::from_millis(750)).await;
    cluster.heal_partition(0, 1).await;
    cluster.heal_partition(0, 2).await;
    sleep(Duration::from_secs(1)).await;
    println!("=== PMMC Scenario Complete ===");
    Ok(())
}

async fn run_pmmc_role_split_debug_scenario(alternate_replicas: bool) -> anyhow::Result<()> {
    println!(
        "Starting PMMC role-split debug scenario (2L, 2R, 3A) mode={}...\n",
        if alternate_replicas {
            "alternate replicas"
        } else {
            "sticky replica"
        }
    );
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let observer = Arc::new(ConsoleObserver);
    let mut configs = Vec::new();
    for _ in 0..2 {
        configs.push(PmmcNodeConfig {
            roles: Roles {
                proposer: true,
                acceptor: false,
                learner: false,
            },
        });
    }
    for _ in 0..2 {
        configs.push(PmmcNodeConfig {
            roles: Roles {
                proposer: false,
                acceptor: false,
                learner: true,
            },
        });
    }
    for _ in 0..3 {
        configs.push(PmmcNodeConfig {
            roles: Roles {
                proposer: false,
                acceptor: true,
                learner: false,
            },
        });
    }

    let cluster = ClusterRuntime::new_with_configs(ip, configs, observer).await?;
    cluster.start_all().await;
    sleep(Duration::from_millis(350)).await;

    println!(
        "--- Phase 1: replica proposals ({}) ---",
        if alternate_replicas {
            "RR over replica nodes N2/N3"
        } else {
            "single replica node N2"
        }
    );
    for i in 0..8usize {
        let replica = if alternate_replicas {
            if i % 2 == 0 { 2 } else { 3 }
        } else {
            2
        };
        cluster
            .propose_from(
                replica,
                paxos::paxos_command::PaxosCommand::PUT {
                    key: "pmmc-role-split".to_string(),
                    version: 1,
                    value: i + 1,
                }
                .with_client(Uuid::from_u128(0xC0), i as u64 + 1),
            )
            .await;
        sleep(Duration::from_millis(350)).await;
    }

    println!("--- Phase 2: observe for liveness (no new proposals, just protocol traffic) ---");
    sleep(Duration::from_secs(4)).await;
    println!("=== PMMC Role-Split Debug Scenario Complete ===");
    Ok(())
}
