use crate::cluster::cluster::Cluster;
use crate::common::persistence::Persistence;
use crate::common::types::DecreeId;
use crate::node::classic_paxos::acceptor::Acceptor;
use crate::node::classic_paxos::ledger::Ledger;
use crate::paxos_command::PaxosCommand;
use std::net::IpAddr;

pub struct CatchUpScenario;

impl CatchUpScenario {
    /// Setup initial state for catch-up scenario
    /// Node 0 starts with partial ledger [2, 4, 5], missing [0, 1, 3]
    /// Other nodes have full ledger [0-5]
    pub async fn setup(ip: IpAddr, node_count: usize) -> anyhow::Result<()> {
        let full_ledger = Self::create_full_ledger();
        let catching_up_ledger = Self::create_catching_up_ledger(&full_ledger);

        // Pre-populate ledger and acceptor state before cluster starts
        for node_id in 0..node_count {
            let node_uuid = Cluster::node_uuid(ip, node_id);
            let node_persistence = Persistence::cluster(ip).node(node_uuid);
            let ledger = if node_id == 0 {
                catching_up_ledger.clone()
            } else {
                full_ledger.iter().map(|cmd| Some(cmd.clone())).collect()
            };

            // Prepopulate ledger
            Ledger::prepopulate(&node_persistence, ledger.clone()).await?;

            // Prepopulate acceptor with the same decrees
            let initial_decrees: Vec<(DecreeId, PaxosCommand)> = ledger
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| opt.as_ref().map(|cmd| (DecreeId(idx), cmd.clone())))
                .collect();
            Acceptor::prepopulate(&node_persistence, initial_decrees).await?;
        }

        Ok(())
    }

    /// Propose to fill gaps in Node 0's ledger
    pub async fn propose_gap_filler(cluster: &mut Cluster, node_id: usize) -> anyhow::Result<()> {
        // Get the node's ledger to find gaps
        let gap_decree_num = if let Some(node) = cluster.nodes.get(node_id) {
            node.get_next_gap().await
        } else {
            None
        };

        if let Some(gap_decree_num) = gap_decree_num {
            let cmd = PaxosCommand::EnactDecree {
                author: "Olive Day".to_string(),
                law: "The ides of February is national olive day".to_string(),
            };
            let proposer_uuid = cluster.nodes[node_id].uuid;
            cluster
                .propose_from_with_decree_num(proposer_uuid, Some(gap_decree_num), cmd)
                .await;
        }

        Ok(())
    }

    fn create_full_ledger() -> Vec<PaxosCommand> {
        vec![
            PaxosCommand::EnactDecree {
                author: "Setup".to_string(),
                law: "Grant citizenship to foreign merchant Kallias".to_string(),
            },
            PaxosCommand::EnactDecree {
                author: "Setup".to_string(),
                law: "Authorize marble purchase for the Acropolis".to_string(),
            },
            PaxosCommand::EnactDecree {
                author: "Setup".to_string(),
                law: "Third decree".to_string(),
            },
            PaxosCommand::EnactDecree {
                author: "Setup".to_string(),
                law: "Fourth decree".to_string(),
            },
            PaxosCommand::EnactDecree {
                author: "Setup".to_string(),
                law: "Fifth decree".to_string(),
            },
            PaxosCommand::EnactDecree {
                author: "Setup".to_string(),
                law: "Sixth decree".to_string(),
            },
        ]
    }

    fn create_catching_up_ledger(full_ledger: &[PaxosCommand]) -> Vec<Option<PaxosCommand>> {
        vec![
            None,                         // [0] - missing
            None,                         // [1] - missing
            Some(full_ledger[2].clone()), // [2]
            None,                         // [3] - missing
            Some(full_ledger[4].clone()), // [4]
            Some(full_ledger[5].clone()), // [5]
        ]
    }
}
