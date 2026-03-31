use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    common::persistence::NodePersistence,
    common::types::DecreeId,
    monitor::{Event, PaxosObserver, current_timestamp_millis},
    node::{
        classic_paxos::{
            acceptor::Acceptor, decree_notes::DecreeNotes, learner::Learner, ledger::Ledger,
            message::ClassicMessage, proposer::proposer::Proposer,
            router::{ClassicComponent, ClassicRouter},
            transport::ClassicHandle,
        },
        config::ClassicNodeConfig,
        inflight_proposals::{InflightProposal, InflightProposals},
        routing::{LocalRoutingDecision, ProtocolInboxRouter, ProtocolRouter, RoutingDecision},
    },
    paxos_command::PaxosCommand,
};

pub struct PaxosState {
    uuid: Uuid,
    proposer: Option<Proposer>,
    acceptor: Option<Acceptor>,
    learner: Option<Learner>,
    ledger: Ledger,
    peers: Arc<ClassicHandle>,
    inflight_proposals: Arc<InflightProposals>,
    router: ClassicRouter,
    observer: Arc<dyn PaxosObserver>,
}

impl PaxosState {
    pub async fn init(
        uuid: Uuid,
        quorum: usize,
        peers: Arc<ClassicHandle>,
        persistence: NodePersistence,
        inflight_proposals: Arc<InflightProposals>,
        observer: Arc<dyn PaxosObserver>,
        config: ClassicNodeConfig,
        topology: crate::node::peer_topology::PeerTopology,
    ) -> anyhow::Result<Self> {
        let ledger = Ledger::init(uuid, persistence.clone()).await?;
        let decree_notes = Arc::new(Mutex::new(DecreeNotes::load_or_init(&persistence).await?));

        let proposer = if config.roles.proposer {
            Some(Proposer::new(
                uuid,
                quorum,
                persistence.clone(),
                Arc::clone(&decree_notes),
                Arc::clone(&observer),
            ))
        } else {
            None
        };

        let acceptor = if config.roles.acceptor {
            Some(Acceptor::new(uuid, persistence.clone(), Arc::clone(&observer)).await?)
        } else {
            None
        };

        // Learner gets decree_notes only if node has proposer role
        let learner_decree_notes = if config.roles.proposer {
            Some(Arc::clone(&decree_notes))
        } else {
            None
        };

        let learner = if config.roles.learner {
            Some(Learner::new(
                uuid,
                quorum,
                learner_decree_notes,
                Arc::clone(&observer),
            ))
        } else {
            None
        };

        // Emit capabilities event
        let mut roles_str = Vec::new();
        if config.roles.proposer {
            roles_str.push("Proposer".to_string());
        }
        if config.roles.acceptor {
            roles_str.push("Acceptor".to_string());
        }
        if config.roles.learner {
            roles_str.push("Learner".to_string());
        }

        observer.on_event(Event::NodeCapabilities {
            id: uuid,
            roles: roles_str,
            learning_strategy: format!("{:?}", config.learning_strategy),
        });

        let router = ClassicRouter::new(config.learning_strategy.clone(), topology);

        let state = Self {
            uuid,
            proposer,
            acceptor,
            learner,
            ledger,
            peers,
            inflight_proposals,
            router,
            observer,
        };
        state.emit_ledger_state().await;
        Ok(state)
    }

    pub async fn propose(
        &self,
        cmd: PaxosCommand,
        decree_num: Option<DecreeId>,
    ) -> InflightProposal {
        let num = match decree_num {
            Some(num) => num,
            None => self.next().await,
        };

        if let Some(proposer) = &self.proposer {
            let msg = proposer.propose(num, cmd.clone()).await;
            self.route_and_send(&msg, self.uuid).await;
            self.inflight_proposals.insert(num, cmd.clone()).await
        } else {
            tracing::warn!(
                "[Node {}] Attempted to propose without Proposer role",
                self.uuid
            );
            // Return a dummy inflight proposal or handle error appropriately.
            // For now, we'll insert it but nothing will happen network-wise.
            self.inflight_proposals.insert(num, cmd.clone()).await
        }
    }

    pub async fn retry_proposal(&self, inflight: InflightProposal) {
        if let Some(proposer) = &self.proposer {
            let msg = proposer
                .propose(inflight.decree_num, inflight.cmd.clone())
                .await;
            self.route_and_send(&msg, self.uuid).await;
        }
    }

    pub async fn next(&self) -> DecreeId {
        self.ledger.next().await
    }

    pub async fn get_next_gap(&self) -> Option<DecreeId> {
        self.ledger.next_gap().await
    }

    pub async fn handle_message(&self, classic_msg: ClassicMessage) {
        let from = match &classic_msg {
            ClassicMessage::Prepare { from, .. }
            | ClassicMessage::Promise { from, .. }
            | ClassicMessage::Accept { from, .. }
            | ClassicMessage::Accepted { from, .. }
            | ClassicMessage::Success { from, .. }
            | ClassicMessage::PrepareBatch { from, .. } => *from,
        };

        match self.router.classify(&classic_msg, &()) {
            LocalRoutingDecision::DeliverTo(ClassicComponent::Proposer) => {
                if let Some(proposer) = &self.proposer {
                    tracing::debug!("[Node {}] Handling Promise message", self.uuid);
                    if let Some(reply) = proposer.handle_message(classic_msg.clone()).await {
                        self.route_and_send(&reply, from).await;
                    }
                }
            }
            LocalRoutingDecision::DeliverTo(ClassicComponent::Acceptor) => {
                if let Some(acceptor) = &self.acceptor {
                    tracing::debug!(
                        "[Node {}] Handling Prepare/Accept from node {}",
                        self.uuid,
                        from
                    );
                    if let Some(reply) = acceptor.handle_message(classic_msg.clone()).await {
                        self.route_and_send(&reply, from).await;
                    }
                }
            }
            LocalRoutingDecision::DeliverTo(ClassicComponent::Learner) => {
                if let Some(learner) = &self.learner {
                    match &classic_msg {
                        ClassicMessage::Accepted { .. } => {
                            tracing::debug!("[Node {}] Handling Accepted message", self.uuid);
                            let reply = learner
                                .handle_message(classic_msg.clone(), &self.ledger)
                                .await;
                            if let Some(reply @ ClassicMessage::Success { .. }) = reply {
                                tracing::debug!(
                                    "[Node {}] Learner reached quorum, broadcasting Success",
                                    self.uuid
                                );
                                self.route_and_send(&reply, from).await;
                            }
                        }
                        ClassicMessage::Success { .. } => {
                            tracing::debug!("[Node {}] Handling Success message", self.uuid);
                            learner
                                .learn_decree(classic_msg.clone(), &self.ledger)
                                .await;
                        }
                        _ => {}
                    }
                }
            }
            LocalRoutingDecision::Drop => {
                tracing::debug!("[Node {}] Ignoring unhandled message type", self.uuid);
            }
        }

        if let ClassicMessage::Success { decree_num, .. } = &classic_msg {
            self.inflight_proposals.cancel(*decree_num).await;
        }
    }

    async fn route_and_send(&self, msg: &ClassicMessage, from: Uuid) {
        let decision = self.router.route(msg, &from);
        match decision {
            RoutingDecision::Broadcast => {
                self.peers.broadcast(msg.clone()).await;
            }
            RoutingDecision::SendTo(to) => {
                self.peers.send(to, msg.clone()).await;
            }
            RoutingDecision::SendToMany(nodes) => {
                for node in nodes {
                    self.peers.send(node, msg.clone()).await;
                }
            }
            RoutingDecision::Drop => {}
        }
    }

    pub async fn emit_ledger_state(&self) {
        let initial_decrees = self.ledger.get_initial_decrees().await;
        for (decree_num, value) in initial_decrees {
            self.observer.on_event(Event::InitialDecree {
                id: self.uuid,
                decree_num,
                value,
                created_at: current_timestamp_millis(),
            });
        }
    }

    pub async fn emit_ledger_batch(&self) {
        let initial_decrees = self.ledger.get_initial_decrees().await;
        if !initial_decrees.is_empty() {
            self.observer.on_event(Event::BatchInitialDecrees {
                id: self.uuid,
                decrees: initial_decrees,
                created_at: current_timestamp_millis(),
            });
        }
    }

    pub async fn emit_full_ledger(&self) {
        let all_decrees = self.ledger.get_all_decrees().await;
        if !all_decrees.is_empty() {
            self.observer.on_event(Event::LedgerDump {
                id: self.uuid,
                decrees: all_decrees,
                created_at: current_timestamp_millis(),
            });
        }
    }
}
