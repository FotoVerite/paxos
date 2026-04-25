use uuid::Uuid;

use crate::common::ballot::Ballot;

use super::quorum::Quorum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VerticalPaxosVariant {
    V1,
    V2,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VerticalClusterConfiguration {
    id: Uuid,
    leader: Uuid,
    variant: VerticalPaxosVariant,
    ballot: Ballot,
    start_index: usize,
    acceptors: Vec<Uuid>,
    replicas: Vec<Uuid>,
    read_quorum: Quorum,
    write_quorum: Quorum,
    complete_bal: Option<Ballot>,
}

impl VerticalClusterConfiguration {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Uuid,
        leader: Uuid,
        variant: VerticalPaxosVariant,
        ballot: Ballot,
        start_index: usize,
        acceptors: Vec<Uuid>,
        replicas: Vec<Uuid>,
        read_quorum: Quorum,
        write_quorum: Quorum,
        complete_bal: Option<Ballot>,
    ) -> Result<Self, VerticalPaxosConfigError> {
        validate_acceptors(&acceptors)?;
        if let Some(member) = read_quorum
            .members()
            .iter()
            .find(|member| !acceptors.contains(member))
        {
            return Err(VerticalPaxosConfigError::ReadQuorumOutsideAcceptors(
                *member,
            ));
        }
        if let Some(member) = write_quorum
            .members()
            .iter()
            .find(|member| !acceptors.contains(member))
        {
            return Err(VerticalPaxosConfigError::WriteQuorumOutsideAcceptors(
                *member,
            ));
        }
        if let Some(complete_bal) = complete_bal {
            if complete_bal >= ballot {
                return Err(VerticalPaxosConfigError::CompleteBallotNotEarlier {
                    complete_bal,
                    ballot,
                });
            }
        }

        Ok(Self {
            id,
            leader,
            variant,
            ballot,
            start_index,
            acceptors,
            replicas,
            read_quorum,
            write_quorum,
            complete_bal,
        })
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn leader(&self) -> Uuid {
        self.leader
    }

    pub fn variant(&self) -> VerticalPaxosVariant {
        self.variant
    }

    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    pub fn start_index(&self) -> usize {
        self.start_index
    }

    pub fn acceptors(&self) -> &[Uuid] {
        &self.acceptors
    }

    pub fn replicas(&self) -> &[Uuid] {
        &self.replicas
    }

    pub fn read_quorum(&self) -> &Quorum {
        &self.read_quorum
    }

    pub fn write_quorum(&self) -> &Quorum {
        &self.write_quorum
    }

    pub fn complete_bal(&self) -> Option<Ballot> {
        self.complete_bal
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerticalPaxosConfigError {
    EmptyAcceptors,
    EmptyQuorum,
    DuplicateAcceptor(Uuid),
    DuplicateQuorumMember(Uuid),
    ReadQuorumOutsideAcceptors(Uuid),
    WriteQuorumOutsideAcceptors(Uuid),
    CompleteBallotNotEarlier {
        complete_bal: Ballot,
        ballot: Ballot,
    },
}

fn validate_acceptors(acceptors: &[Uuid]) -> Result<(), VerticalPaxosConfigError> {
    if acceptors.is_empty() {
        return Err(VerticalPaxosConfigError::EmptyAcceptors);
    }
    if let Some(member) = first_duplicate(acceptors) {
        return Err(VerticalPaxosConfigError::DuplicateAcceptor(member));
    }
    Ok(())
}

fn first_duplicate(members: &[Uuid]) -> Option<Uuid> {
    let mut seen = std::collections::HashSet::with_capacity(members.len());
    members.iter().copied().find(|member| !seen.insert(*member))
}

#[cfg(test)]
mod tests;
