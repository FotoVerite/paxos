use uuid::Uuid;

use super::configuration::VerticalPaxosConfigError;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Quorum {
    members: Vec<Uuid>,
}

impl Quorum {
    pub fn new(members: Vec<Uuid>) -> Result<Self, VerticalPaxosConfigError> {
        if members.is_empty() {
            return Err(VerticalPaxosConfigError::EmptyQuorum);
        }
        if let Some(member) = first_duplicate(&members) {
            return Err(VerticalPaxosConfigError::DuplicateQuorumMember(member));
        }
        Ok(Self { members })
    }

    pub fn members(&self) -> &[Uuid] {
        &self.members
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn contains(&self, member: &Uuid) -> bool {
        self.members.contains(member)
    }
}

fn first_duplicate(members: &[Uuid]) -> Option<Uuid> {
    let mut seen = std::collections::HashSet::with_capacity(members.len());
    members.iter().copied().find(|member| !seen.insert(*member))
}
