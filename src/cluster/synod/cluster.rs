use std::collections::HashMap;

use uuid::Uuid;

use super::{ClientAssignment, ClientId, SynodMembership};

#[derive(Debug)]
pub struct SynodCluster {
    assignments: HashMap<ClientId, ClientAssignment>,
    assignment_order: Vec<ClientId>,
}

impl SynodCluster {
    pub fn new() -> Self {
        Self {
            assignments: HashMap::new(),
            assignment_order: Vec::new(),
        }
    }

    pub fn assign_client(&mut self, client_id: Option<ClientId>) -> ClientAssignment {
        if let Some(client_id) = client_id {
            if let Some(assignment) = self.assignments.get(&client_id) {
                return assignment.clone();
            }
        }

        let assignment = ClientAssignment {
            client_id: ClientId::new(),
            node_id: Uuid::new_v4(),
        };

        self.assignment_order.push(assignment.client_id.clone());
        self.assignments
            .insert(assignment.client_id.clone(), assignment.clone());

        assignment
    }

    pub fn assignment_for(&self, client_id: &ClientId) -> Option<&ClientAssignment> {
        self.assignments.get(client_id)
    }

    pub fn assignment_count(&self) -> usize {
        self.assignments.len()
    }

    pub fn membership(&self) -> SynodMembership {
        let assignments = self
            .assignment_order
            .iter()
            .filter_map(|client_id| self.assignments.get(client_id))
            .cloned()
            .collect();

        SynodMembership {
            assignments,
            bootstrap_node: self.bootstrap_node(),
        }
    }

    pub fn bootstrap_node(&self) -> Option<Uuid> {
        self.assignment_order
            .first()
            .and_then(|client_id| self.assignments.get(client_id))
            .map(|assignment| assignment.node_id)
    }
}

impl Default for SynodCluster {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assigns_new_client_to_server_owned_node() {
        let mut cluster = SynodCluster::new();

        let assignment = cluster.assign_client(None);

        assert!(assignment.client_id.as_str().starts_with("c_"));
        assert_eq!(cluster.assignment_count(), 1);
        assert_eq!(
            cluster.assignment_for(&assignment.client_id),
            Some(&assignment)
        );
    }

    #[test]
    fn reuses_known_client_assignment() {
        let mut cluster = SynodCluster::new();
        let first = cluster.assign_client(None);

        let second = cluster.assign_client(Some(first.client_id.clone()));

        assert_eq!(second, first);
        assert_eq!(cluster.assignment_count(), 1);
    }

    #[test]
    fn unknown_client_gets_fresh_assignment() {
        let mut cluster = SynodCluster::new();

        let assignment = cluster.assign_client(Some(ClientId::from_existing("c_missing")));

        assert_ne!(assignment.client_id.as_str(), "c_missing");
        assert_eq!(cluster.assignment_count(), 1);
    }

    #[test]
    fn first_assignment_is_bootstrap_node() {
        let mut cluster = SynodCluster::new();
        let first = cluster.assign_client(None);
        let _second = cluster.assign_client(None);

        assert_eq!(cluster.bootstrap_node(), Some(first.node_id));
    }

    #[test]
    fn membership_reports_client_node_assignments() {
        let mut cluster = SynodCluster::new();
        let first = cluster.assign_client(None);
        let second = cluster.assign_client(None);

        let membership = cluster.membership();

        assert_eq!(membership.assignments, vec![first.clone(), second]);
        assert_eq!(membership.bootstrap_node, Some(first.node_id));
        assert_eq!(membership.node_count(), 2);
    }
}
