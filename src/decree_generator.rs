/// Generates interesting, thematic decree values for Paxos proposals
#[derive(Clone)]
pub struct DecreeGenerator {
    decrees: Vec<String>,
    index: usize,
}

impl DecreeGenerator {
    pub fn new() -> Self {
        let decrees = vec![
            // Athenian governance theme
            "Grant citizenship to foreign merchant Kallias".to_string(),
            "Authorize marble purchase for the Acropolis".to_string(),
            "Exile tyrant sympathizer Megakles".to_string(),
            "Commission new bronze statue for Athena".to_string(),
            "Tax wealthy landholders for military funds".to_string(),
            "Regulate grain price in agora".to_string(),
            "Mandate education curriculum reforms".to_string(),
            "Fortify walls of Piraeus harbor".to_string(),
            "Allow women to own property rights".to_string(),
            "Establish court for maritime disputes".to_string(),
            "Grant monopoly on tin trade".to_string(),
            "Create new festival for Poseidon".to_string(),
            "Implement jury selection by lottery".to_string(),
            "Expand naval fleet by 20 triremes".to_string(),
            "Pardon exiled political enemies".to_string(),
            "Fund road construction to Sparta border".to_string(),
            "Appoint new generals for campaign".to_string(),
            "Restrict foreign merchants from voting".to_string(),
            "Build aqueduct to outlying demes".to_string(),
            "Declare sacred truce for Olympic games".to_string(),
            "Consecrate new temple to Demeter".to_string(),
            "Levy tax on wine imports".to_string(),
            "Restrict theater attendance by slaves".to_string(),
            "Grant sanctuary to Egyptian refugees".to_string(),
            "Mandate training period for hoplites".to_string(),
        ];

        Self {
            decrees,
            index: 0,
        }
    }

    /// Get next decree in sequence (round-robin)
    pub fn next(&mut self) -> String {
        let decree = self.decrees[self.index % self.decrees.len()].clone();
        self.index += 1;
        decree
    }

    /// Get decree by ID (for consistency across nodes)
    pub fn by_id(&self, id: usize) -> String {
        self.decrees[id % self.decrees.len()].clone()
    }
}
