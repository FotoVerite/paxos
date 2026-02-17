use rand::Rng; // Import Rng trait

use crate::paxos_command::PaxosCommand;

/// A factory for creating whimsical, Greek-themed Paxos commands for simulations.
pub struct DecreeFactory {
    philosophers: Vec<&'static str>,
    citizens: Vec<&'static str>,
    architects: Vec<&'static str>,
    laws: Vec<&'static str>,
}

impl DecreeFactory {
    /// Creates a new factory with pre-populated thematic data.
    pub fn new() -> Self {
        Self {
            philosophers: vec![
                "Socrates",
                "Plato",
                "Aristotle",
                "Diogenes",
                "Aspasia",
                "Pericles",
            ],
            citizens: vec!["Meletus", "Anytus", "Lycon", "Cleon", "Alcibiades"],
            architects: vec!["Ictinus", "Callicrates", "Mnesikles", "Phidias"],
            laws: vec![
                "All citizens must wear their togas inside-out on Tuesdays.",
                "The official state bird is the owl, obviously.",
                "All statues must be given comically oversized hats.",
                "Mandatory philosophy discussions every full moon.",
                "The word 'fetch' is now to be replaced with 'yeet'.",
            ],
        }
    }

    /// Creates a random, whimsical decree.
    pub fn create_random_decree(&self) -> PaxosCommand {
        let mut rng = rand::rng();
        let command_type = rng.random_range(0..4);

        match command_type {
            0 => PaxosCommand::EnactDecree {
                author: self.philosophers[rng.random_range(0..self.philosophers.len())].to_string(),
                law: self.laws[rng.random_range(0..self.laws.len())].to_string(),
            },
            1 => PaxosCommand::Ostracize {
                citizen: self.citizens[rng.random_range(0..self.citizens.len())].to_string(),
            },
            2 => PaxosCommand::AppointArchon {
                name: self.philosophers[rng.random_range(0..self.philosophers.len())].to_string(),
                term_length_years: rng.random_range(1..11),
            },
            3 => PaxosCommand::BuildAcropolis {
                stones_required: rng.random_range(100..1001),
                architect: self.architects[rng.random_range(0..self.architects.len())].to_string(),
            },
            _ => PaxosCommand::NOOP,
        }
    }
}
