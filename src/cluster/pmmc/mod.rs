//! PMMC orchestration, control-plane, and reconfiguration code.

pub mod control;
pub mod process_manager;
pub mod reconfiguration;
pub mod system;

pub use process_manager::PmmcProcessManager;
pub use system::PmmcCluster;
