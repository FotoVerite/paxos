//! Vertical Paxos currently reuses the PMMC message vocabulary.
//!
//! Keep the local alias so vertical code reads cleanly. If the protocol
//! diverges later, this is the seam where a distinct message type returns.

pub type VerticalPaxosMessage = crate::node::pmmc::message::PmmcMessage;
