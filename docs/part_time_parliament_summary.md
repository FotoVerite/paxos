# The Part-Time Parliament - Summary

Based on the paper by Leslie Lamport (1998).

## The Metaphor
*   **Context:** Data consistency in a distributed system is mapped to lawmaking in an ancient Greek parliament on the island of Paxos.
*   **Legislators:** Processes/Nodes.
*   **Chamber:** The network/system.
*   **Leaving/Entering Chamber:** Node Failure/Recovery.
*   **Messenger:** Network packets (unreliable, can be lost/delayed, but not corrupted).
*   **Ledgers:** Stable storage (disk). Legislators write in indelible ink.
*   **Decrees:** Consensus values.

## Key Concepts

### The Synod Protocol
The core consensus mechanism (Single-Decree Paxos).
*   **Requirement:** Consistency of ledgers. No two ledgers can contain contradictory decrees.
*   **Quorums:** A set of legislators. Any two quorums must have at least one member in common. This ensures overlap in voting.

### Progress (Liveness)
*   To pass a decree, a quorum of legislators must be present in the chamber for a sufficient time.
*   If legislators keep entering/leaving too fast, no decree might be passed (dueling proposers/livelock).
*   **Distinguished Proposer (Leader):** To avoid livelock, a single legislator is often elected to be the sole proposer.

## Practical Implications for Us
1.  **Stable Storage is Critical:** "Writing in specific ledger spots with indelible ink" == `fsync`. We must survive crashes.
2.  **Monotonic Ballots:** "Decree numbers" must be strictly increasing.
3.  **Read-Modify-Write:** The "Prepare" phase effectively reads the state of the quorum before refining the proposal in the "Accept" phase.
