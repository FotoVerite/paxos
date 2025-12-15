# Paxos Made Simple - Summary & Spec

Based on the paper by Leslie Lamport (2001).

## Goal
Achieve consensus on a single value among a group of unreliable processors.
**Safety Guarantee:** Only a value that has been proposed may be chosen, only a single value is chosen, and a process never learns that a value has been chosen unless it actually has been.

## Roles
*   **Proposers:** Initiate proposals.
*   **Acceptors:** Vote on proposals. (Our "stable storage").
*   **Learners:** Discover the chosen value.

## The Algorithm (Single Decree)

### Phase 1: Prepare
1.  **Proposer:** Selects a proposal number `n` and sends a `Prepare(n)` request to a majority of acceptors.
2.  **Acceptor:** Upon receiving `Prepare(n)`:
    *   If `n > min_proposal` (the highest proposal number usually seen so far):
        *   Set `min_proposal = n`.
        *   Reply with `Promise(n, accepted_proposal, accepted_value)`.
            *   `accepted_proposal`: The highest proposal number accepted so far (or null).
            *   `accepted_value`: The value associated with `accepted_proposal` (or null).
    *   Else (optional): Ignore or reply with a NACK.

### Phase 2: Accept
1.  **Proposer:** When receiving `Promise(n, ...)` from a majority:
    *   Select value `v`:
        *   If any acceptor returned an `accepted_value`, `v` must be the value associated with the *highest* `accepted_proposal` returned.
        *   Otherwise, `v` can be any new value.
    *   Send `Accept(n, v)` request to the majority.
2.  **Acceptor:** Upon receiving `Accept(n, v)`:
    *   If `n >= min_proposal`:
        *   Set `accepted_proposal = n`.
        *   Set `accepted_value = v`.
        *   Reply with `Accepted(n, v)` (or notify Learners).
    *   Else: Ignore.

### Phase 3: Learn (Not strictly specified, but common impl)
*   **Acceptor:** When a proposal is accepted, send `Accepted(n, v)` to all Learners (or a distinguished learner).
*   **Learner:** When receiving `Accepted(n, v)` from a majority of acceptors, the value `v` is chosen.

## Message Types (Implementation Spec)

```rust
enum Message {
    Prepare { proposal_id: usize },
    Promise { proposal_id: usize, prev_accepted_id: Option<usize>, prev_accepted_val: Option<String> },
    AcceptRequest { proposal_id: usize, value: String }, // "Accept" in paper
    Accepted { proposal_id: usize, value: String },
}
```
