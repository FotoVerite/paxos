# Why (Round, NodeId) Works

The paper requires that "Different proposers choose their numbers from disjoint sets of numbers."

## The Math
If we have 3 nodes (1, 2, 3).
*   **Node 1** can only use: 1, 4, 7, 10... (k * N + 1)
*   **Node 2** can only use: 2, 5, 8, 11... (k * N + 2)
*   **Node 3** can only use: 3, 6, 9, 12... (k * N + 3)

These sets are **disjoint** (no overlap).

## The Implementation
Using a tuple `(Round, NodeId)` is effectively the same thing but easier to read and debug.
*   Round 1, Node 1 -> `(1, 1)` -> Effectively "1.1"
*   Round 1, Node 2 -> `(1, 2)` -> Effectively "1.2"

Lexicographical sorting (compare Round first, then NodeId) preserves the requirement that higher rounds supersede lower rounds, and NodeId breaks ties within a round.

So yes, it **is** that simple in practice! Implementation often collapses abstract mathematical requirements into concrete structures like this.
