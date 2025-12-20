# Historical Notes on Paxos and Distributed Systems

## Paxos Origin and Lamport’s Motivation

> “A fault‑tolerant file system called Echo was built at SRC in the late 80s… I decided that what they were trying to do was impossible, and set out to prove it. Instead, I discovered the Paxos algorithm.”  
> — Leslie Lamport, [The Part‑Time Parliament](https://www.microsoft.com/en-us/research/publication/part-time-parliament/)

- **Echo** was a fault-tolerant file system built at SRC that motivated Lamport to formalize consensus protocols.
- Paxos emerged from Lamport trying to **prove impossibility** of certain consensus problems but instead discovering a correct algorithm.

---

## Three‑Phase Commit and Dale Skeen

> “Dale Skeen seems to have been the first to have recognized the need for a three‑phase protocol to avoid blocking in the presence of an arbitrary single failure.”  
> — Leslie Lamport, [The Part‑Time Parliament](https://www.microsoft.com/en-us/research/publication/part-time-parliament/)

- Skeen identified the **blocking problem in 2PC** and motivated 3PC to avoid it.
- Lamport cites this as part of the historical backdrop for Paxos’ design.

---

## Petal and Frangipani: Early Implementations

> “When Ed Lee and I were working on Petal we needed some sort of commit protocol… We knew about 3PC… Leslie gave Ed a copy of the Part‑Time Parliament tech report… Paxos had all the necessary properties… Leslie provided essential consulting help… the first implementation of the Paxos algorithm (including dynamic reconfiguration)… a year later… we used Paxos again.”  
> — Chandu Thekkath & Leslie Lamport, [The Part‑Time Parliament](https://www.microsoft.com/en-us/research/publication/part-time-parliament/)

- **Petal**: Distributed virtual disk system.  
- **Frangipani**: Distributed file system using Paxos for lock management and consistency.  
- Paxos’ practical applicability was **verified in these production-style systems**.

---

## Greek Metaphor & Naming

> “I decided to cast the algorithm in terms of a parliament on an ancient Greek island… Leo Guibas suggested the name Paxos… People reading the paper got so distracted by the Greek parable that they didn’t understand the algorithm.”  
> — Leslie Lamport, [The Part‑Time Parliament](https://www.microsoft.com/en-us/research/publication/part-time-parliament/)

- The **allegory** made the original paper more readable (or more confusing), depending on the audience.
- Paxos = **fictional parliament on the island of Paxos**.

---

## Other Primary Sources

| Topic | Link |
|-------|------|
| Paxos Made Simple | [Microsoft Research](https://www.microsoft.com/en-us/research/publication/paxos-made-simple/) |
| Three-Phase Commit Protocol | [Wikipedia](https://en.wikipedia.org/wiki/Three-phase_commit_protocol) |
| Paxos History & Greek Naming | [Wikipedia](https://en.wikipedia.org/wiki/Paxos_(computer_science)) |
| Echo / historical context | [KTH Lecture Slides](https://www.csc.kth.se/utbildning/kth/kurser/DD2451/pardis11/DD2451_lecture9.pdf) |
| Petal Paper (1996) | [Petal: Distributed Virtual Disks PDF](https://www.cs.princeton.edu/courses/archive/spring99/cs598e/papers/petal.pdf) |
