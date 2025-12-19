# The Part-Time Parliament

**Leslie Lamport**  
Digital Equipment Corporation

*This article appeared in ACM Transactions on Computer Systems 16, 2 (May 1998), 133-169. Minor corrections were made on 29 August 2000.*

---

## Editor's Note

This submission was recently discovered behind a filing cabinet in the TOCS editorial office. Despite its age, the editor-in-chief felt that it was worth publishing. Because the author is currently doing field work in the Greek isles and cannot be reached, I was asked to prepare it for publication.

The author appears to be an archeologist with only a passing interest in computer science. This is unfortunate; even though the obscure ancient Paxon civilization he describes is of little interest to most computer scientists, its legislative system is an excellent model for how to implement a distributed computer system in an asynchronous environment. Indeed, some of the refinements the Paxons made to their protocol appear to be unknown in the systems literature.

The author does give a brief discussion of the Paxon Parliament's relevance to distributed computing in Section 4. Computer scientists will probably want to read that section first. Even before that, they might want to read the explanation of the algorithm for computer scientists by Lampson [1996]. The algorithm is also described more formally by De Prisco et al. [1997].

**Keith Marzullo**  
University of California, San Diego

---

## Abstract

Recent archaeological discoveries on the island of Paxos reveal that the parliament functioned despite the peripatetic propensity of its part-time legislators. The legislators maintained consistent copies of the parliamentary record, despite their frequent forays from the chamber and the forgetfulness of their messengers. The Paxon parliament's protocol provides a new way of implementing the state-machine approach to the design of distributed systems.

---

## 1 The Problem

### 1.1 The Island of Paxos

Early in this millennium, the Aegean island of Paxos was a thriving mercantile center. Wealth led to political sophistication, and the Paxons replaced their ancient theocracy with a parliamentary form of government. But trade came before civic duty, and no one in Paxos was willing to devote his life to Parliament. The Paxon Parliament had to function even though legislators continually wandered in and out of the parliamentary Chamber.

The problem of governing with a part-time parliament bears a remarkable correspondence to the problem faced by today's fault-tolerant distributed systems, where:
- Legislators correspond to processes
- Leaving the Chamber corresponds to failing

The Paxons' solution may therefore be of some interest to computer scientists.

Paxon civilization was destroyed by a foreign invasion, and archaeologists have just recently begun to unearth its history. Our knowledge of the Paxon Parliament is therefore fragmentary. Although the basic protocols are known, we are ignorant of many details. Where such details are of interest, I will take the liberty of speculating on what the Paxons might have done.

### 1.2 Requirements

Parliament's primary task was to determine the law of the land, which was defined by a sequence of decrees. A decree was a command that any citizen could obey.

Parliament had to satisfy two requirements:

1. **Consistency**: All citizens obey the same law.
2. **Progress**: A decree is eventually passed (barring catastrophe).

The problem of ensuring both consistency and progress in a parliament with part-time legislators is essentially the problem of implementing a distributed system that must tolerate the failure of some of its processes.

---

## 2 The Synod

The Paxon Parliament established a council of priests, called the Synod, charged with the task of determining the value of decrees. A single legislator might propose a decree, and the Synod would decide whether to accept or reject it.

### 2.1 Mathematical Results

The Synod's decree was chosen through a series of numbered **ballots**, where a ballot was a referendum on a single decree. In each ballot, a priest had the choice only of voting for the decree or not voting.

Associated with a ballot was a set of priests called a **quorum**. A ballot succeeded iff every priest in the quorum voted for the decree.

Formally, a ballot *B* consisted of the following four components:

- **B.dec**: A decree (the one being voted on)
- **B.qrm**: A nonempty set of priests (the ballot's quorum)
- **B.vot**: A set of priests (the ones who cast votes for the decree)
- **B.bal**: A ballot number

A ballot *B* was said to be **successful** iff *B.qrm ⊆ B.vot*, so a successful ballot was one in which every quorum member voted.

#### Ballot Numbers

Ballot numbers were chosen from an unbounded ordered set of numbers. If *B'.bal > B.bal*, then ballot *B'* was said to be **later** than ballot *B*. However, this indicated nothing about the order in which ballots were conducted; a later ballot could actually have taken place before an earlier one.

#### Three Conditions for Consistency

Paxon mathematicians defined three conditions on a set *B* of ballots, and then showed that consistency was guaranteed and progress was possible if the set of ballots that had taken place satisfied those conditions:

**B1(B)**: Each ballot in *B* has a unique ballot number.

**B2(B)**: The quorums of any two ballots in *B* have at least one priest in common.

**B3(B)**: For every ballot *B* in *B*, if any priest in *B*'s quorum voted in an earlier ballot in *B*, then the decree of *B* equals the decree of the latest of those earlier ballots.

### 2.2 The Preliminary Protocol

The preliminary Synod protocol allowed priests to conduct multiple ballots concurrently. The protocol consisted of several steps:

#### Phase 1: Prepare Phase
1. The initiator *p* of a ballot selects a ballot number *b*
2. He sends a "prepare" message with ballot number *b* to a subset of priests
3. Each priest, upon receiving this message, records the ballot number

#### Phase 2: Accept Phase
4. Priests who recorded the ballot number send back their previous votes (if any)
5. If the initiator receives responses containing more than half the priests, they received from priests promising not to vote in any ballot numbered between their previous ballot and *b*
6. The initiator can now send an "accept" message with a decree and the ballot number
7. Priests who promised to participate vote for this decree

#### Phase 3: Learn Phase
8. The decree is learned once all priests in the quorum have voted

### 2.3 The Basic Protocol

The basic protocol simplifies the preliminary protocol by having the initiator *p* conduct only one ballot at a time—ballot number *lastTried[p]*. After *p* initiates this ballot, he ignores messages that pertain to any other ballot that he had previously initiated.

The key distinction from the preliminary protocol:

- In the preliminary protocol, each *LastVote(b, v)* message represents a promise not to vote in any ballot numbered between *v.bal* and *b*
- In the basic protocol, it represents the stronger promise not to cast a new vote in any ballot numbered less than *b*

### 2.4 Dealing with Forgotten Information

A priest might fail and lose all information stored in his ledger. When he recovers, he doesn't remember any of the ballots in which he participated. This could violate the B3 condition.

The Paxons solved this by having priests write their promises to a slip of paper. Each priest kept a slip of paper on which he recorded:
- The ballot number he had agreed to participate in
- The decree he had voted for

If he lost this slip of paper, he could no longer participate in that ballot.

---

## 3 The Multi-Decree Parliament

When Parliament was established, a protocol to satisfy its consistency and progress requirements was derived from the Synod protocol. Instead of passing just one decree, the Paxon Parliament had to pass a series of numbered decrees.

### 3.1 The Parliamentary Protocol

As in the Synod protocol, a president was elected. Anyone who wanted a decree passed would inform the president, who would assign a number to the decree and attempt to pass it.

Logically, the parliamentary protocol used a separate instance of the complete Synod protocol for each decree number. However, a single president was selected for all these instances, and he performed the first two steps of the protocol just once.

#### Key Optimization

The key to deriving the parliamentary protocol is the observation that in the Synod protocol, the president does not choose the decree or the quorum until step 3. This meant:

1. The president could execute steps 1-2 just once
2. For each decree number, the president only needed to execute steps 3-6
3. This reduced the number of message rounds needed per decree

#### Parliament vs. Synod

| Aspect | Synod | Parliament |
|--------|-------|-----------|
| Decrees | Single decree | Multiple numbered decrees |
| President | Elected per ballot | Single president for all decrees |
| First phase | Repeated per decree | Executed once for all decrees |
| Efficiency | Higher message overhead | Lower message overhead per decree |

### 3.2 Properties of the Protocol

The parliamentary protocol maintained the same consistency and progress properties as the Synod protocol:

- **Consistency**: All legislators eventually agree on the value of each decree
- **Progress**: Decrees are eventually passed (assuming the president and a quorum of legislators don't fail)

### 3.3 Evolution of the Protocol

The Paxons made several refinements to their protocol over time:

#### 3.3.1 Dealing with Failures

The Paxons discovered that their protocol could tolerate:
- Failed legislators (who stopped responding)
- Failed messengers (who lost or delayed messages)
- Legislators joining and leaving Parliament

#### 3.3.2 President Selection

A key challenge was electing a new president if the current president failed. The Paxons used a protocol where legislators agreed on a president through a Synod ballot.

#### 3.3.3 Delegation

The Paxons used **delegation** to allow a legislator who was leaving the Chamber to delegate his voting power to another legislator. This maintained the effectiveness of the quorum.

#### 3.3.4 Monotonicity

To ensure that previously passed decrees remained stable, the Paxons required that decree numbers be monotonically increasing. This prevented new ballots from undoing previously agreed-upon decrees.

#### 3.3.5 Learning Decrees

The Paxons had a method for ensuring all legislators eventually learned the value of each decree:

1. Once a decree is passed, the president informs all legislators
2. Legislators who miss this announcement ask other legislators
3. A legislator who learns of a decree records it in his ledger

#### 3.3.6 Adding New Legislators

The Paxons could add new legislators to Parliament while it was running:

1. A new legislator was added by Synod ballot
2. Existing legislators informed the new legislator of all previously passed decrees
3. The new legislator participated in future ballots

---

## 4 Relevance to Computer Science

### 4.1 The State Machine Approach

Although Paxos's Parliament was destroyed many centuries ago, its protocol is still useful for modern distributed systems.

#### Example: Distributed Database System

Consider a simple distributed database system that might be used as a name server:

**System Components:**
- A state of the database consists of an assignment of values to names
- Copies of the database are maintained by multiple servers
- A client program can issue requests to any server

**Request Types:**
- **Slow read**: Returns the current value assigned to a name
- **Fast read**: Faster but might not reflect a recent change
- **Update**: Changes the value assigned to a name

#### Parliament ↔ Database Correspondence

| Parliamentary Term | Database Term |
|--------------------|---------------|
| Legislator | Server |
| Citizen | Client program |
| Current law | Database state |
| Decree | Command (read/update) |
| Consistency requirement | All replicas have same state |
| Progress requirement | Operations eventually complete |

### 4.2 Implementing the State Machine

Using the Paxon protocol:

1. **Clients submit commands** to any server
2. **The protocol ensures** all servers eventually execute commands in the same order
3. **Each server maintains** an identical state machine
4. **Clients learn results** once a quorum has executed the command

### 4.3 Comparison to Other Protocols

#### Three-Phase Commit

The Paxon Synod protocol is related to three-phase commit protocols:
- Both involve multiple rounds of messages
- The Synod protocol allows failures during the voting phase
- Three-phase commit assumes a coordinator, while Synod is more flexible

#### Relationship to Other Results

The theorems on which the Synod protocol is based are similar to results obtained by Dwork, Lynch, and Stockmeyer. However, their algorithms execute ballots sequentially in separate rounds, and they seem to be unrelated to the Synod protocol.

---

## 5 Conclusion

The Paxon Parliament's protocol demonstrates that it is possible for a distributed system to:

1. Maintain consistency among replicated state despite failures
2. Make progress in the face of arbitrary failures (except total loss of a quorum)
3. Handle processors that join and leave the system
4. Tolerate asynchronous message delivery

These properties make the protocol valuable for building reliable distributed systems.

### Modern Applications

The Paxon protocol and its variants are used in:
- Google's Chubby lock service
- Google's BigTable distributed database
- Etcd (Kubernetes configuration management)
- Apache Zookeeper
- Consensus algorithms in blockchain systems

### Future Work

Much research remains to be done in applying the Paxon protocols to real systems, particularly in:
- Handling network partitions
- Optimizing message complexity
- Enabling safe membership changes
- Providing efficient read operations

---

## References

- De Prisco, R., Lampson, B., Lynch, N. (1997). "Revisiting the Paxos Algorithm." MIT LCS Technical Memo TM-632.
- Dwork, C., Lynch, N., Stockmeyer, L. (1988). "Consensus in the Presence of Partial Synchrony." Journal of the ACM 35(2), 288-323.
- Fischer, M. J., Lynch, N. A., Paterson, M. S. (1985). "Impossibility of Distributed Consensus with One Faulty Process." Journal of the ACM 32(2), 374-382.
- Gray, C., Cheriton, D. (1989). "Leases: An Efficient Fault-Tolerant Mechanism for Distributed File Cache Consistency." ACM SIGOPS Operating Systems Review 23(5), 202-210.
- Keidar, I., Dolev, D. (1996). "Increasing the Resilience of Distributed Protocols." Journal of Computer and System Sciences 53(2), 141-173.
- Lampson, B. (1996). "How to Build a Highly Available System Using Consensus." In Operating Systems Design and Implementation, 1-17.
- Ladin, R., Liskov, B., Shrira, L., Ghemawat, S. (1992). "Providing High Availability and Fault Tolerance in Weakly Replicated Data." ACM Transactions on Computer Systems 10(4), 299-338.
- Oki, B. M., Liskov, B. H. (1988). "Viewstamped Replication: A New Primary Copy Model for Distributed Systems." In Proc. 7th Symposium on Principles of Distributed Computing, 8-17.
- Schneider, F. B. (1990). "Implementing Fault-Tolerant Services Using the State Machine Approach: A Tutorial." ACM Computing Surveys 22(4), 299-319.

---

## Appendix: Key Algorithms

### A1. Basic Protocol Variables

Each priest *p* must maintain the following variables:

**In Ledger (persistent):**
- **outcome[p]**: The decree written in *p*'s ledger, or blank if nothing is written yet
- **lastTried[p]**: The number of the last ballot that *p* tried to begin, or -∞ if none
- **prevBal[p]**: The number of the last ballot in which *p* voted, or -∞ if never
- **prevDec[p]**: The decree for which *p* last voted, or blank if never voted
- **nextBal[p]**: The number of the last ballot in which *p* agreed to participate, or -∞ if never

**On Slip of Paper (can be lost):**
- **status[p]**: The current status of the ballot being conducted
- Messages being sent or received

### A2. Protocol Steps

#### Step 1: Prepare
Priest *p* selects a ballot number *b* > *lastTried[p]* and sends a prepare request to a set of priests.

#### Step 2: Promise
Each priest receiving the prepare request:
- If *b* > *nextBal[p]*, sets *nextBal[p] = b* and responds with a promise
- Response includes *prevBal[p]* and *prevDec[p]*

#### Step 3: Propose
Priest *p* selects a decree and sends an accept request:
- If all respondents had *prevBal = -∞*, *p* can propose any decree
- Otherwise, *p* must propose the decree from the highest *prevBal*

#### Step 4: Accept
Each priest receiving an accept request:
- If *b* ≥ *nextBal[p]*, sets *prevBal[p] = b*, *prevDec[p] = decree*, and votes
- Sends an accepted message

#### Step 5: Learn
Once *p* receives votes from a quorum of priests:
- The decree is chosen
- *p* sends learn messages to all priests

#### Step 6: Accept Learn
Each priest receiving a learn message:
- Sets *outcome[p] = decree*

---

## Additional Notes for Computer Scientists

### Understanding the Protocol

The core insight of Paxos is that **safety and liveness can be decoupled**:

- **Safety** (consistency): All processes agree on the same value
- **Liveness** (progress): A value is eventually chosen

By separating these concerns, the protocol achieves both even when:
- Processes can fail and restart
- Messages can be lost or delayed
- The network can partition temporarily

### Key Properties

1. **Only one value is ever chosen** (consistency)
2. **A process only learns a value that could have been chosen** (safety)
3. **If a value is chosen and learning is complete, clients eventually learn it** (liveness)

### Practical Considerations

When implementing Paxos in practice, consider:
- **Quorum selection**: How many processes must agree?
- **Message optimization**: Can we reduce the number of rounds?
- **Failure detection**: How do we detect when a leader has failed?
- **Membership changes**: How do we add/remove processes safely?
- **Persistence**: What must be written to stable storage?

---

*This markdown adaptation is based on the original paper by Leslie Lamport, "The Part-Time Parliament," ACM Transactions on Computer Systems, 1998.*
