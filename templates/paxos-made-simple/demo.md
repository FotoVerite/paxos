# Partial Roles Demo

## Goal

Make the partial roles demo feel like part of `Paxos Made Simple`: direct, sharp, and easier to read on first contact.

Right now it works more like a generic protocol monitor with a lot of side furniture.
The main job is to teach role separation clearly, not to show every available instrument at once.

## Anti-Patterns Verdict

Fail.

The current shell still has several generic demo-dashboard tells:

- too many boxed sections with similar weight
- controls, stats, topology, decree panel, and log all visible at once
- role meaning mostly delegated to color chips and labels
- a lot of operational UI before the user has seen the core lesson

It is better than before, but it still reads like a reusable tooling surface more than a designed teaching page.

## Overall Impression

The strongest thing here is still the visualization itself.
The weakest thing is the surrounding shell: it does not tell the user what to look at first, and it spends a lot of space on secondary instrumentation.

The biggest opportunity is to make the demo clearly about one idea:

`different nodes can hold different Paxos roles, and consensus still works through those constraints.`

Everything in the page should support that point.

## What Is Working

- The demo has a real concept. Partial roles is not just another event replay; it has an actual teaching angle.
- The topology panel is useful in principle because role separation does need explicit explanation.
- The decree panel gives the learner side of the system a concrete payoff instead of stopping at message traffic.
- The new green color pass at least stops the page from looking like it belongs to a different paper.

## Priority Issues

### 1. The page has no clear first-read path

**What**

The page opens with topology, decree viewer, controls, status, stats, and event log all competing at once.

**Why it matters**

A first-time reader does not know what this demo is trying to prove.
That makes the whole experience feel heavier than it is.

**Fix**

Rebuild the shell around a simpler sequence:

- short framing block
- main visualization
- essential controls
- secondary instrumentation below or progressively revealed

Hide or mute everything that is not needed before the first run.

**Command**

`distill`

### 2. The role explanation is too dependent on color

**What**

The legend is a list of color swatches with role labels, and the topology panel repeats role counts as text.

**Why it matters**

This is the core concept of the demo, but the interface explains it in the weakest possible way.
Colors alone are not enough, especially now that the page wants to live inside the PMS green shell.

**Fix**

Use a stronger role language than raw color:

- role chips
- small structural marks
- tighter topology grouping
- clearer role summary

Color should support the roles, not carry the whole explanation.

**Command**

`clarify`

### 3. The controls are still too tool-like for the first run

**What**

Scenario, learning strategy, speed, layout toggle, play/pause/reset, stepping, and cursor are all presented as peers.

**Why it matters**

The demo feels like a control surface before it feels like a lesson.
That is backwards for this paper.

**Fix**

Make the first-run controls smaller and clearer:

- scenario
- play
- maybe speed

Everything else should be secondary, delayed, or tucked into a lighter control row.

**Command**

`distill`

### 4. The visualization is not visually dominant enough

**What**

The left and right side panels eat a lot of attention around the SVG, especially before anything is happening.

**Why it matters**

The graph is the thing users are meant to watch.
If the side panels visually compete with it, the page loses its center of gravity.

**Fix**

Reduce panel emphasis and treat them as supporting rails, not equal columns.
The visualization should feel like the main sentence. Everything else should feel like notes.

**Command**

`normalize`

### 5. Status and scenario language is still mechanical

**What**

The page uses labels like `Ready`, `Running`, `Processing`, `Complete`, plus scenario descriptions that explain the machinery more than the lesson.

**Why it matters**

The user needs cues about what to watch, not just the simulation lifecycle.

**Fix**

Rewrite the status area so it explains the current learning mode:

- what the current scenario is demonstrating
- what changed
- what the user should notice next

This should sound like PMS, not like a test harness.

**Command**

`clarify`

### 6. The topology panel is useful but badly packaged

**What**

It currently reads like a data dump: total nodes, role counts, role lists, quorum, strategy.

**Why it matters**

The information is good, but the presentation is noisy and hard to scan quickly.

**Fix**

Turn it into a concise role snapshot:

- count by role
- maybe node ids as chips
- quorum as one strong line
- strategy phrased more plainly

It should explain the setup in seconds.

**Command**

`distill`

### 7. Stats and event log are still over-prominent

**What**

The event counts and full log are always visible and occupy half the lower page.

**Why it matters**

They pull attention away from the actual role story.
They are useful after the user has a mental model, not before.

**Fix**

Reduce their default weight, or make one of them secondary until playback starts.
The first run should not open like a debugging console.

**Command**

`normalize`

### 8. The decree viewer needs a clearer reason to exist

**What**

It is present from the start, but its value depends on learner state and node selection.

**Why it matters**

Empty but visible teaching surfaces feel dead.

**Fix**

Either hide it until it becomes meaningful, or make it explicitly learner-focused with a stronger empty state.

**Command**

`onboard`

## Minor Observations

- The inline legend colors are still hard-coded in the template.
- The page still relies on the generic shared demo typography and spacing too much.
- `0.75x` as the default speed still biases the demo toward sluggishness.
- `Processing` is still a weak state name for this paper.
- `State Machine Demo` in the breadcrumb is too generic compared with the route’s actual point.
- The topology layout toggle is probably too visible for how secondary it is.

## Suggested Work Order

1. Distill the shell and reduce first-run clutter.
2. Clarify the teaching copy and state language.
3. Rebuild the topology explanation around roles rather than raw counts.
4. Rebalance the lower instrumentation.
5. Polish the visual language once the structure is right.

## Questions To Keep In Mind

- What is the single thing a user should understand after ten seconds?
- Does this need to look like a demo tool, or like a guided proof by example?
- Which panels are actually teaching, and which ones are just reporting?
- If the visualization is the main sentence, what can become punctuation?

## Scenario Direction

This demo should focus on what `Paxos Made Simple` makes newly visible here:

`role separation`

That means the scenarios should not re-teach the same protocol stories already covered in the Part-Time Parliament demos.
They should show what changes when proposers, acceptors, and learners are split across different nodes.

### Recommended Scenario Set

#### 1. Dedicated Acceptors

**Purpose**

Baseline scenario for separated roles.
Show proposers and learners interacting with an acceptor-only core.

**Selector Name**

`Dedicated Acceptors`

**Short Description**

`Proposers send requests into an acceptor-only quorum while learners stay off the write path.`

**Status Copy Direction**

- `Watch the acceptor core choose the value.`
- `Learners are present, but they are not deciding anything here.`

#### 2. Learners Off the Write Path

**Purpose**

Show that learning is downstream of choosing.
This is useful because it separates consensus from dissemination.

**Selector Name**

`Learners Off the Write Path`

**Short Description**

`Acceptors choose first. Learner-only nodes catch up afterward.`

**Status Copy Direction**

- `The quorum can choose before every learner hears about it.`
- `Watch the chosen value spread after the vote is already settled.`

#### 3. Competing Proposers, Shared Acceptor Core

**Purpose**

Keep the asymmetry idea, but make the teaching point explicitly about separated proposer nodes competing over the same acceptor set.

**Selector Name**

`Competing Proposers`

**Short Description**

`Two proposer nodes compete against the same acceptor core while learners stay separate.`

**Status Copy Direction**

- `The contention is at the proposer edge, not in the acceptor quorum.`
- `Watch the shared acceptor core force one path to win.`

#### 4. Mixed vs Dedicated Roles

**Purpose**

Directly compare all-in-one nodes with dedicated-role nodes.
This is probably the most important architecture scenario for this page.

**Selector Name**

`Mixed vs Dedicated`

**Short Description**

`Compare a cluster with mixed-role nodes to one with dedicated proposers, acceptors, and learners.`

**Status Copy Direction**

- `Same protocol, different topology.`
- `Watch how the role split changes who does what, not what safety requires.`

### Scenario To Remove

#### Reconfiguration & Recovery

This should leave this page.

It belongs to a later paper and muddies the point of the demo.
The partial roles page should stay about role separation, not membership change.

### Naming Notes

- Avoid names that sound like generic failure labs.
- Prefer topology names over algorithm names.
- Keep the scenario list readable without extra explanation.

### Good Naming Pattern

- `Dedicated Acceptors`
- `Learners Off the Write Path`
- `Competing Proposers`
- `Mixed vs Dedicated`

Short, concrete, and tied to the actual teaching goal.
