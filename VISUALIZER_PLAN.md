# Paxos Visualizer Improvement Plan

## Current Understanding

The visualizer has two main parts:

1. **Static Scenarios** (`static/scenarios/*.js`)
   - Predefined educational walkthroughs (success, livelock, two-proposers, etc.)
   - Manual phase structure: `getPhases()` returns array of steps
   - Each step is an `action()` async function with handwritten visualization code
   - Directly calls visualizer methods (`drawBeamsTo`, `activateNode`, etc.)

2. **Live Demo** (`basic-protocol-demo.js`)
   - Real Paxos cluster visualization from live backend events
   - WebSocket receives actual Paxos events (Proposal, Promise, Accept, Accepted, Learn)
   - Event queue with batching/throttling (50µs windows)
   - Maps each event to visualization (`visualizeProposal`, `visualizePromise`, etc.)
   - Tracks partition state, learned decrees per node, node selection

**Key Insight**: These are two completely different use cases - educational demos vs live cluster monitoring. The visualizer (`paxos-visualizer.js`) is doing fine for the rendering part, but the glue code is messy.

---

## The Real Problems

### 1. Scenario Code is Repetitive & Brittle
- 7 scenario files with similar structure but no shared patterns
- Each phase manually handles node state, colors, beams, event logging
- Copy-paste errors easy (look at `success.js` - inconsistent node ranges)
- Hard to reorder/remix phases

### 2. Event Handler is a Giant Monolith
- `basic-protocol-demo.js` is 682 lines of imperative event handling
- `visualizeProposal`, `visualizePromise`, `visualizeAccept`, etc. are 50+ lines each
- Partition state scattered (separate tracking, partitionedNodes Set)
- Decree tracking (nodeDecrees) mixed with visualization
- Event formatting, visualization, and UI updates all tangled together

### 3. No Abstraction Between Events & Visualization
- Each event type has its own `visualize*` function
- Hard to add new visualization or change existing ones
- Color definitions still scattered (eventColorMap, colors.js)
- Event filtering/transformation happens in handler - could be in mapper

### 4. State Management Spread Across Global Variables
```javascript
let clusterInfo = null;           // Cluster metadata
let nodeDecrees = {};             // Per-node decrees (for decree panel)
let selectedNodeId = null;        // Currently selected node
let partitionedNodes = new Set(); // Partitioned nodes
let eventCounts = {};             // Event counters
let eventQueue = [];              // Event batching queue
let isRunning = false;            // Simulation state
```

---

## Proposed Solution (Minimal, Pragmatic)

Focus on **extracting repeatable patterns** without a full rewrite. No React, no complex frameworks.

### 1. Scenario Helper Library (50 lines)
Replace boilerplate in scenario files:

```javascript
// static/scenario-helpers.js
export class ScenarioPhase {
  constructor(visualizer, utils) {
    this.v = visualizer;
    this.u = utils;
  }

  async run(actions) {
    for (const action of actions) await action();
  }

  resetNodes(ids, color = '#3b82f6') {
    return async () => {
      ids.forEach(id => {
        this.v.setNodeState(id, '--');
        this.v.setNodeColor(id, color);
      });
      this.v.clearBeams();
    };
  }

  nodeActivate(id, state, color) {
    return async () => {
      this.v.setNodeState(id, state);
      this.v.activateNode(id, color);
    };
  }

  beamsTo(from, to, color, dur = 500, stagger = 80) {
    return async () => {
      await this.v.drawBeamsTo(from, to, color, dur, 'solid', stagger);
    };
  }

  beamsFrom(from, to, color, dur = 500, stagger = 150) {
    return async () => {
      await this.v.drawBeamsFrom(from, to, color, dur, 'dashed', stagger);
    };
  }

  log(msg, color) {
    return async () => this.u.addEvent(msg, color);
  }

  wait(ms = 300) {
    return async () => this.u.sleep(ms);
  }

  increment(counter) {
    return async () => {
      this.u.eventCounts[counter]++;
      this.u.updateCounts();
    };
  }
}
```

**Usage in scenarios**:
```javascript
const scenarioSuccess = {
  name: "Clean Success",
  getPhases(colors, utils) {
    const phase = new ScenarioPhase(visualizer, utils);
    return [
      {
        title: "Step 1: NextBallot(b)",
        action: () => phase.run([
          phase.resetNodes([0,1,2,3,4,5,6]),
          phase.nodeActivate(0, 'propose', colors.nextballot),
          phase.log('[NextBallot] Node 0 sends ballot 100', colors.nextballot),
          phase.beamsTo(0, [1,2,3,4,5,6], colors.nextballot),
          phase.increment('nextballot'),
          phase.wait(300),
        ])
      },
      // ... more phases
    ];
  }
};
```

**Benefit**: Reduces each scenario phase from 15-20 lines to 5-8, eliminates repetition, makes it easier to remix phases.

---

### 2. Event Visualizer Registry (100 lines)
Instead of 50-line `visualizeProposal()`, `visualizePromise()` functions, create a declarative registry:

```javascript
// static/event-visualizers.js
const EVENT_VISUALIZERS = {
  Proposal: {
    color: '#60a5fa',
    name: 'NextBallot',
    format: (e) => `[NextBallot] Node ${e.id}: "${formatDecree(e)}"`,
    async visualize(event, color, visualizer, clusterInfo, canCommunicate) {
      visualizer.setNodeState(event.id, 'propose');
      visualizer.activateNode(event.id, color);
      const beams = [];
      for (let i = 0; i < clusterInfo.total_nodes; i++) {
        if (i !== event.id && canCommunicate(event.id, i)) {
          beams.push(visualizer.drawBeam(event.id, i, color, 350, 'solid'));
        }
      }
      await Promise.all(beams);
    }
  },

  Promise: {
    color: '#ec4899',
    name: 'LastVote',
    format: (e) => `[LastVote] Node ${e.id} → Node ${e.from}: Ballot ${e.ballot}`,
    async visualize(event, color, visualizer, clusterInfo, canCommunicate) {
      visualizer.setNodeState(event.id, 'promise');
      visualizer.activateNode(event.id, color);
      if (event.from !== undefined && event.from !== event.id) {
        await visualizer.drawBeam(event.id, event.from, color, 350, 'dashed');
      }
    }
  },

  Accept: {
    color: '#f87171',
    name: 'BeginBallot',
    format: (e) => `[BeginBallot] Node ${e.id}: Ballot ${e.ballot}, Decree #${e.decree_num}`,
    async visualize(event, color, visualizer, clusterInfo, canCommunicate) {
      visualizer.setNodeState(event.id, 'accept');
      visualizer.activateNode(event.id, color);
      const beams = [];
      if (event.quorum && Array.isArray(event.quorum)) {
        for (const nodeId of event.quorum) {
          if (nodeId !== event.id) {
            beams.push(visualizer.drawBeam(event.id, nodeId, color, 350, 'solid'));
          }
        }
      }
      await Promise.all(beams);
    }
  },

  Accepted: {
    color: '#10b981',
    name: 'Voted',
    format: (e) => `[Voted] Node ${e.id} → Node ${e.from}: Ballot ${e.ballot}`,
    async visualize(event, color, visualizer, clusterInfo, canCommunicate) {
      visualizer.setNodeState(event.id, 'voted');
      visualizer.activateNode(event.id, color);
      if (event.from !== undefined && event.from !== event.id) {
        await visualizer.drawBeam(event.id, event.from, color, 350, 'dashed');
      }
    }
  },

  LearnedValue: {
    color: '#34d399',
    name: 'Learned',
    format: (e) => {
      const decree = e.value?.EnactDecree?.law || `Decree #${e.decree_num}`;
      return `[Learned] Node ${e.id}: "${decree}"`;
    },
    async visualize(event, color, visualizer, clusterInfo, canCommunicate) {
      visualizer.setNodeState(event.id, 'learn');
      visualizer.activateNode(event.id, color);
    }
  },

  // Add others as needed...
};

export function getEventVisualizer(eventType) {
  return EVENT_VISUALIZERS[eventType];
}
```

**Usage in event handler**:
```javascript
// OLD - 50+ lines per visualizer function
async function visualizeProposal(event, color) { /* ... */ }

// NEW - one function for all events
async function visualizeEvent(eventType, event, visualizer, clusterInfo, canCommunicate) {
  const viz = getEventVisualizer(eventType);
  if (viz) {
    await viz.visualize(event, viz.color, visualizer, clusterInfo, canCommunicate);
  }
}
```

Then in `processEventQueue()`:
```javascript
// Replace the big switch statement with:
for (const { eventType, event } of batch) {
  const viz = getEventVisualizer(eventType);
  if (viz) {
    // Format and log
    addEvent(viz.format(event), viz.color);
    // Track counts
    eventCounts[eventType]++;
    // Visualize
    await visualizeEvent(eventType, event, visualizer, clusterInfo, canCommunicate);
  }
}
```

**Benefit**: Reduces 300+ lines of event handler boilerplate to 150 lines. Easier to modify/add event visualizations.

---

### 3. Simple State Container (50 lines)
Centralize the scattered global state:

```javascript
// static/demo-state.js
class DemoState {
  constructor() {
    this.cluster = null;
    this.nodes = new Map(); // id -> { state, partitioned, decrees[] }
    this.simulation = {
      running: false,
      speed: 1.0,
      selectedNode: null,
    };
    this.events = [];
    this.listeners = new Set();
  }

  initialize(clusterInfo) {
    this.cluster = clusterInfo;
    for (let i = 0; i < clusterInfo.total_nodes; i++) {
      this.nodes.set(i, {
        state: '--',
        partitioned: false,
        decrees: [],
      });
    }
    this.notify();
  }

  setNodeState(id, state) {
    const node = this.nodes.get(id);
    if (node) {
      node.state = state;
      this.notify();
    }
  }

  setNodePartitioned(id, partitioned) {
    const node = this.nodes.get(id);
    if (node) {
      node.partitioned = partitioned;
      this.notify();
    }
  }

  addDecree(nodeId, decree) {
    const node = this.nodes.get(nodeId);
    if (node) {
      node.decrees.unshift(decree);
      this.notify();
    }
  }

  setSimulationRunning(running) {
    this.simulation.running = running;
    this.notify();
  }

  selectNode(id) {
    this.simulation.selectedNode = this.simulation.selectedNode === id ? null : id;
    this.notify();
  }

  subscribe(callback) {
    this.listeners.add(callback);
  }

  notify() {
    this.listeners.forEach(cb => cb(this.getSnapshot()));
  }

  getSnapshot() {
    return {
      cluster: this.cluster,
      nodes: new Map(this.nodes),
      simulation: { ...this.simulation },
    };
  }
}

export const state = new DemoState();
```

**Usage**:
```javascript
// Subscribe to changes
state.subscribe((snapshot) => {
  updateDecreePanel(snapshot.nodes.get(snapshot.simulation.selectedNode));
});

// Update state
state.setNodeState(0, 'propose');
state.setNodePartitioned(3, true);
```

**Benefit**: Single source of truth, easier to debug, easier to add new state tracking.

---

### 4. Event Processing Refactor (100 lines saved)
Extract event batching into separate module:

```javascript
// static/event-queue.js
export class EventQueue {
  constructor(batchWindow = 50) { // microseconds
    this.queue = [];
    this.batchWindow = batchWindow;
    this.processing = false;
  }

  push(event) {
    this.queue.push(event);
    // Debounce processing
    if (this.processingTimeout) clearTimeout(this.processingTimeout);
    this.processingTimeout = setTimeout(() => this.process(), 5);
  }

  async process() {
    if (this.processing || this.queue.length === 0) return;
    this.processing = true;

    // Batch events by timestamp
    const batch = this.batch();
    
    // Process all in parallel
    await Promise.all(batch.map(e => this.handleEvent(e)));

    this.processing = false;

    // Continue if more events
    if (this.queue.length > 0) {
      setTimeout(() => this.process(), 100);
    }
  }

  batch() {
    const batch = [];
    const start = this.queue[0].timestamp;
    
    while (this.queue.length > 0 && this.queue[0].timestamp - start < this.batchWindow) {
      batch.push(this.queue.shift());
    }
    
    return batch;
  }

  // Override in subclass
  async handleEvent(event) {
    // To be implemented
  }
}
```

Then use in demo:
```javascript
class PaxosEventQueue extends EventQueue {
  constructor(visualizer, state, clusterInfo) {
    super();
    this.visualizer = visualizer;
    this.state = state;
    this.clusterInfo = clusterInfo;
  }

  async handleEvent(event) {
    const { eventType, eventData } = event;
    const viz = getEventVisualizer(eventType);
    
    if (viz) {
      // Format and log
      addEvent(viz.format(eventData), viz.color);
      // Visualize
      await viz.visualize(eventData, viz.color, this.visualizer, this.clusterInfo);
      // Track state
      this.state.events.push({ type: eventType, data: eventData });
    }
  }
}
```

---

## File Changes Summary

### Create (New)
- `static/scenario-helpers.js` (50 lines)
- `static/event-visualizers.js` (150 lines)
- `static/demo-state.js` (80 lines)
- `static/event-queue.js` (80 lines)

### Refactor (Existing)
- `basic-protocol-demo.js`: 682 → 250 lines
  - Remove event handler boilerplate
  - Use EVENT_VISUALIZERS registry
  - Use EventQueue
  - Use DemoState
  
- `static/scenarios/*.js`: Reduce repetition
  - Use ScenarioPhase helpers
  - ~20-30% less code per scenario

### Keep As-Is
- `paxos-visualizer.js` (719 lines) - working fine
- Rust backend - no changes needed
- HTML templates - no changes needed

---

## Benefits

| Aspect | Before | After |
|--------|--------|-------|
| **Scenario Code Reuse** | None | ~40% reduction via ScenarioPhase |
| **Event Handler Size** | 682 lines | 250 lines |
| **Event Visualizers** | 7× 50-line functions | 1× 150-line registry |
| **State Management** | 5 globals scattered | 1 State class, clear API |
| **Adding New Event Type** | 50+ lines new code | ~20 lines in registry |
| **Modifying Visualization** | Search multiple files | One registry file |

---

## Implementation Order

### Phase 1: State Container
1. Create `demo-state.js`
2. Replace global variables with `state.*` calls
3. Test that everything still works

### Phase 2: Event Visualizer Registry
1. Create `event-visualizers.js` with existing visualization logic
2. Refactor `basic-protocol-demo.js` event handler to use registry
3. Delete old `visualize*()` functions

### Phase 3: Scenario Helpers
1. Create `scenario-helpers.js`
2. Update scenario files to use ScenarioPhase
3. Profit from reduced boilerplate

### Phase 4: Event Queue Refactor
1. Create `event-queue.js`
2. Integrate into demo

**Total Time**: ~1 week for one developer (part-time)

---

## Key Differences from Original Plan

✓ **No component extraction** - `paxos-visualizer.js` is already clean
✓ **No React/Vue** - Vanilla JS, it's fine
✓ **No animation engine rewrite** - Current RAF-based beams work well
✓ **No massive refactor** - Incremental improvements only
✓ **Focus on the right problem** - Boilerplate reduction, not architecture

The visualizer is **not the problem**. The problem is the glue code around it.

