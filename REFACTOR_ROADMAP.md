# Visualizer Refactor & Role Improvements Roadmap

## Phase 1: Foundation Refactor (Keep Everything Working)

Implement the 4 modules from VISUALIZER_PLAN.md. All existing functionality stays the same, we're just organizing the code better.

### 1.1 Create `static/demo-state.js` (80 lines)
Centralize scattered global state. This will replace:
- `clusterInfo`
- `nodeDecrees`
- `selectedNodeId`
- `partitionedNodes`
- `eventCounts`
- `isRunning`, `speed`

```javascript
// static/demo-state.js
export class DemoState {
  constructor() {
    this.cluster = null;
    this.nodes = new Map(); // id -> { state, partitioned, decrees[], role, color }
    this.simulation = {
      running: false,
      speed: 1.0,
      selectedNode: null,
    };
    this.eventCounts = {};
    this.listeners = new Set();
  }

  initialize(clusterInfo) {
    this.cluster = clusterInfo;
    for (let i = 0; i < clusterInfo.total_nodes; i++) {
      this.nodes.set(i, {
        state: '--',
        partitioned: false,
        decrees: [],
        role: null,
        color: '#3b82f6',
      });
    }
    this.notify();
  }

  setNodeCapabilities(nodeId, roles, learningStrategy) {
    // Store role info from backend
    const node = this.nodes.get(nodeId);
    if (node) {
      node.role = { roles, learningStrategy };
      this.notify();
    }
  }

  // ... other methods like setNodeState, addDecree, etc.
  
  subscribe(callback) {
    this.listeners.add(callback);
  }

  notify() {
    this.listeners.forEach(cb => cb(this.snapshot()));
  }

  snapshot() {
    return {
      cluster: this.cluster,
      nodes: new Map(this.nodes),
      simulation: { ...this.simulation },
      eventCounts: { ...this.eventCounts },
    };
  }
}

export const state = new DemoState();
```

**Changes to `basic-protocol-demo.js`**:
```diff
+ import { state } from './demo-state.js';

- let clusterInfo = null;
- let nodeDecrees = {};
- let selectedNodeId = null;
- let partitionedNodes = new Set();
- let eventCounts = {};
- let isRunning = false;

  function initializeCluster(clusterInfoData) {
-   clusterInfo = clusterInfoData;
+   state.initialize(clusterInfoData);
  }

  function updateProposalStats() {
-   for (let nodeId = 0; nodeId < clusterInfo.total_nodes; nodeId++) {
-     const count = nodeDecrees[nodeId]?.count || 0;
+   const snapshot = state.snapshot();
+   for (let nodeId = 0; nodeId < snapshot.cluster.total_nodes; nodeId++) {
+     const count = snapshot.nodes.get(nodeId)?.decrees.length || 0;
```

**Benefit**: Single source of truth, easier to debug, no more hunting for global variables

---

### 1.2 Create `static/event-visualizers.js` (150 lines)
Replace the 7 separate `visualize*()` functions with a registry:

```javascript
// static/event-visualizers.js
export const EVENT_VISUALIZERS = {
  Proposal: {
    color: '#60a5fa',
    name: 'NextBallot',
    format: (e) => `[NextBallot] Node ${e.id}: "${formatDecree(e)}"`,
    async visualize(event, viz, state, canCommunicate) {
      viz.setNodeState(event.id, 'propose');
      viz.activateNode(event.id, this.color);
      const beams = [];
      const snapshot = state.snapshot();
      for (let i = 0; i < snapshot.cluster.total_nodes; i++) {
        if (i !== event.id && canCommunicate(event.id, i)) {
          const duration = Math.max(200, (500 / snapshot.simulation.speed) * 0.67);
          beams.push(viz.drawBeam(event.id, i, this.color, duration, 'solid'));
        }
      }
      await Promise.all(beams);
    }
  },

  Promise: {
    color: '#ec4899',
    name: 'LastVote',
    format: (e) => `[LastVote] Node ${e.id} → Node ${e.from}: Ballot ${e.ballot}`,
    async visualize(event, viz, state, canCommunicate) {
      viz.setNodeState(event.id, 'promise');
      viz.activateNode(event.id, this.color);
      if (event.from !== undefined && event.from !== event.id) {
        const snapshot = state.snapshot();
        const duration = Math.max(200, (500 / snapshot.simulation.speed) * 0.67);
        await viz.drawBeam(event.id, event.from, this.color, duration, 'dashed');
      }
    }
  },

  Accept: {
    color: '#f87171',
    name: 'BeginBallot',
    format: (e) => `[BeginBallot] Node ${e.id}: Ballot ${e.ballot}`,
    async visualize(event, viz, state, canCommunicate) {
      viz.setNodeState(event.id, 'accept');
      viz.activateNode(event.id, this.color);
      const beams = [];
      if (event.quorum && Array.isArray(event.quorum)) {
        const snapshot = state.snapshot();
        const duration = Math.max(200, (500 / snapshot.simulation.speed) * 0.67);
        for (const nodeId of event.quorum) {
          if (nodeId !== event.id) {
            beams.push(viz.drawBeam(event.id, nodeId, this.color, duration, 'solid'));
          }
        }
      }
      await Promise.all(beams);
    }
  },

  // ... Accepted, LearnedValue, Success similar pattern
};

export function getEventVisualizer(eventType) {
  return EVENT_VISUALIZERS[eventType];
}
```

**Changes to `basic-protocol-demo.js`**:
```diff
+ import { EVENT_VISUALIZERS } from './event-visualizers.js';

- function visualizeProposal(event, color) { /* 50+ lines */ }
- function visualizePromise(event, color) { /* 50+ lines */ }
- // ... etc

  async function processEventQueue() {
    // ...
    const promises = batch.map(async ({ eventType, event }) => {
-     const colorInfo = eventColorMap[eventType];
-     addEvent(colorInfo ? colorInfo.name : eventType, colorInfo?.color || '#fff');
-     
-     switch (eventType) {
-       case 'Proposal':
-         return visualizeProposal(event, colorInfo.color);
-       case 'Promise':
-         return visualizePromise(event, colorInfo.color);
-       // ... etc
-     }
+     const viz = EVENT_VISUALIZERS[eventType];
+     if (viz) {
+       addEvent(viz.format(event), viz.color);
+       state.eventCounts[eventType] = (state.eventCounts[eventType] || 0) + 1;
+       return viz.visualize(event, visualizer, state, canCommunicate);
+     }
    });
```

**Benefit**: All event visualizations in one place, 300+ lines of boilerplate removed

---

### 1.3 Create `static/scenario-helpers.js` (50 lines)
Builder pattern for scenario phases:

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

  nodeState(id, state) {
    return async () => this.v.setNodeState(id, state);
  }

  activate(id, color) {
    return async () => this.v.activateNode(id, color);
  }

  beamsTo(from, to, color, dur = 500, stagger = 80) {
    return async () => await this.v.drawBeamsTo(from, to, color, dur, 'solid', stagger);
  }

  beamsFrom(from, to, color, dur = 500, stagger = 150) {
    return async () => await this.v.drawBeamsFrom(from, to, color, dur, 'dashed', stagger);
  }

  log(msg, color) {
    return async () => this.u.addEvent(msg, color);
  }

  wait(ms = 300) {
    return async () => await this.u.sleep(ms);
  }

  incr(counter) {
    return async () => {
      this.u.eventCounts[counter]++;
      this.u.updateCounts();
    };
  }
}
```

**Changes to scenario files**:
```diff
+ import { ScenarioPhase } from '../scenario-helpers.js';

  const scenarioSuccess = {
    name: "Clean Success",
    getPhases(colors, utils) {
+     const phase = new ScenarioPhase(visualizer, utils);
      return [
        {
          title: "Step 1: NextBallot(b)",
-         action: async () => {
-           for (let i = 0; i < 7; i++) {
-             visualizer.setNodeState(i, '--');
-             visualizer.setNodeColor(i, '#3b82f6');
-           }
-           visualizer.clearBeams();
-           visualizer.setNodeState(0, "propose");
-           visualizer.activateNode(0, colors.nextballot);
-           addEvent("[NextBallot]...", colors.nextballot);
-           const acceptors = [1, 2, 3, 4, 5, 6];
-           await visualizer.drawBeamsTo(0, acceptors, colors.nextballot, 500, 'solid', 80);
-           eventCounts.nextballot++;
-           updateCounts();
-           await sleep(300);
-         }
+         action: () => phase.run([
+           phase.resetNodes([0,1,2,3,4,5,6]),
+           phase.nodeState(0, 'propose'),
+           phase.activate(0, colors.nextballot),
+           phase.log('[NextBallot] Node 0 sends ballot 100', colors.nextballot),
+           phase.beamsTo(0, [1,2,3,4,5,6], colors.nextballot),
+           phase.incr('nextballot'),
+           phase.wait(300),
+         ])
        },
```

**Benefit**: 50% less code per scenario, clearer intent

---

### 1.4 Create `static/event-queue.js` (80 lines)
Extract event batching logic:

```javascript
// static/event-queue.js
export class EventQueue {
  constructor(handleEvent, batchWindow = 50) {
    this.queue = [];
    this.batchWindow = batchWindow; // microseconds
    this.processing = false;
    this.handleEvent = handleEvent;
  }

  push(event) {
    this.queue.push(event);
    this.scheduleBatch();
  }

  scheduleBatch() {
    if (this.processingTimeout) clearTimeout(this.processingTimeout);
    this.processingTimeout = setTimeout(() => this.processBatch(), 5);
  }

  async processBatch() {
    if (this.processing || this.queue.length === 0) return;
    this.processing = true;

    const batch = this.extractBatch();
    const promises = batch.map(e => this.handleEvent(e));
    
    try {
      await Promise.all(promises);
    } catch (err) {
      console.error('Error processing batch:', err);
    }

    this.processing = false;

    // Continue if more events
    if (this.queue.length > 0) {
      setTimeout(() => this.processBatch(), 100);
    }
  }

  extractBatch() {
    const batch = [];
    if (this.queue.length === 0) return batch;

    const startTime = this.queue[0].created_at;
    while (this.queue.length > 0 && this.queue[0].created_at - startTime < this.batchWindow) {
      batch.push(this.queue.shift());
    }
    return batch;
  }
}
```

**Usage in `basic-protocol-demo.js`**:
```diff
+ import { EventQueue } from './event-queue.js';

+ const eventQueue = new EventQueue(async (event) => {
+   const { eventType, eventData } = event;
+   const viz = EVENT_VISUALIZERS[eventType];
+   if (viz) {
+     addEvent(viz.format(eventData), viz.color);
+     state.eventCounts[eventType] = (state.eventCounts[eventType] || 0) + 1;
+     await viz.visualize(eventData, visualizer, state, canCommunicate);
+   }
+ });

  function handlePaxosEvent(eventData) {
-   eventQueue.push({ eventType, event, colorInfo });
+   const eventType = Object.keys(eventData)[0];
+   eventQueue.push({ eventType, eventData: eventData[eventType], created_at: performance.now() });
  }
```

**Benefit**: Event batching logic is isolated and reusable

---

## Summary of Phase 1

**Files Created**:
- `static/demo-state.js` (80 lines)
- `static/event-visualizers.js` (150 lines)
- `static/scenario-helpers.js` (50 lines)
- `static/event-queue.js` (80 lines)

**Files Modified**:
- `static/basic-protocol-demo.js`: 682 → 250 lines
- `static/scenarios/*.js`: ~20% reduction per file

**Result**: 
- Same functionality, cleaner code
- Much easier to modify event visualizations
- Much easier to add new scenarios
- All existing features still work

---

## Phase 2: Role Improvements (After Phase 1)

Once Phase 1 is solid and tested, we can enhance role support:

### 2.1 Enhanced Role Visualization
- Different colors for P/A/L roles in the state object
- Visual indicators (badges, outline colors)
- Better role-grouped layout with actual grouping

### 2.2 Role-Aware Event Filtering
- Only visualize events relevant to node's roles
- Show learning strategy visually (Direct, Distinguished, ProposerManaged)
- Highlight which nodes participate in each message

### 2.3 Role Statistics Panel
- Real-time counter of role distribution
- Visual breakdown of what each role is doing
- Topology validation (quorum size vs acceptor count, etc.)

---

## Implementation Steps

1. **Start with Phase 1.1** - Create `demo-state.js` and update `basic-protocol-demo.js`
   - Test that visualizer still works
   - Verify all existing scenarios still work

2. **Phase 1.2** - Create `event-visualizers.js`
   - Move all `visualize*()` functions to registry
   - Test event handling

3. **Phase 1.3** - Create `scenario-helpers.js`
   - Update one scenario first (success.js)
   - Test it works
   - Apply to other scenarios

4. **Phase 1.4** - Create `event-queue.js`
   - Integrate into demo
   - Verify event batching still works

5. **Test everything** - Make sure all scenarios, live demo, role pages work

6. **Then Phase 2** - Build on top with role enhancements

---

## Risk Assessment

**Low Risk** - We're:
- Creating new files (no deletions)
- Replacing global variables with a state object (drop-in replacement)
- Moving existing functions into a registry (same logic)
- Using builder pattern for scenarios (same output)

**Zero Breaking Changes** - The public APIs (`visualizer.*`, `addEvent()`, `sleep()`, etc.) all stay the same.

**Rollback Strategy** - If something breaks, we just delete the new files and revert the changes. Very safe.

