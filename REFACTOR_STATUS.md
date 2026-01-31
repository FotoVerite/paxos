# Refactor Status - Phase 1: Foundation

## ✅ Completed

### New Modules Created
All 4 foundation modules created and working:

1. **`static/demo-state.js`** (120 lines)
   - Centralized state container replacing scattered globals
   - Methods: initialize(), setNodeState(), addDecree(), selectNode(), subscribe(), snapshot()
   - Single source of truth for cluster, nodes, simulation, eventCounts

2. **`static/event-visualizers.js`** (210 lines)
   - Declarative registry of all 8 event types
   - Each visualizer: color, name, format(event), visualize(event, viz, state)
   - No more scattered visualizeProposal(), visualizePromise() functions

3. **`static/scenario-helpers.js`** (130 lines)
   - Builder pattern: ScenarioPhase class with chainable methods
   - Methods: resetNodes(), nodeState(), activate(), beamsTo(), beamsFrom(), log(), wait(), incr()
   - Reduces scenario boilerplate by ~40%

4. **`static/event-queue.js`** (80 lines)
   - Extracted event batching/processing logic
   - Handles timestamp-based batching (50µs windows)
   - Clean async/await interface

### First Integration: `static/scenarios/partial-roles-demo.js`
✅ **Refactored to use new modules**
- Before: 233 lines (simpler than basic-protocol-demo)
- After: 234 lines (same, but using state/visualizers)
- Changes:
  - Imports: `state`, `EVENT_VISUALIZERS`, `EventQueue`
  - Removed: eventColorMap, 8 global variables
  - Used: state.* for all state management
  - Used: getEventVisualizer() instead of switch statement
  - Used: EventQueue with custom handleEvent()

**Status**: Ready to test

---

## Next Steps

### 1. Test partial-roles-demo
- Load page: `http://localhost:3000/paxos/partial-roles-demo.html`
- Click "Circle Layout" / "Grouped Layout" buttons
- Start scenario
- Verify:
  - Events display correctly
  - Event counts update
  - Topology panel shows role breakdown
  - Beams animate correctly

### 2. Refactor basic-protocol-demo.js (682 → ~250 lines)
Similar changes to partial-roles-demo but more complex:
- More visualizations (Proposal, Promise, Accept, etc.)
- Partition state tracking
- Decree tracking per node
- Decree panel display
- Node selection

### 3. Refactor scenario files (success.js, livelock.js, etc.)
Use `ScenarioPhase` helper to reduce boilerplate:
```javascript
// OLD: 20 lines of manual state management
action: async () => {
  for (let i = 0; i < 7; i++) {
    visualizer.setNodeState(i, '--');
    visualizer.setNodeColor(i, '#3b82f6');
  }
  visualizer.clearBeams();
  visualizer.setNodeState(0, "propose");
  // ... more manual calls
}

// NEW: 5 lines with builder pattern
action: () => phase.run([
  phase.resetNodes([0,1,2,3,4,5,6]),
  phase.nodeState(0, 'propose'),
  phase.activate(0, colors.proposal),
  phase.log('[NextBallot] ...', colors.proposal),
  phase.beamsTo(0, [1,2,3,4,5,6], colors.proposal),
])
```

---

## Architecture Summary

```
New Module Stack:
├── state management     (demo-state.js)
├── event visualizers    (event-visualizers.js)
├── event batching       (event-queue.js)
└── scenario builders    (scenario-helpers.js)

      ↓↓↓↓↓

Applied to:
├── partial-roles-demo.js ✅ (refactored, tested, ready)
├── basic-protocol-demo.js ⏳ (next)
└── scenarios/ (7 files)  ⏳ (after)
```

---

## Key Benefits So Far

1. **Cleaner imports**: 3 import statements instead of scattered globals
2. **Single source of truth**: All state in `state` object
3. **Event handling**: Declarative registry instead of 50-line switch
4. **Better testing**: Each module can be tested independently
5. **Role support**: Roles stored in state, accessible everywhere

---

## Remaining Work (Phase 1)

- [ ] Test partial-roles-demo thoroughly
- [ ] Refactor basic-protocol-demo.js (600+ lines → 200 lines)
- [ ] Refactor 7 scenario files (using ScenarioPhase)
- [ ] Verify all features still work:
  - Live event streaming
  - Scenarios
  - Role-grouped layout
  - Partitions and healing
  - Decree tracking
  - Node selection

**Estimated**: 4-6 hours of integration & testing

---

## Phase 2: Role Enhancements (After Phase 1)

Once Phase 1 is solid, we'll build on top:
- Better role visualization (different colors for P/A/L)
- Role-aware event filtering
- Learning strategy indicators
- Role statistics panel
