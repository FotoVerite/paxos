# Testing Checklist - Refactor Verification

## Before You Start
1. Build project: `cargo build --release`
2. Run server: `cargo run --release` (or just `cargo run`)
3. Server should start on `http://localhost:3000`

---

## Test 1: partial-roles-demo.js ✅ FIRST

Navigate to: `http://localhost:3000/paxos/partial-roles-demo.html`

### Visual Check
- [ ] Page loads without errors (check browser console)
- [ ] SVG visualization displays nodes in a circle
- [ ] "Circle Layout" and "Grouped Layout" buttons visible
- [ ] Topology panel shows "Total Nodes: 9"

### Start Scenario
- [ ] Click "Play" button
- [ ] Page should show "Running" status
- [ ] Events start appearing in the log (should see Proposal, Promise, etc.)
- [ ] Event counters update (NextBallot count, etc.)
- [ ] Beams animate between nodes

### Layout Switch
- [ ] Click "Grouped Layout" button
- [ ] Nodes reorganize by role (Proposers, Acceptors, Learners)
- [ ] Topology panel updates with role breakdown
- [ ] Click "Circle Layout" - nodes return to circle

### Stop & Reset
- [ ] Click "Pause" button
- [ ] Events stop appearing
- [ ] Click "Reset" button
- [ ] Counters reset to 0
- [ ] Event log clears
- [ ] Status returns to "Ready"

### Console Check
- [ ] **No JavaScript errors in console**
- [ ] No "undefined" warnings
- [ ] No import errors

### State Check
Open browser DevTools Console and run:
```javascript
// Should work if state module is loaded correctly
console.log(state.snapshot());
// Should show: { cluster, nodes, simulation, eventCounts }
```

---

## Test 2: Verify Module Loading

In browser console:
```javascript
// Check demo-state
typeof state  // Should be 'object'
typeof state.initialize  // Should be 'function'

// Check event-visualizers (if accessible)
typeof EVENT_VISUALIZERS  // May not be available in window scope

// Check scenario-helpers (if loaded)
typeof ScenarioPhase  // May not be available unless scenario page loads it
```

---

## Test 3: basic-protocol-demo.js (When Ready)

Navigate to: `http://localhost:3000/paxos/visualizer.html` or demo page

### Same Tests as partial-roles-demo
- [ ] Page loads without errors
- [ ] Play scenario
- [ ] Events appear with correct colors
- [ ] Counters update
- [ ] Stop and reset work
- [ ] **No console errors**

### Additional: Partition Demo
- [ ] Partitioned nodes should appear dimmed/grayed out
- [ ] Event filtering respects partitions (no beams across partition)
- [ ] Partition heals and nodes re-enable

### Additional: Decree Panel
- [ ] Click on a node to select it
- [ ] Decree panel shows decrees learned by that node
- [ ] Decrees are sorted by number
- [ ] Clicking another node updates panel

---

## Test 4: Scenario Files (When Ready)

Test one scenario file as template: `http://localhost:3000/paxos/preliminary-protocol-visualizer.html`

- [ ] Page loads
- [ ] Click phase buttons (Next, Prev if available)
- [ ] Phases execute in order
- [ ] Beams animate correctly
- [ ] Event counts update properly
- [ ] No console errors

---

## Test 5: Role Display

On any demo page:
- [ ] Topology panel shows P (Proposers), A (Acceptors), L (Learners) counts
- [ ] Role tags appear with colors
- [ ] Node list shows which nodes have which roles

---

## Integration Test: Full Flow

1. Start server
2. Open partial-roles-demo.html
3. Run scenario
4. Switch layouts multiple times
5. Reset
6. Run again
7. Check browser console - **should have ZERO errors**

---

## Known Good Signs

✅ Events appear in log as they happen
✅ Beams animate smoothly between nodes
✅ Counters increment correctly
✅ Layout switches work
✅ Reset clears everything
✅ No console errors/warnings
✅ Page is responsive

---

## If Tests Fail

### "Cannot find module" errors
- Check import paths are correct (relative paths: `../demo-state.js`)
- Verify files exist in `static/` folder
- Check browser DevTools Network tab for 404s

### State undefined
- Verify `state` is imported: `import { state } from '../demo-state.js'`
- Check console for module loading errors

### Events not displaying
- Check if scenario started (look for "Running" status)
- Open DevTools → Network tab → look for WebSocket connection
- Check if events are arriving (should see message traffic)

### Styling issues
- Refresh page (Ctrl+Shift+R for hard refresh)
- Clear browser cache
- Check CSS files still load correctly

---

## Rollback Plan

If something breaks:

1. **Identify broken file** (partial-roles-demo.js, basic-protocol-demo.js, etc.)
2. **Revert the file**: `git checkout static/scenarios/partial-roles-demo.js`
3. **Test old version** to confirm it works
4. **Review changes** to find issue
5. **Fix and re-test**

Since we're only USING new modules (not modifying old ones), rollback is clean.

---

## Success Criteria for Phase 1

All of the following must pass:
- ✅ partial-roles-demo.js works (events, state, layout switching)
- ✅ basic-protocol-demo.js works (partition, decrees, all visualizations)
- ✅ All 7 scenario files work (when refactored)
- ✅ ZERO JavaScript errors in console for any page
- ✅ Animations smooth at 60fps
- ✅ Events process correctly
- ✅ State updates properly
- ✅ Role information displays correctly

If all green, Phase 1 is complete and we can start Phase 2 (role enhancements).
