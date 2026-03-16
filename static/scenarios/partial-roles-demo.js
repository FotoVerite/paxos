/**
 * Partial Roles Demo
 * Visualizing heterogeneous node roles and role-aware routing
 */

import { state } from '/demo-state.js';
import { createPaxosDemoController } from '/js/paxos-demo/core/runtime.js';
import { createEventLogPlugin } from '/js/paxos-demo/plugins/event-log.js';
import { createEventCountsPlugin } from '/js/paxos-demo/plugins/event-counts.js';
import { createVisualizeEventsPlugin } from '/js/paxos-demo/plugins/visualize-events.js';
import { createPartitionStatePlugin } from '/js/paxos-demo/plugins/partition-state.js';
import { createDecreePanelPlugin } from '/js/paxos-demo/plugins/decree-panel.js';
import { createPlaybackDelayPlugin } from '/js/paxos-demo/plugins/playback-delay.js';
import { createNodeCapabilitiesPlugin } from '/js/paxos-demo/plugins/node-capabilities.js';

const FRAME_WINDOW_MICROS = 50;
const ROLE_ORDER = ['Proposer', 'Acceptor', 'Learner'];
const ROLE_CODE = {
  Proposer: 'P',
  Acceptor: 'A',
  Learner: 'L',
};
const SIGNATURE_PRIORITY = ['PAL', 'PA', 'PL', 'AL', 'P', 'A', 'L'];

const scenarioDescriptions = {
  happy_path_with_leader:
    'Start here. Every node carries every role, so you can see the baseline before the topology gets split apart.',
  dedicated_acceptors:
    'One proposer-learner node writes through an acceptor-only quorum while separate learner-only nodes stay downstream.',
  learners_off_write_path:
    'The proposer-learner reaches quorum first, then the learner-only tier catches up from the success broadcast.',
  competing_proposers:
    'Two proposer-learner nodes compete against the same acceptor-only core while learner-only followers stay separate.',
  partial_roles:
    'A mixed cluster combines full-role nodes with dedicated proposers, acceptors, and learners so the topology split is visible in one run.',
};

const scenarioTeachingNotes = {
  happy_path_with_leader: {
    title: 'Baseline',
    focus:
      'Every node is doing every job here. This is the control case for the rest of the page.',
    runningChange:
      'Nothing is split yet. Proposing, accepting, and learning are all happening on the same nodes.',
    readyNext:
      'Watch the full path first, then move to the role-split scenarios and compare what actually changes.',
    runningNext:
      'Notice how short the path feels when one node can carry the whole cycle.',
    replayNext:
      'Step through the batches and use this as your mental baseline for the split topologies.',
  },
  dedicated_acceptors: {
    title: 'Dedicated Acceptors',
    focus:
      'This run isolates the write path. Acceptors hold the quorum, while separate learners stay downstream.',
    runningChange:
      'The quorum now forms on acceptor-only nodes. Learning happens after that, not in the same place.',
    readyNext:
      'Watch who actually handles Prepare and Accept, then see who only shows up once success is broadcast.',
    runningNext:
      'Notice which nodes are busy during voting, and which ones stay quiet until the value is already chosen.',
    replayNext:
      'Step back through the batches and compare the acceptor core to the learner-only followers.',
  },
  learners_off_write_path: {
    title: 'Learners Off the Write Path',
    focus:
      'This one separates choosing from learning. The quorum finishes first, and the learner tier catches up afterward.',
    runningChange:
      'The write path is over before every learner knows the result.',
    readyNext:
      'Watch for the moment the value is effectively decided, then watch how the learner-only nodes hear about it later.',
    runningNext:
      'Pay attention to the gap between votes landing and learner-only nodes updating.',
    replayNext:
      'Use step mode if you want to isolate the exact handoff from quorum to learning.',
  },
  competing_proposers: {
    title: 'Competing Proposers',
    focus:
      'Now the proposers are split too. Two proposer-learner nodes are contending for the same acceptor core.',
    runningChange:
      'The conflict is happening at the proposer edge, while the acceptor quorum stays shared.',
    readyNext:
      'Watch which proposer gets traction, and how the acceptor-only nodes become the common bottleneck.',
    runningNext:
      'Notice that the learners are downstream observers here. The real struggle is between the proposers and the acceptor core.',
    replayNext:
      'Step through this one if the contention feels too fast on live playback.',
  },
  partial_roles: {
    title: 'Mixed Roles',
    focus:
      'This is the comparison run. Full-role nodes sit beside dedicated proposers, acceptors, and learners.',
    runningChange:
      'You are watching the same protocol move through mixed topologies at once.',
    readyNext:
      'Compare which nodes carry the whole cycle and which ones only touch a single phase.',
    runningNext:
      'Watch the full-role nodes against the dedicated ones. The difference is architectural, not algorithmic.',
    replayNext:
      'Step through the batches if you want to see exactly where the mixed topology stops behaving like the baseline.',
  },
};

let controller = null;
let visualizer = null;
let scenarioTimeout = null;
let scenarioRunning = false;
let scenarioStarted = false;

let eventLog;
let speedSlider;
let speedValue;
let statusTitle;
let statusDescription;
let statusFocus;
let statusChange;
let statusNext;
let playBtn;
let resetBtn;
let stepBackBtn;
let stepForwardBtn;
let playbackCursor;
let topologyDetails;
let learningStrategySelect;
let scenarioSelect;
let scenarioDetails;
let statsContainer;
let decreePanel;
let filterBar;
let filterChips;
let filterToggle;
let filterClose;
let filterStatus;
let secondaryControls;
let playbackTools;

function getScenarioTeachingNote() {
  return scenarioTeachingNotes[scenarioSelect?.value] || scenarioTeachingNotes.happy_path_with_leader;
}

function setTeachingStatus(mode, options = {}) {
  if (!statusTitle || !statusFocus || !statusChange || !statusNext) return;

  const note = getScenarioTeachingNote();
  const totalEvents = options.totalEvents ?? 0;

  switch (mode) {
    case 'ready':
      statusTitle.textContent = note.title;
      statusTitle.style.color = '#60a5fa';
      statusFocus.textContent = note.focus;
      statusChange.textContent = 'Nothing has moved yet.';
      statusNext.textContent = note.readyNext;
      break;
    case 'running':
      statusTitle.textContent = note.title;
      statusTitle.style.color = '#60a5fa';
      statusFocus.textContent = note.focus;
      statusChange.textContent = note.runningChange;
      statusNext.textContent = note.runningNext;
      break;
    case 'paused':
      statusTitle.textContent = 'Paused on the split';
      statusTitle.style.color = '#f59e0b';
      statusFocus.textContent = note.focus;
      statusChange.textContent = 'The live run is paused on the current playback position.';
      statusNext.textContent = 'Use Resume to keep following live, or step through the batches to inspect the handoff more closely.';
      break;
    case 'stepping':
      statusTitle.textContent = 'Step through it';
      statusTitle.style.color = '#f59e0b';
      statusFocus.textContent = note.focus;
      statusChange.textContent = 'You are looking at one captured batch at a time now.';
      statusNext.textContent = note.replayNext;
      break;
    case 'replaying':
      statusTitle.textContent = 'Live run captured';
      statusTitle.style.color = '#f59e0b';
      statusFocus.textContent = note.focus;
      statusChange.textContent = 'The live run has finished, but the captured playback is still catching up.';
      statusNext.textContent = note.replayNext;
      break;
    case 'complete':
      statusTitle.textContent = 'Captured run';
      statusTitle.style.color = '#34d399';
      statusFocus.textContent = note.focus;
      statusChange.textContent = `This run captured ${totalEvents} events.`;
      statusNext.textContent = note.replayNext;
      break;
    case 'error':
      statusTitle.textContent = 'Run failed';
      statusTitle.style.color = '#ef4444';
      statusFocus.textContent = note.focus;
      statusChange.textContent = options.message || 'The scenario did not start cleanly.';
      statusNext.textContent = 'Reset the run and try again.';
      break;
    default:
      break;
  }
}

function getRoleSignature(roles = []) {
  return ROLE_ORDER.filter((role) => roles.includes(role))
    .map((role) => ROLE_CODE[role])
    .join('');
}

function getRoleBucketMeta(signature) {
  if (signature === 'PAL') {
    return { label: 'All roles', markClass: 'role-mark-all' };
  }
  if (signature === 'A') {
    return { label: 'Acceptor only', markClass: 'role-mark-acceptor' };
  }
  if (signature === 'P') {
    return { label: 'Proposer only', markClass: 'role-mark-proposer' };
  }
  if (signature === 'L') {
    return { label: 'Learner only', markClass: 'role-mark-learner' };
  }
  return { label: 'Two roles', markClass: 'role-mark-mixed' };
}

function updatePlaybackControls(playbackState) {
  if (!playbackState) return;
  const { batchCount, cursor, playerMode, isBusy } = playbackState;
  const followLive = playerMode === 'follow';
  const hasCapturedPlayback = batchCount > 0;

  if (secondaryControls) {
    secondaryControls.hidden = !scenarioStarted && !hasCapturedPlayback;
  }
  if (playbackTools) {
    playbackTools.hidden = !hasCapturedPlayback;
  }

  if (stepBackBtn) {
    stepBackBtn.disabled = followLive || isBusy || cursor < 0;
  }
  if (stepForwardBtn) {
    stepForwardBtn.disabled = followLive || isBusy || cursor + 1 >= batchCount;
  }

  if (playbackCursor) {
    const displayIndex = Math.max(0, cursor + 1);
    playbackCursor.textContent = `Batch ${displayIndex}/${batchCount}`;
  }

  if (playBtn) {
    if (scenarioRunning && followLive) {
      playBtn.textContent = 'Pause';
    } else if (!followLive && scenarioStarted && batchCount > 0) {
      playBtn.textContent = 'Resume';
    } else if (!scenarioStarted) {
      playBtn.textContent = 'Start';
    } else {
      playBtn.textContent = 'Play';
    }
  }
}

function updateScenarioDescription() {
  if (!scenarioSelect) return;
  if (scenarioDetails) {
    const description = scenarioDescriptions[scenarioSelect.value] || '';
    scenarioDetails.textContent = description;
  }
  if (!scenarioStarted) {
    setTeachingStatus('ready');
  }
}

function updateTopologyPanel() {
  const snapshot = state.snapshot();
  if (!snapshot.cluster || !topologyDetails) return;

  const buckets = new Map();

  for (let i = 0; i < snapshot.cluster.total_nodes; i++) {
    const node = snapshot.nodes.get(i);
    if (!node || !node.role) continue;

    const roles = node.role.roles || [];
    const signature = getRoleSignature(roles);
    if (!signature) continue;
    if (!buckets.has(signature)) {
      buckets.set(signature, []);
    }
    buckets.get(signature).push(i);
  }

  const strategyLabel = learningStrategySelect
    ? learningStrategySelect.options[learningStrategySelect.selectedIndex]?.text
    : 'Direct';

  const orderedSignatures = Array.from(buckets.keys()).sort((a, b) => {
    const indexA = SIGNATURE_PRIORITY.indexOf(a);
    const indexB = SIGNATURE_PRIORITY.indexOf(b);
    if (indexA === -1 && indexB === -1) return a.localeCompare(b);
    if (indexA === -1) return 1;
    if (indexB === -1) return -1;
    return indexA - indexB;
  });

  const roleRows = orderedSignatures
    .map((signature) => {
      const nodes = buckets.get(signature) || [];
      const meta = getRoleBucketMeta(signature);
      const chips = nodes
        .map((nodeId) => `<span class="topology-node-chip">${nodeId}</span>`)
        .join('');

      return `
        <div class="topology-role-row">
          <div class="topology-role-label">
            <span class="role-mark ${meta.markClass}" aria-hidden="true"></span>
            <span>${meta.label}</span>
          </div>
          <div class="topology-node-list">${chips}</div>
          <div class="topology-role-count">${nodes.length}</div>
        </div>
      `;
    })
    .join('');

  topologyDetails.innerHTML = `
    <div class="topology-summary">
      <div class="topology-summary-chip">
        <span class="k">Nodes</span>
        <span class="v">${snapshot.cluster.total_nodes}</span>
      </div>
      <div class="topology-summary-chip">
        <span class="k">Quorum</span>
        <span class="v">${snapshot.cluster.quorum_size} A</span>
      </div>
      <div class="topology-summary-chip">
        <span class="k">Learning</span>
        <span class="v">${strategyLabel}</span>
      </div>
    </div>
    <div class="topology-role-list">
      ${roleRows}
    </div>
  `;
}

function canCommunicate(from, to) {
  const snapshot = state.snapshot();
  const fromNode = snapshot.nodes.get(from);
  const toNode = snapshot.nodes.get(to);
  if (!fromNode || !toNode) return true;
  return fromNode.partitioned === toNode.partitioned;
}

function buildController() {
  const clusterRenderPlugin = {
    onCluster(clusterInfo, ctx) {
      ctx.visualizer.render(clusterInfo);
      updateTopologyPanel();
      const snapshot = ctx.state.snapshot();
      if (snapshot.leaderId !== null && snapshot.leaderId !== undefined) {
        ctx.visualizer.setLeader(snapshot.leaderId);
      }
    },
  };

  const topologyPlugin = {
    onEvent({ eventType }, ctx) {
      if (eventType === 'NodeCapabilities') {
        updateTopologyPanel();
      }
    },
    onCluster() {
      updateTopologyPanel();
    },
    onReset() {
      updateTopologyPanel();
    },
  };

  controller = createPaxosDemoController({
    state,
    visualizer,
    canCommunicate,
    frameWindowMicros: FRAME_WINDOW_MICROS,
    onPlaybackUpdate: updatePlaybackControls,
    plugins: [
      clusterRenderPlugin,
      topologyPlugin,
      createEventLogPlugin({
        eventLog,
        filterBar,
        filterChips,
        filterToggle,
        filterClose,
        filterStatus,
      }),
      createEventCountsPlugin(),
      createVisualizeEventsPlugin({
        skip: new Set(['PartitionCreated', 'PartitionHealed']),
      }),
      createPartitionStatePlugin(),
      createNodeCapabilitiesPlugin(),
      createDecreePanelPlugin({
        statsContainer,
        decreePanel,
        nodeFilter: (node) => node?.role?.roles?.includes('Learner'),
        hideStatsWhenEmpty: true,
        emptySelectionHint: 'Click a learner node to view its decrees',
        invalidSelectionHint: 'Selected node is not a learner',
      }),
      createPlaybackDelayPlugin(),
    ],
  });
}

async function resumePlayback() {
  if (!controller) return;
  controller.setCaptureEnabled(true);
  await controller.resume();
  if (scenarioRunning) {
    state.setRunning(true);
  }
  playBtn.disabled = false;
  setTeachingStatus('running');
  updatePlaybackControls(controller.getPlaybackState());
}

async function playScenario() {
  if (!controller) return;
  const playback = controller.getPlaybackState();
  if (scenarioStarted && playback.playerMode !== 'follow') {
    await resumePlayback();
    return;
  }

  if (scenarioRunning) return;

  const scenarioValue = scenarioSelect.value;
  const strategy = learningStrategySelect.value;
  const startingNewScenario = !scenarioStarted;

  scenarioStarted = true;
  updatePlaybackControls(controller.getPlaybackState());

  if (controller) {
    if (startingNewScenario) {
      controller.invalidate({ clearHistory: true });
      controller.pauseIngestion({ untilClusterInitialized: true });
    }
    controller.setCaptureEnabled(true);
    await controller.resume();
  }

  scenarioRunning = true;
  state.setRunning(true);
  playBtn.disabled = false;
  scenarioSelect.disabled = true;
  learningStrategySelect.disabled = true;
  updateScenarioDescription();
  setTeachingStatus('running');

  try {
    const payload = {
      node_count: 9,
      duration_secs: 60,
      scenario_type: scenarioValue,
      learning_strategy: strategy,
    };

    const response = await fetch('/api/start-scenario', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      throw new Error('Failed to start scenario');
    }

    await new Promise((resolve) => {
      scenarioTimeout = setTimeout(() => {
        scenarioRunning = false;
        state.setRunning(false);
        resolve();
      }, 60000);
    });

    setTeachingStatus('replaying');

    while (
      controller &&
      controller.getPlaybackState().playerMode === 'follow' &&
      (controller.eventQueue.length() > 0 ||
        controller.eventQueue.isProcessing() ||
        controller.hasPendingBatches())
    ) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  } catch (error) {
    console.error(error);
    scenarioRunning = false;
    state.setRunning(false);
    setTeachingStatus('error', { message: error.message });
  }

  scenarioRunning = false;
  state.setRunning(false);
  if (controller) {
    controller.setCaptureEnabled(false);
    controller.pause({ waitForFrame: false });
  }

  playBtn.disabled = false;
  scenarioSelect.disabled = false;
  learningStrategySelect.disabled = false;
  updatePlaybackControls(controller.getPlaybackState());

  const totalEvents = controller ? controller.getCapturedEventCount() : 0;

  setTeachingStatus('complete', { totalEvents });
}

async function pauseScenario() {
  if (controller) {
    controller.pause({ waitForFrame: false });
  }
  state.setRunning(false);
  playBtn.disabled = false;
  scenarioSelect.disabled = false;
  learningStrategySelect.disabled = false;
  updatePlaybackControls(controller.getPlaybackState());

  setTeachingStatus('paused');
  if (playBtn) {
    playBtn.textContent = scenarioStarted ? 'Resume' : 'Start';
  }
}

async function stepForward() {
  if (!controller) return;
  await controller.pause({ waitForFrame: true });
  await controller.stepForward();
  setTeachingStatus('stepping');
}

async function stepBack() {
  if (!controller) return;
  await controller.pause({ waitForFrame: true });
  await controller.stepBack();
  setTeachingStatus('stepping');
}

async function resetScenario() {
  state.resetSimulation();
  if (controller) {
    controller.pause({ waitForFrame: false });
    controller.pauseIngestion({ untilClusterInitialized: true });
    controller.setCaptureEnabled(false);
    controller.reset({ clearHistory: true });
  }

  scenarioRunning = false;
  scenarioStarted = false;
  state.setRunning(false);
  if (scenarioTimeout) {
    clearTimeout(scenarioTimeout);
    scenarioTimeout = null;
  }

  try {
    await fetch('/api/stop-scenario', { method: 'POST' });
  } catch (error) {
    console.error('Error stopping scenario:', error);
  }

  playBtn.disabled = false;
  scenarioSelect.disabled = false;
  learningStrategySelect.disabled = false;
  updatePlaybackControls(controller ? controller.getPlaybackState() : null);

  updateScenarioDescription();
  setTeachingStatus('ready');
}

document.addEventListener('DOMContentLoaded', () => {
  eventLog = document.getElementById('eventLog');
  speedSlider = document.getElementById('speed');
  speedValue = document.getElementById('speedValue');
  statusTitle = document.getElementById('statusTitle');
  statusDescription = document.getElementById('statusDescription');
  statusFocus = document.getElementById('statusFocus');
  statusChange = document.getElementById('statusChange');
  statusNext = document.getElementById('statusNext');
  playBtn = document.getElementById('playBtn');
  resetBtn = document.getElementById('resetBtn');
  stepBackBtn = document.getElementById('stepBackBtn');
  stepForwardBtn = document.getElementById('stepForwardBtn');
  playbackCursor = document.getElementById('playbackCursor');
  topologyDetails = document.getElementById('topologyDetails');
  learningStrategySelect = document.getElementById('learningStrategySelect');
  scenarioSelect = document.getElementById('scenarioSelect');
  scenarioDetails = document.getElementById('scenarioDetails');
  statsContainer = document.getElementById('proposalStatsContainer');
  decreePanel = document.getElementById('decreePanel');
  filterBar = document.getElementById('eventFilterBar');
  filterChips = document.getElementById('eventFilterChips');
  filterToggle = document.getElementById('eventFilterToggle');
  filterClose = document.getElementById('eventFilterClose');
  filterStatus = document.getElementById('eventFilterStatus');
  secondaryControls = document.getElementById('secondaryControls');
  playbackTools = document.getElementById('playbackTools');

  visualizer = new PaxosVisualizer('partialRolesSvg', {
    nodeRadius: 180,
    nodeCircleRadius: 24,
    layoutMode: 'circle',
    useRoleShapes: true,
  });

  speedSlider.addEventListener('change', (e) => {
    const speed = parseFloat(e.target.value);
    speedValue.textContent = speed.toFixed(2) + 'x';
    state.setSpeed(speed);
  });

  scenarioSelect.addEventListener('change', async () => {
    updateScenarioDescription();
    if (scenarioStarted) {
      await resetScenario();
    }
  });

  learningStrategySelect.addEventListener('change', async () => {
    updateTopologyPanel();
    if (scenarioStarted) {
      await resetScenario();
    }
  });

  if (playBtn) {
    playBtn.addEventListener('click', async () => {
      if (scenarioRunning && controller?.getPlaybackState().playerMode === 'follow') {
        await pauseScenario();
        return;
      }
      await playScenario();
    });
  }
  if (resetBtn) resetBtn.addEventListener('click', resetScenario);
  if (stepBackBtn) stepBackBtn.addEventListener('click', stepBack);
  if (stepForwardBtn) stepForwardBtn.addEventListener('click', stepForward);

  const initialSpeed = parseFloat(speedSlider.value);
  state.setSpeed(initialSpeed);
  updateScenarioDescription();

  buildController();
  controller.connect();
  resetScenario();

  if (controller) {
    updatePlaybackControls(controller.getPlaybackState());
  }
});
