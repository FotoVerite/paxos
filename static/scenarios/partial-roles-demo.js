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
let playBtn;
let pauseBtn;
let resetBtn;
let stepBackBtn;
let stepForwardBtn;
let playbackCursor;
let topologyDetails;
let circleLayoutBtn;
let gridLayoutBtn;
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
    if (!followLive && scenarioStarted && batchCount > 0) {
      playBtn.textContent = 'Resume';
    } else {
      playBtn.textContent = 'Play';
    }
  }
}

function updateScenarioDescription() {
  if (!scenarioDetails || !scenarioSelect) return;
  const description = scenarioDescriptions[scenarioSelect.value] || '';
  scenarioDetails.textContent = description;
}

function setLayout(mode) {
  if (!visualizer) return;
  visualizer.setLayoutMode(mode);

  if (circleLayoutBtn) {
    circleLayoutBtn.classList.toggle('active', mode === 'circle');
  }
  if (gridLayoutBtn) {
    gridLayoutBtn.classList.toggle('active', mode === 'role-grouped');
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

function maybeRerenderRoleLayout() {
  if (!visualizer || visualizer.layoutMode !== 'role-grouped') return;
  const snapshot = state.snapshot();
  if (!snapshot.cluster) return;

  let roleCount = 0;
  for (let i = 0; i < snapshot.cluster.total_nodes; i++) {
    const node = snapshot.nodes.get(i);
    if (node?.role) {
      roleCount += 1;
    }
  }
  if (roleCount === snapshot.cluster.total_nodes) {
    visualizer.render(snapshot.cluster);
    if (snapshot.leaderId !== null && snapshot.leaderId !== undefined) {
      visualizer.setLeader(snapshot.leaderId);
    }
  }
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
        maybeRerenderRoleLayout();
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
  playBtn.disabled = true;
  pauseBtn.disabled = false;
  statusTitle.textContent = 'Running';
  statusTitle.style.color = '#60a5fa';
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
  playBtn.disabled = true;
  pauseBtn.disabled = false;
  scenarioSelect.disabled = true;
  learningStrategySelect.disabled = true;
  updateScenarioDescription();

  statusTitle.textContent = 'Running';
  statusTitle.style.color = '#60a5fa';

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

    statusTitle.textContent = 'Processing';
    statusTitle.style.color = '#f59e0b';

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
    statusTitle.style.color = '#ef4444';
    statusTitle.textContent = 'Error';
    if (scenarioDetails) {
      scenarioDetails.textContent = error.message;
    }
  }

  scenarioRunning = false;
  state.setRunning(false);
  if (controller) {
    controller.setCaptureEnabled(false);
    controller.pause({ waitForFrame: false });
  }

  playBtn.disabled = false;
  pauseBtn.disabled = true;
  scenarioSelect.disabled = false;
  learningStrategySelect.disabled = false;

  const totalEvents = controller ? controller.getCapturedEventCount() : 0;

  statusTitle.textContent = 'Complete';
  statusTitle.style.color = '#34d399';
  if (scenarioDetails) {
    scenarioDetails.textContent = `Scenario finished - ${totalEvents} total events visualized`;
  }
}

async function pauseScenario() {
  if (controller) {
    controller.pause({ waitForFrame: false });
  }
  state.setRunning(false);
  playBtn.disabled = false;
  pauseBtn.disabled = true;
  scenarioSelect.disabled = false;
  learningStrategySelect.disabled = false;

  statusTitle.textContent = 'Paused';
  statusTitle.style.color = '#f59e0b';
  if (playBtn) {
    playBtn.textContent = scenarioStarted ? 'Resume' : 'Play';
  }
}

async function stepForward() {
  if (!controller) return;
  await controller.pause({ waitForFrame: true });
  await controller.stepForward();
  statusTitle.textContent = 'Stepping';
  statusTitle.style.color = '#f59e0b';
}

async function stepBack() {
  if (!controller) return;
  await controller.pause({ waitForFrame: true });
  await controller.stepBack();
  statusTitle.textContent = 'Stepping';
  statusTitle.style.color = '#f59e0b';
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
  pauseBtn.disabled = true;
  scenarioSelect.disabled = false;
  learningStrategySelect.disabled = false;

  statusTitle.textContent = 'Ready';
  statusTitle.style.color = '#60a5fa';
  updateScenarioDescription();
}

document.addEventListener('DOMContentLoaded', () => {
  eventLog = document.getElementById('eventLog');
  speedSlider = document.getElementById('speed');
  speedValue = document.getElementById('speedValue');
  statusTitle = document.getElementById('statusTitle');
  statusDescription = document.getElementById('statusDescription');
  playBtn = document.getElementById('playBtn');
  pauseBtn = document.getElementById('pauseBtn');
  resetBtn = document.getElementById('resetBtn');
  stepBackBtn = document.getElementById('stepBackBtn');
  stepForwardBtn = document.getElementById('stepForwardBtn');
  playbackCursor = document.getElementById('playbackCursor');
  topologyDetails = document.getElementById('topologyDetails');
  circleLayoutBtn = document.getElementById('circleLayoutBtn');
  gridLayoutBtn = document.getElementById('gridLayoutBtn');
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

  if (playBtn) playBtn.addEventListener('click', playScenario);
  if (pauseBtn) pauseBtn.addEventListener('click', pauseScenario);
  if (resetBtn) resetBtn.addEventListener('click', resetScenario);
  if (stepBackBtn) stepBackBtn.addEventListener('click', stepBack);
  if (stepForwardBtn) stepForwardBtn.addEventListener('click', stepForward);
  if (circleLayoutBtn) circleLayoutBtn.addEventListener('click', () => setLayout('circle'));
  if (gridLayoutBtn) gridLayoutBtn.addEventListener('click', () => setLayout('role-grouped'));

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
