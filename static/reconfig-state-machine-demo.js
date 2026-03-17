import { state } from '/demo-state.js';
import { createPaxosDemoController } from '/js/paxos-demo/core/runtime.js';
import { createEventLogPlugin } from '/js/paxos-demo/plugins/event-log.js';
import { createVisualizeEventsPlugin } from '/js/paxos-demo/plugins/visualize-events.js';
import { createDecreePanelPlugin } from '/js/paxos-demo/plugins/decree-panel.js';
import { createPlaybackDelayPlugin } from '/js/paxos-demo/plugins/playback-delay.js';
import { createNodeCapabilitiesPlugin } from '/js/paxos-demo/plugins/node-capabilities.js';
import { createReconfigPhasePlugin } from '/js/paxos-demo/plugins/reconfig-phase.js';
import { createSlotTapePlugin } from '/js/paxos-demo/plugins/slot-tape.js';
import { getPmmcScenarioNodeCount } from '/js/paxos-demo/config/pmmc-scenarios.js';

let controller = null;
let visualizer = null;
let scenarioTimeout = null;
let scenarioRunning = false;
let scenarioStarted = false;
let scenarioFinished = false;
let scenarioStartInFlight = false;

let eventLog;
let speedSlider;
let speedValue;
let scenarioSelect;
let statusTitle;
let statusDescription;
let statusFocus;
let statusChange;
let statusNext;
let reconfigPhaseTitle;
let reconfigPhaseDescription;
let togglePlayBtn;
let resetBtn;
let stepBackBtn;
let stepForwardBtn;
let statsContainer;
let decreePanel;
let playbackCursor;
let filterBar;
let filterChips;
let filterToggle;
let filterClose;
let filterStatus;
let copyAllLogsBtn;
let nodeLabelModeSelect;
let topologyPanel;
let secondaryControls;
let playbackTools;
let decreeViewer;
let slotTape;
let resizeHandle = null;
const FRAME_WINDOW_MICROS = 50;
const NODE_LABEL_MODE_STORAGE_KEY = 'paxos.nodeLabelMode';
const RECONFIG_LOG_PRESETS = [
  { key: 'all', label: 'All', types: null },
  { key: 'flow', label: 'Flow', types: ['PmmcPropose', 'PmmcP1A', 'PmmcP1B', 'PmmcP2A', 'PmmcP2B', 'NodeCrashed', 'PartitionCreated', 'PartitionHealed'] },
  {
    key: 'reconfig',
    label: 'Reconfig',
    types: [
      'ReconfigurationRequested',
      'ReconfigurationStopStarted',
      'ReconfigurationStopCommandSent',
      'ReconfigurationStopCompleted',
      'ReconfigurationStopDecided',
      'ReconfigurationStopApplied',
      'ReconfigurationProposalObserved',
      'ReconfigurationCheckpointSelected',
      'ReconfigurationApplied',
      'ReconfigurationReady',
      'ReconfigurationNodeRetired',
      'ReconfigurationNodeRebooted',
      'ReconfigurationFailed',
    ],
  },
  { key: 'barrier', label: 'Barrier', types: ['ReconfigurationStopStarted', 'ReconfigurationStopCommandSent', 'ReconfigurationStopCompleted', 'ReconfigurationStopDecided', 'ReconfigurationStopApplied'] },
  { key: 'membership', label: 'Membership', types: ['ReconfigurationApplied', 'ReconfigurationReady', 'ReconfigurationNodeRetired', 'ReconfigurationNodeRebooted'] },
];
const scenarioTeachingNotes = {
  pmmc_reconfig_padding: {
    title: 'Padding',
    focus: 'This run keeps the stop boundary visible while late work is padded out with safe filler.',
    runningChange: 'The old runtime is shutting down, but later slots are still being absorbed rather than rejected.',
    readyNext: 'Watch the stop boundary first, then compare the old runtime winding down with the new runtime coming online.',
    runningNext: 'Use the slot tape to see which late proposals become NOOP instead of real work.',
    replayNext: 'Step through the stop boundary and padded tail if they blur together live.',
  },
  pmmc_reconfig_joint_consensus: {
    title: 'Joint Consensus',
    focus: 'This run keeps both configurations visible during the handoff instead of pretending membership changed instantly.',
    runningChange: 'The machine is transitioning while old and new membership both still matter.',
    readyNext: 'Watch the barrier on the tape, then watch which nodes remain active after the handoff.',
    runningNext: 'The topology shows who exists; the slot tape shows where the handoff actually lives.',
    replayNext: 'Use replay to isolate the moment the old membership yields to the new one.',
  },
  pmmc_reconfig_stop_sign: {
    title: 'Stop Sign',
    focus: 'This run uses a direct stop barrier: decide STOP, apply it, then boot the next machine.',
    runningChange: 'A hard stop boundary is now being chosen inside the replicated log.',
    readyNext: 'Watch when STOP is decided, then when it is applied. Those are different moments.',
    runningNext: 'The visualizer shows who is still active; the tape shows where the barrier actually sits.',
    replayNext: 'Step through decided versus applied if the barrier feels too abrupt live.',
  },
  pmmc_reconfig_delayed_stop_sign: {
    title: 'Delayed Stop Sign',
    focus: 'This run leaves a grace window after STOP is chosen, so the old runtime does not stop immediately.',
    runningChange: 'The stop boundary exists now, but the barrier does not become active all at once.',
    readyNext: 'Watch the delayed slots after STOP. They are the point of this variant.',
    runningNext: 'The tape makes the grace window legible in a way the raw event stream does not.',
    replayNext: 'Use replay to inspect the gap between STOP being chosen and the barrier actually biting.',
  },
  pmmc_reconfig_brick_wall: {
    title: 'Brick Wall',
    focus: 'This run shows the harshest case: once the barrier is active, late work just hits the wall.',
    runningChange: 'The system is approaching the point where proposals stop being rewritten and start being rejected.',
    readyNext: 'Watch the exact moment proposals stop flowing through and start bouncing off the barrier.',
    runningNext: 'The topology tells you who is alive; the tape tells you why progress stopped.',
    replayNext: 'Step through the rejection phase if the wall feels too sudden on live playback.',
  },
};

function getPreferredNodeLabelMode() {
  const stored = localStorage.getItem(NODE_LABEL_MODE_STORAGE_KEY);
  return stored === 'greek' ? 'greek' : 'number';
}

function adjustNodeRadiusForViewport() {
  const svgContainer = document.getElementById('basicProtocolSvg');
  if (!svgContainer || !visualizer) return;

  const rect = svgContainer.getBoundingClientRect();
  const containerHeight = rect.height;

  if (containerHeight < 300) {
    visualizer.nodeRadius = 92;
  } else if (containerHeight < 400) {
    visualizer.nodeRadius = 140;
  } else {
    visualizer.nodeRadius = 210;
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
  const topologyPlugin = createTopologyPanelPlugin({ container: topologyPanel });
  const slotTapePlugin = createSlotTapePlugin({ container: slotTape, maxSlots: 16 });
  const ensureLearnableSelection = {
    onCluster(_, ctx) {
      const selectedNode = ctx.state.snapshot().simulation.selectedNode;
      if (selectedNode === null || selectedNode === undefined) return;
      const roles = ctx.state.snapshot().nodes.get(selectedNode)?.role?.roles || [];
      if (roles.includes('Replica') || roles.includes('Learner')) return;
      ctx.state.selectNode(null);
    },
    onEvent({ eventType, eventData }, ctx) {
      if (eventType !== 'NodeCapabilities' || !eventData) return;
      const snapshot = ctx.state.snapshot();
      const selectedNode = snapshot.simulation.selectedNode;
      const selectedRoles = snapshot.nodes.get(selectedNode)?.role?.roles || [];
      const selectedCanLearn = selectedRoles.includes('Replica') || selectedRoles.includes('Learner');
      if (selectedCanLearn) return;

      let nextLearner = null;
      snapshot.nodes.forEach((node, nodeId) => {
        if (nextLearner !== null) return;
        const roles = node?.role?.roles || [];
        if (roles.includes('Replica') || roles.includes('Learner')) {
          nextLearner = nodeId;
        }
      });

      ctx.state.selectNode(nextLearner);
    },
  };
  const clusterRenderPlugin = {
    onCluster(clusterInfo, ctx) {
      if (ctx.state.snapshot().simulation.selectedNode === null && clusterInfo.total_nodes > 0) {
        ctx.state.selectNode(0);
      }
      adjustNodeRadiusForViewport();
      ctx.visualizer.render(clusterInfo);
      const snapshot = ctx.state.snapshot();
      if (snapshot.leaderId !== null && snapshot.leaderId !== undefined) {
        ctx.visualizer.setLeader(snapshot.leaderId);
      }
    },
  };

  controller = createPaxosDemoController({
    state,
    visualizer,
    canCommunicate,
    frameWindowMicros: FRAME_WINDOW_MICROS,
    onPlaybackUpdate: updatePlaybackControls,
    nodeLabelMode: nodeLabelModeSelect?.value || getPreferredNodeLabelMode(),
    plugins: [
      clusterRenderPlugin,
      createEventLogPlugin({
        eventLog,
        limit: 500,
        presets: RECONFIG_LOG_PRESETS,
        defaultFilter: 'reconfig',
        excludeTypes: ['NodeCapabilities', 'BatchInitialDecrees', 'LedgerDump', 'InitialDecree'],
        filterBar,
        filterChips,
        filterToggle,
        filterClose,
        filterStatus,
        copyAllButton: copyAllLogsBtn,
      }),
      createVisualizeEventsPlugin(),
      createNodeCapabilitiesPlugin(),
      ensureLearnableSelection,
      topologyPlugin,
      createReconfigPhasePlugin({
        titleEl: reconfigPhaseTitle,
        descriptionEl: reconfigPhaseDescription,
      }),
      slotTapePlugin,
      createDecreePanelPlugin({
        statsContainer,
        decreePanel,
        nodeFilter: (_node, nodeId, snapshot) => {
          const roleSet = snapshot.nodes.get(nodeId)?.role?.roles || [];
          return roleSet.includes('Replica') || roleSet.includes('Learner');
        },
        itemLabelSingular: 'command',
        itemLabelPlural: 'commands',
        itemRenderLabel: 'cmd',
        emptySelectionHint: 'Click a node to view its learned KV commands',
      }),
      createPlaybackDelayPlugin(),
    ],
  });
}

function createTopologyPanelPlugin({ container } = {}) {
  const roleByNode = new Map();
  const totals = { leaders: 0, replicas: 0, acceptors: 0 };

  const rolePatterns = [
    { key: 'Leader+Replica+Acceptor', label: 'Full node', shape: 'circle', tone: 'all' },
    { key: 'Leader+Replica', label: 'Leader + replica', shape: 'octagon', tone: 'mixed' },
    { key: 'Leader+Acceptor', label: 'Leader + acceptor', shape: 'octagon', tone: 'mixed' },
    { key: 'Replica+Acceptor', label: 'Replica + acceptor', shape: 'octagon', tone: 'mixed' },
    { key: 'Leader', label: 'Leader only', shape: 'diamond', tone: 'leader' },
    { key: 'Replica', label: 'Replica only', shape: 'triangle', tone: 'replica' },
    { key: 'Acceptor', label: 'Acceptor only', shape: 'square', tone: 'acceptor' },
  ];

  function normalizeRoles(roles = []) {
    const normalized = new Set();
    for (const role of roles) {
      if (role === 'Leader' || role === 'Proposer') normalized.add('Leader');
      if (role === 'Replica' || role === 'Learner') normalized.add('Replica');
      if (role === 'Acceptor') normalized.add('Acceptor');
    }
    return {
      leader: normalized.has('Leader'),
      replica: normalized.has('Replica'),
      acceptor: normalized.has('Acceptor'),
    };
  }

  function rolePatternKey(roleSet) {
    return ['Leader', 'Replica', 'Acceptor']
      .filter((role) => {
        if (role === 'Leader') return roleSet.leader;
        if (role === 'Replica') return roleSet.replica;
        return roleSet.acceptor;
      })
      .join('+');
  }

  function recalcTotals() {
    totals.leaders = 0;
    totals.replicas = 0;
    totals.acceptors = 0;
    for (const roleSet of roleByNode.values()) {
      if (roleSet.leader) totals.leaders += 1;
      if (roleSet.replica) totals.replicas += 1;
      if (roleSet.acceptor) totals.acceptors += 1;
    }
  }

  function render(clusterSize = 0) {
    if (!container) return;
    const nodesByPattern = new Map();
    for (const [nodeId, roleSet] of roleByNode.entries()) {
      const key = rolePatternKey(roleSet);
      if (!key) continue;
      if (!nodesByPattern.has(key)) {
        nodesByPattern.set(key, []);
      }
      nodesByPattern.get(key).push(nodeId);
    }

    const rows = rolePatterns
      .map((pattern) => {
        const nodeIds = (nodesByPattern.get(pattern.key) || []).sort((a, b) => a - b);
        if (!nodeIds.length) return '';
        const countLabel = nodeIds.length === 1 ? 'node' : 'nodes';
        const nodeChips = nodeIds
          .map((nodeId) => `<span class="topology-node-chip">${nodeShortLabel(nodeId)}</span>`)
          .join('');
        return `
          <div class="topology-role-row">
            <span class="topology-shape topology-shape-${pattern.shape} topology-tone-${pattern.tone}"></span>
            <div class="topology-role-main">
              <span class="topology-role-label">${pattern.label}</span>
              <span class="topology-role-count">${nodeIds.length} ${countLabel}</span>
            </div>
            <div class="topology-node-list">${nodeChips}</div>
          </div>
        `;
      })
      .filter(Boolean)
      .join('');

    container.innerHTML = `
      <div class="topology-head">
        <div class="topology-title">Topology</div>
      </div>
      <div class="topology-summary">
        <div class="topology-summary-item">
          <span class="topology-summary-label">Leaders</span>
          <span class="topology-summary-value">${totals.leaders}</span>
        </div>
        <div class="topology-summary-item">
          <span class="topology-summary-label">Replicas</span>
          <span class="topology-summary-value">${totals.replicas}</span>
        </div>
        <div class="topology-summary-item">
          <span class="topology-summary-label">Acceptors</span>
          <span class="topology-summary-value">${totals.acceptors}</span>
        </div>
        <div class="topology-summary-item">
          <span class="topology-summary-label">Nodes</span>
          <span class="topology-summary-value">${clusterSize}</span>
        </div>
      </div>
      <div class="topology-role-list">${rows}</div>
      <div class="topology-mini-key">
        <span class="topology-mini-key-item"><span class="topology-shape topology-shape-circle topology-tone-all"></span><span>full</span></span>
        <span class="topology-mini-key-item"><span class="topology-shape topology-shape-diamond topology-tone-leader"></span><span>leader</span></span>
        <span class="topology-mini-key-item"><span class="topology-shape topology-shape-triangle topology-tone-replica"></span><span>replica</span></span>
        <span class="topology-mini-key-item"><span class="topology-shape topology-shape-square topology-tone-acceptor"></span><span>acceptor</span></span>
        <span class="topology-mini-key-item"><span class="topology-shape topology-shape-octagon topology-tone-mixed"></span><span>mixed</span></span>
      </div>
    `;
  }

  return {
    onCluster(clusterInfo) {
      render(clusterInfo?.total_nodes || 0);
    },
    onReset() {
      roleByNode.clear();
      recalcTotals();
      render(0);
    },
    onEvent({ eventType, eventData }, ctx) {
      if (eventType !== 'NodeCapabilities' || !eventData) return;
      roleByNode.set(eventData.id, normalizeRoles(eventData.roles || []));
      recalcTotals();
      render(ctx?.state?.snapshot()?.cluster?.total_nodes || roleByNode.size);
    },
  };
}

function getScenarioTeachingNote() {
  return scenarioTeachingNotes[scenarioSelect?.value || 'pmmc_reconfig_padding']
    || scenarioTeachingNotes.pmmc_reconfig_padding;
}

function setTeachingStatus(mode, options = {}) {
  if (!statusTitle || !statusFocus || !statusChange || !statusNext) return;

  const note = getScenarioTeachingNote();
  const totalBatches = options.totalBatches ?? 0;

  switch (mode) {
    case 'ready':
      statusTitle.textContent = note.title;
      statusTitle.style.color = '#60a5fa';
      statusFocus.textContent = note.focus;
      statusChange.textContent = 'Nothing is moving yet.';
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
      statusTitle.textContent = 'Paused on the live path';
      statusTitle.style.color = '#f59e0b';
      statusFocus.textContent = note.focus;
      statusChange.textContent = 'The live run is paused at the current playback position.';
      statusNext.textContent = 'Resume to keep following the run, or step through the captured batches to inspect the transition.';
      break;
    case 'replaying':
      statusTitle.textContent = 'Catching up';
      statusTitle.style.color = '#f59e0b';
      statusFocus.textContent = note.focus;
      statusChange.textContent = 'The live run has finished, but the captured playback is still draining into the timeline.';
      statusNext.textContent = note.replayNext;
      break;
    case 'stepping':
      statusTitle.textContent = 'Step through it';
      statusTitle.style.color = '#f59e0b';
      statusFocus.textContent = note.focus;
      statusChange.textContent = 'You are moving one captured batch at a time now.';
      statusNext.textContent = note.replayNext;
      break;
    case 'complete':
      statusTitle.textContent = 'Captured run';
      statusTitle.style.color = '#34d399';
      statusFocus.textContent = note.focus;
      statusChange.textContent = `This run captured ${totalBatches} batches of protocol activity.`;
      statusNext.textContent = note.replayNext;
      break;
    case 'error':
      statusTitle.textContent = 'Run failed';
      statusTitle.style.color = '#f87171';
      statusFocus.textContent = note.focus;
      statusChange.textContent = options.message || 'The scenario did not start cleanly.';
      statusNext.textContent = 'Reset the demo and try the run again.';
      break;
    default:
      break;
  }
}

function updatePlaybackControls(playbackState) {
  if (!playbackState || !stepBackBtn || !stepForwardBtn || !togglePlayBtn) return;
  const { batchCount, cursor, playerMode, isBusy } = playbackState;
  const followLive = playerMode === 'follow';
  const hasCapturedPlayback = batchCount > 0;
  if (secondaryControls) {
    secondaryControls.hidden = !scenarioStarted && !hasCapturedPlayback;
  }
  if (playbackTools) {
    playbackTools.hidden = !hasCapturedPlayback;
  }
  if (decreeViewer) {
    decreeViewer.hidden = !scenarioStarted && !hasCapturedPlayback;
  }
  stepBackBtn.disabled = followLive || isBusy || cursor < 0;
  stepForwardBtn.disabled = followLive || isBusy || cursor + 1 >= batchCount;
  const hasUnplayed = batchCount > 0 && cursor + 1 < batchCount;

  if (scenarioFinished && !scenarioRunning && !hasUnplayed) {
    togglePlayBtn.textContent = 'Stopped';
  } else if (followLive) {
    togglePlayBtn.textContent = 'Pause';
  } else if (batchCount > 0 && scenarioStarted) {
    togglePlayBtn.textContent = 'Resume';
  } else {
    togglePlayBtn.textContent = 'Play';
  }

  if (playbackCursor) {
    const displayIndex = Math.max(0, cursor + 1);
    playbackCursor.textContent = `Batch ${displayIndex}/${batchCount}`;
  }
}

async function resetScenario() {
  scenarioStartInFlight = false;
  state.resetSimulation();
  if (controller) {
    controller.pause({ waitForFrame: false });
    controller.pauseIngestion({ untilClusterInitialized: true });
    controller.setCaptureEnabled(false);
    controller.reset({ clearHistory: true });
  }
  scenarioRunning = false;
  scenarioStarted = false;
  scenarioFinished = false;
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

  if (togglePlayBtn) {
    togglePlayBtn.textContent = 'Play';
  }
  if (scenarioSelect) {
    scenarioSelect.disabled = false;
  }
  setTeachingStatus('ready');
}

async function playScenario() {
  if (scenarioStartInFlight) return;
  const startingNewScenario = !scenarioStarted;
  scenarioStarted = true;
  scenarioFinished = false;

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
  if (scenarioSelect) {
    scenarioSelect.disabled = true;
  }
  if (togglePlayBtn) {
    togglePlayBtn.textContent = 'Pause';
  }
  setTeachingStatus('running');

  try {
    scenarioStartInFlight = true;
    const scenarioType = scenarioSelect?.value || 'pmmc_reconfig_padding';
    const scenarioNodeCount = getPmmcScenarioNodeCount(scenarioType, 5);
    const response = await fetch('/api/start-scenario', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        node_count: scenarioNodeCount,
        duration_secs: 30,
        scenario_type: scenarioType,
      }),
    });

    if (!response.ok) {
      throw new Error('Failed to start scenario');
    }

    scenarioStartInFlight = false;

    await new Promise((resolve) => {
      scenarioTimeout = setTimeout(() => {
        scenarioRunning = false;
        state.setRunning(false);
        resolve();
      }, 30000);
    });

    setTeachingStatus('replaying');

    while (
      controller &&
      controller.getPlaybackState().playerMode === 'follow' &&
      (
        controller.getPlaybackState().isBusy ||
        controller.eventQueue.length() > 0 ||
        controller.eventQueue.isProcessing() ||
        controller.hasPendingBatches() ||
        (
          controller.getPlaybackState().batchCount > 0 &&
          controller.getPlaybackState().cursor + 1 < controller.getPlaybackState().batchCount
        )
      )
    ) {
      await new Promise((resolve) => setTimeout(resolve, 250));
    }

    setTeachingStatus('complete', {
      totalBatches: controller?.getPlaybackState().batchCount || 0,
    });
    scenarioRunning = false;
    state.setRunning(false);
    scenarioFinished = true;
    if (controller) {
      controller.setCaptureEnabled(false);
      await controller.pause({ waitForFrame: false });
    }
    if (togglePlayBtn) {
      togglePlayBtn.textContent = 'Stopped';
    }
    if (scenarioSelect) {
      scenarioSelect.disabled = false;
    }
  } catch (error) {
    scenarioStartInFlight = false;
    console.error('Error running scenario:', error);
    setTeachingStatus('error', { message: String(error) });
  }
}

async function pauseScenario() {
  if (controller) {
    controller.pause({ waitForFrame: false });
  }
  if (togglePlayBtn) {
    togglePlayBtn.textContent = scenarioStarted ? 'Resume' : 'Play';
  }
  setTeachingStatus('paused');
}

async function resumePlayback() {
  if (controller) {
    controller.setCaptureEnabled(true);
    await controller.resume();
  }
  if (scenarioRunning) {
    state.setRunning(true);
  }
  if (togglePlayBtn) {
    togglePlayBtn.textContent = 'Pause';
  }
  setTeachingStatus('running');
}

async function togglePlayPause() {
  if (!controller) return;
  if (scenarioStartInFlight) return;

  const playbackState = controller.getPlaybackState();
  const hasUnplayed = playbackState.batchCount > 0 && playbackState.cursor + 1 < playbackState.batchCount;
  if (playbackState.playerMode === 'follow') {
    await pauseScenario();
    return;
  }

  if (scenarioFinished && !hasUnplayed) {
    return;
  }

  if (scenarioStarted) {
    await resumePlayback();
    return;
  }

  if (!scenarioStarted) {
    await playScenario();
    return;
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

function handleVisualizerResize() {
  if (!visualizer) return;
  if (resizeHandle) {
    cancelAnimationFrame(resizeHandle);
  }
  resizeHandle = requestAnimationFrame(() => {
    visualizer.onResize();
    resizeHandle = null;
  });
}

function initUI() {
  eventLog = document.getElementById('eventLog');
  slotTape = document.getElementById('slotTape');
  speedSlider = document.getElementById('speed');
  speedValue = document.getElementById('speedValue');
  scenarioSelect = document.getElementById('scenarioSelect');
  statusTitle = document.getElementById('statusTitle');
  statusDescription = document.getElementById('statusDescription');
  statusFocus = document.getElementById('statusFocus');
  statusChange = document.getElementById('statusChange');
  statusNext = document.getElementById('statusNext');
  reconfigPhaseTitle = document.getElementById('reconfigPhaseTitle');
  reconfigPhaseDescription = document.getElementById('reconfigPhaseDescription');
  togglePlayBtn = document.getElementById('togglePlayBtn');
  resetBtn = document.getElementById('resetBtn');
  stepBackBtn = document.getElementById('stepBackBtn');
  stepForwardBtn = document.getElementById('stepForwardBtn');
  statsContainer = document.getElementById('proposalStatsContainer');
  decreePanel = document.getElementById('decreePanel');
  playbackCursor = document.getElementById('playbackCursor');
  filterBar = document.getElementById('eventFilterBar');
  filterChips = document.getElementById('eventFilterChips');
  filterToggle = document.getElementById('eventFilterToggle');
  filterClose = document.getElementById('eventFilterClose');
  filterStatus = document.getElementById('eventFilterStatus');
  copyAllLogsBtn = document.getElementById('eventCopyAllBtn');
  nodeLabelModeSelect = document.getElementById('nodeLabelMode');
  topologyPanel = document.getElementById('topologyPanel');
  secondaryControls = document.getElementById('secondaryControls');
  playbackTools = document.getElementById('playbackTools');
  decreeViewer = document.getElementById('decree-viewer');

  visualizer = new PaxosVisualizer('basicProtocolSvg', {
    nodeRadius: 210,
    nodeCircleRadius: 27,
    nodeStateOffsetY: 38,
    centerYOffset: 15,
    useRoleShapes: true,
  });

  if (nodeLabelModeSelect) {
    nodeLabelModeSelect.value = getPreferredNodeLabelMode();
    nodeLabelModeSelect.addEventListener('change', () => {
      localStorage.setItem(NODE_LABEL_MODE_STORAGE_KEY, nodeLabelModeSelect.value);
      if (controller) {
        controller.setNodeLabelMode(nodeLabelModeSelect.value);
      }
    });
  }

  if (speedSlider) {
    speedValue.textContent = `${speedSlider.value}x`;
    speedSlider.addEventListener('input', () => {
      speedValue.textContent = `${speedSlider.value}x`;
      if (controller) {
        controller.setSpeed(parseFloat(speedSlider.value));
      }
      state.setSpeed(parseFloat(speedSlider.value));
    });
  }

  scenarioSelect?.addEventListener('change', async () => {
    await resetScenario();
  });

  togglePlayBtn?.addEventListener('click', togglePlayPause);
  resetBtn?.addEventListener('click', () => void resetScenario());
  stepBackBtn?.addEventListener('click', stepBack);
  stepForwardBtn?.addEventListener('click', stepForward);

  buildController();
  controller.connect();
  resetScenario();
  if (controller) {
    updatePlaybackControls(controller.getPlaybackState());
  }
  window.addEventListener('resize', handleVisualizerResize);
}

document.addEventListener('DOMContentLoaded', initUI);
window.addEventListener('beforeunload', () => {
  if (scenarioRunning) {
    void fetch('/api/stop-scenario', { method: 'POST' });
  }
  window.removeEventListener('resize', handleVisualizerResize);
});
