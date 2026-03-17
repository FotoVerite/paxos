import { state } from '/demo-state.js';
import { createPaxosDemoController } from '/js/paxos-demo/core/runtime.js';
import { createEventLogPlugin } from '/js/paxos-demo/plugins/event-log.js';
import { createVisualizeEventsPlugin } from '/js/paxos-demo/plugins/visualize-events.js';
import { createDecreePanelPlugin } from '/js/paxos-demo/plugins/decree-panel.js';
import { createPlaybackDelayPlugin } from '/js/paxos-demo/plugins/playback-delay.js';
import { createNodeCapabilitiesPlugin } from '/js/paxos-demo/plugins/node-capabilities.js';
import { createReconfigPhasePlugin } from '/js/paxos-demo/plugins/reconfig-phase.js';
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
let resizeHandle = null;
const FRAME_WINDOW_MICROS = 50;
const NODE_LABEL_MODE_STORAGE_KEY = 'paxos.nodeLabelMode';
const PMMC_LOG_PRESETS = [
  { key: 'all', label: 'All', types: null },
  { key: 'flow', label: 'Flow', types: ['PmmcPropose', 'PmmcP1A', 'PmmcP1B', 'PmmcP2A', 'PmmcP2B', 'NodeCrashed', 'PartitionCreated', 'PartitionHealed'] },
  { key: 'leader', label: 'Leader', types: ['LeaderElected', 'LeaderSteppedDown', 'NodeCrashed', 'PartitionCreated', 'PartitionHealed', 'PmmcAdopted', 'PmmcPreempted', 'PmmcHeartbeat'] },
  { key: 'acks', label: 'Acks', types: ['PmmcAck'] },
];
const PMMC_CLIENT_DEBUG = true;
const scenarioTeachingNotes = {
  pmmc_single_client: {
    title: 'Single Client',
    focus: 'Start with one client and follow how a single request becomes ordered state.',
    runningChange: 'The request path is live now: propose, admit, accept, apply.',
    readyNext: 'Watch the request move from the client edge into the replicated log.',
    runningNext: 'Notice where leadership matters, and where replicas only catch up after the slot is chosen.',
    replayNext: 'Step through the captured batches if you want to isolate the handoff from leader to replicas.',
  },
  pmmc_leader_crash: {
    title: 'Leader Crash',
    focus: 'This run is about continuity after the obvious failure. The client keeps pressure on while leadership breaks.',
    runningChange: 'The request path now has to survive a leadership handoff.',
    readyNext: 'Watch when the first leader stops being useful, then see which node actually takes over.',
    runningNext: 'Notice how client intent stays the same while the leadership path changes underneath it.',
    replayNext: 'Use step mode to inspect the exact gap between crash, takeover, and resumed progress.',
  },
  pmmc_replica_crash_failover: {
    title: 'Replica Crash Failover',
    focus: 'This run separates the ordering path from the state machine edge. Replicas can fail without immediately breaking ordering.',
    runningChange: 'One replica drops out, but the rest of the pipeline keeps moving.',
    readyNext: 'Watch the chosen commands continue even after one replica disappears from the apply path.',
    runningNext: 'Notice what the leader and acceptors keep doing while replica state shifts.',
    replayNext: 'Step through the batches if you want to separate log progress from application progress.',
  },
  pmmc_leader_partition_heal: {
    title: 'Leader Partition',
    focus: 'This is about split communication, not just a dead node. The leader is still alive, but not useful everywhere.',
    runningChange: 'The request path now depends on who can still hear whom.',
    readyNext: 'Watch the moment the partition makes the current leader ineffective, then watch recovery after heal.',
    runningNext: 'Notice how topology, not just leadership, controls whether the client request can keep moving.',
    replayNext: 'Use replay to inspect the before, during, and after of the partition.',
  },
  pmmc_acceptor_majority_loss_then_recover: {
    title: 'Acceptor Majority Loss',
    focus: 'This run is about quorum pressure. The protocol can look busy long before it can make progress again.',
    runningChange: 'Messages are still moving, but the acceptor majority is gone for part of the run.',
    readyNext: 'Watch for the difference between traffic and actual progress.',
    runningNext: 'Notice when the pipeline starts working again: it is the quorum returning that matters, not just more messages.',
    replayNext: 'Step through this one if the loss and recovery blur together on live playback.',
  },
  pmmc_staggered_leader_join: {
    title: 'Staggered Leader Join',
    focus: 'This run is about leadership arriving unevenly instead of being cleanly installed from the start.',
    runningChange: 'The ordering path is being assembled while requests are already in flight.',
    readyNext: 'Watch which leader actually becomes useful, not just which leader exists on paper.',
    runningNext: 'Notice how the client path stabilizes only after the leadership path settles.',
    replayNext: 'Use replay to compare the early unstable steps with the later steady state.',
  },
  pmmc_role_split: {
    title: 'Role Split',
    focus: 'This is the architectural version of the story. Leaders, acceptors, and replicas are fully split out.',
    runningChange: 'The request path is now visibly distributed across distinct roles instead of bundled into one node type.',
    readyNext: 'Watch who orders, who votes, and who applies. They are not the same machines anymore.',
    runningNext: 'Notice how the system gets clearer once the roles stop pretending to be one thing.',
    replayNext: 'Step through this one if you want to see the role boundaries without the live motion.',
  },
};

function extractClientDebugInfo(cmd) {
  if (!cmd || typeof cmd !== 'object') return { clientId: null, requestId: null };
  if (cmd.ClientRequest && typeof cmd.ClientRequest === 'object') {
    return {
      clientId: cmd.ClientRequest.client_id ?? null,
      requestId: cmd.ClientRequest.request_id ?? null,
    };
  }
  return { clientId: null, requestId: null };
}

function createPmmcEventDebugPlugin() {
  let seq = 0;
  return {
    onEvent({ eventType, eventData }) {
      if (!PMMC_CLIENT_DEBUG) return;
      if (eventType !== 'PmmcPropose' && eventType !== 'LearnedValue') return;
      seq += 1;
      if (eventType === 'PmmcPropose') {
        const info = extractClientDebugInfo(eventData?.cmd);
        console.debug(`[PMMC DEBUG #${seq}] PmmcPropose slot=${eventData?.slot ?? '?'} node=${eventData?.id ?? '?'} client=${info.clientId ?? 'none'} req=${info.requestId ?? 'none'}`, eventData);
        return;
      }
      const info = extractClientDebugInfo(eventData?.value);
      console.debug(`[PMMC DEBUG #${seq}] LearnedValue decree=${eventData?.decree_num ?? '?'} node=${eventData?.id ?? '?'} client=${info.clientId ?? 'none'} req=${info.requestId ?? 'none'}`, eventData);
    },
  };
}

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
        presets: PMMC_LOG_PRESETS,
        defaultFilter: 'flow',
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
      createPmmcEventDebugPlugin(),
      createReconfigPhasePlugin({
        titleEl: reconfigPhaseTitle,
        descriptionEl: reconfigPhaseDescription,
      }),
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
  return scenarioTeachingNotes[scenarioSelect?.value || 'pmmc_single_client']
    || scenarioTeachingNotes.pmmc_single_client;
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
    const scenarioType = scenarioSelect?.value || 'pmmc_single_client';
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
