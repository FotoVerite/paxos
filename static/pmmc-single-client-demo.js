import { state } from '/demo-state.js';
import { createPaxosDemoController } from '/js/paxos-demo/core/runtime.js';
import { createEventLogPlugin } from '/js/paxos-demo/plugins/event-log.js';
import { createVisualizeEventsPlugin } from '/js/paxos-demo/plugins/visualize-events.js';
import { createDecreePanelPlugin } from '/js/paxos-demo/plugins/decree-panel.js';
import { createPlaybackDelayPlugin } from '/js/paxos-demo/plugins/playback-delay.js';
import { createNodeCapabilitiesPlugin } from '/js/paxos-demo/plugins/node-capabilities.js';
import { createClientStripPlugin } from '/js/paxos-demo/plugins/client-strip.js';

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
let clientStrip;
let clientFeed;
let clientSnapshot;
let topologyPanel;
const FRAME_WINDOW_MICROS = 50;
const NODE_LABEL_MODE_STORAGE_KEY = 'paxos.nodeLabelMode';
const PMMC_LOG_PRESETS = [
  { key: 'all', label: 'All', types: null },
  { key: 'flow', label: 'Flow', types: ['PmmcPropose', 'PmmcP1A', 'PmmcP1B', 'PmmcP2A', 'PmmcP2B', 'NodeCrashed', 'PartitionCreated', 'PartitionHealed'] },
  { key: 'leader', label: 'Leader', types: ['LeaderElected', 'LeaderSteppedDown', 'NodeCrashed', 'PartitionCreated', 'PartitionHealed', 'PmmcAdopted', 'PmmcPreempted', 'PmmcHeartbeat'] },
  { key: 'acks', label: 'Acks', types: ['PmmcAck'] },
];
const PMMC_CLIENT_DEBUG = true;

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
      topologyPlugin,
      createPmmcEventDebugPlugin(),
      createClientStripPlugin({
        container: clientStrip,
        feed: clientFeed,
        snapshotContainer: clientSnapshot,
      }),
      createDecreePanelPlugin({
        statsContainer,
        decreePanel,
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

  function normalizeRoles(roles = []) {
    return {
      leader: roles.includes('Leader'),
      replica: roles.includes('Replica'),
      acceptor: roles.includes('Acceptor'),
    };
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
    container.innerHTML = `
      <div class="topology-title">Topology</div>
      <div class="topology-grid">
        <div class="topology-item topology-item-leader"><span>Leaders</span><span class="topology-value">${totals.leaders}</span></div>
        <div class="topology-item topology-item-replica"><span>Replicas</span><span class="topology-value">${totals.replicas}</span></div>
        <div class="topology-item topology-item-acceptor"><span>Acceptors</span><span class="topology-value">${totals.acceptors}</span></div>
        <div class="topology-item topology-item-node"><span>Nodes</span><span class="topology-value">${clusterSize}</span></div>
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

function updatePlaybackControls(playbackState) {
  if (!playbackState || !stepBackBtn || !stepForwardBtn || !togglePlayBtn) return;
  const { batchCount, cursor, playerMode, isBusy } = playbackState;
  const followLive = playerMode === 'follow';
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
  statusTitle.textContent = 'Ready';
  statusDescription.textContent = 'Click Play to start the PMMC single-client scenario';
  statusTitle.style.color = '#60a5fa';
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

  statusTitle.textContent = 'Running';
  statusDescription.textContent = 'PMMC single-client scenario in progress...';
  statusTitle.style.color = '#60a5fa';

  try {
    scenarioStartInFlight = true;
    const scenarioType = scenarioSelect?.value || 'pmmc_single_client';
    const scenarioNodeCount =
      scenarioType === 'pmmc_role_split' ||
      scenarioType === 'pmmc_leader_crash' ||
      scenarioType === 'pmmc_replica_crash_failover' ||
      scenarioType === 'pmmc_leader_partition_heal' ||
      scenarioType === 'pmmc_acceptor_majority_loss_then_recover' ||
      scenarioType === 'pmmc_staggered_leader_join'
        ? 7
        : 5;
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

    statusTitle.textContent = 'Processing';
    statusDescription.textContent = 'Waiting for all events to visualize...';
    statusTitle.style.color = '#f59e0b';

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

    statusTitle.textContent = 'Complete';
    statusDescription.textContent = 'Scenario complete. Use Step controls or Reset to rerun.';
    statusTitle.style.color = '#34d399';
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
    statusTitle.textContent = 'Error';
    statusDescription.textContent = String(error);
    statusTitle.style.color = '#f87171';
  }
}

async function pauseScenario() {
  if (controller) {
    controller.pause({ waitForFrame: false });
  }
  if (togglePlayBtn) {
    togglePlayBtn.textContent = scenarioStarted ? 'Resume' : 'Play';
  }
  statusTitle.textContent = 'Paused';
  statusTitle.style.color = '#f59e0b';
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
  statusTitle.textContent = 'Running';
  statusTitle.style.color = '#60a5fa';
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

function initUI() {
  eventLog = document.getElementById('eventLog');
  speedSlider = document.getElementById('speed');
  speedValue = document.getElementById('speedValue');
  scenarioSelect = document.getElementById('scenarioSelect');
  statusTitle = document.getElementById('statusTitle');
  statusDescription = document.getElementById('statusDescription');
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
  clientStrip = document.getElementById('clientStrip');
  clientFeed = document.getElementById('clientFeed');
  clientSnapshot = document.getElementById('clientSnapshot');
  topologyPanel = document.getElementById('topologyPanel');

  visualizer = new PaxosVisualizer('basicProtocolSvg', {
    nodeRadius: 210,
    nodeCircleRadius: 27,
    nodeStateOffsetY: 38,
    centerYOffset: 15,
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
}

document.addEventListener('DOMContentLoaded', initUI);
window.addEventListener('beforeunload', () => {
  if (scenarioRunning) {
    void fetch('/api/stop-scenario', { method: 'POST' });
  }
});
