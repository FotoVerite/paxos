/**
 * Basic Protocol Demo
 * Plugin-based visualization for Paxos Basic Protocol
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

let controller = null;
let visualizer = null;
let scenarioTimeout = null;
let scenarioRunning = false;
let scenarioStarted = false;

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
const FRAME_WINDOW_MICROS = 50;

function adjustNodeRadiusForViewport() {
  const svgContainer = document.getElementById('basicProtocolSvg');
  if (!svgContainer || !visualizer) return;

  const rect = svgContainer.getBoundingClientRect();
  const containerHeight = rect.height;

  if (containerHeight < 300) {
    visualizer.nodeRadius = 80;
  } else if (containerHeight < 400) {
    visualizer.nodeRadius = 120;
  } else {
    visualizer.nodeRadius = 195;
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
    plugins: [
      clusterRenderPlugin,
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
      createDecreePanelPlugin({ statsContainer, decreePanel }),
      createPlaybackDelayPlugin(),
    ],
  });
}

function updatePlaybackControls(playbackState) {
  if (!playbackState || !stepBackBtn || !stepForwardBtn || !togglePlayBtn) return;
  const { batchCount, cursor, playerMode, isBusy } = playbackState;
  const followLive = playerMode === 'follow';
  stepBackBtn.disabled = followLive || isBusy || cursor < 0;
  stepForwardBtn.disabled = followLive || isBusy || cursor + 1 >= batchCount;

  if (followLive) {
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

  scenarioSelect.disabled = false;
  if (togglePlayBtn) {
    togglePlayBtn.textContent = 'Play';
  }
  statusTitle.textContent = 'Ready';
  statusDescription.textContent = 'Click Play to start the demonstration';
  statusTitle.style.color = '#60a5fa';
}

async function playScenario() {
  const scenarioName = scenarioSelect.value;
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
  scenarioSelect.disabled = true;
  if (togglePlayBtn) {
    togglePlayBtn.textContent = 'Pause';
  }

  statusTitle.textContent = 'Running';
  statusDescription.textContent = 'Scenario in progress...';
  statusTitle.style.color = '#60a5fa';

  try {
    const response = await fetch('/api/start-scenario', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        node_count: 5,
        duration_secs: 30,
        scenario_type: scenarioName,
      }),
    });

    if (!response.ok) {
      throw new Error('Failed to start scenario');
    }

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
      (controller.eventQueue.length() > 0 ||
        controller.eventQueue.isProcessing() ||
        controller.hasPendingBatches())
    ) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  } catch (error) {
    console.error('Error starting scenario:', error);
    statusTitle.style.color = '#ef4444';
    statusTitle.textContent = 'Error';
    statusDescription.textContent = error.message;
  }

  scenarioRunning = false;
  state.setRunning(false);
  if (controller) {
    controller.setCaptureEnabled(false);
    controller.pause({ waitForFrame: false });
  }
  scenarioSelect.disabled = false;
  if (togglePlayBtn) {
    togglePlayBtn.textContent = 'Play';
  }

  const totalEvents = controller ? controller.getCapturedEventCount() : 0;

  statusTitle.textContent = 'Complete';
  statusTitle.style.color = '#34d399';
  statusDescription.textContent = `Scenario finished - ${totalEvents} total events visualized`;
}

async function pauseScenario() {
  if (controller) {
    controller.pause({ waitForFrame: false });
  }
  scenarioSelect.disabled = false;
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
  scenarioSelect.disabled = true;
  if (togglePlayBtn) {
    togglePlayBtn.textContent = 'Pause';
  }
  statusTitle.textContent = 'Running';
  statusTitle.style.color = '#60a5fa';
}

async function togglePlayPause() {
  if (!controller) return;
  const playback = controller.getPlaybackState();

  if (playback.playerMode === 'follow') {
    await pauseScenario();
    return;
  }

  if (playback && playback.batchCount > 0 && scenarioStarted) {
    await resumePlayback();
    return;
  }

  await playScenario();
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

document.addEventListener('DOMContentLoaded', () => {
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
  playbackCursor = document.getElementById('playbackCursor');
  statsContainer = document.getElementById('proposalStatsContainer');
  decreePanel = document.getElementById('decreePanel');
  filterBar = document.getElementById('eventFilterBar');
  filterChips = document.getElementById('eventFilterChips');
  filterToggle = document.getElementById('eventFilterToggle');
  filterClose = document.getElementById('eventFilterClose');
  filterStatus = document.getElementById('eventFilterStatus');

  visualizer = new PaxosVisualizer('basicProtocolSvg', {
    nodeRadius: 195,
    nodeCircleRadius: 26,
  });

  speedSlider.addEventListener('change', (e) => {
    const speed = parseFloat(e.target.value);
    speedValue.textContent = speed.toFixed(2) + 'x';
    state.setSpeed(speed);
  });

  scenarioSelect.addEventListener('change', async () => {
    if (!scenarioStarted) {
      return;
    }
    await resetScenario();
  });

  if (togglePlayBtn) togglePlayBtn.addEventListener('click', togglePlayPause);
  resetBtn.addEventListener('click', resetScenario);
  if (stepBackBtn) stepBackBtn.addEventListener('click', stepBack);
  if (stepForwardBtn) stepForwardBtn.addEventListener('click', stepForward);

  const initialSpeed = parseFloat(speedSlider.value);
  state.setSpeed(initialSpeed);

  buildController();
  controller.connect();
  resetScenario();

  if (controller) {
    updatePlaybackControls(controller.getPlaybackState());
  }
});
