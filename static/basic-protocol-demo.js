/**
 * Basic Protocol Demo
 * Plugin-based visualization for Paxos Basic Protocol
 */

import { state } from '/demo-state.js';
import { createPaxosDemoController } from '/js/paxos-demo/paxos-demo-controller.js';
import { createEventLogPlugin } from '/js/paxos-demo/plugins/event-log.js';
import { createEventCountsPlugin } from '/js/paxos-demo/plugins/event-counts.js';
import { createVisualizeEventsPlugin } from '/js/paxos-demo/plugins/visualize-events.js';
import { createPartitionStatePlugin } from '/js/paxos-demo/plugins/partition-state.js';
import { createDecreePanelPlugin } from '/js/paxos-demo/plugins/decree-panel.js';
import { createPlaybackDelayPlugin } from '/js/paxos-demo/plugins/playback-delay.js';

let controller = null;
let visualizer = null;
let scenarioTimeout = null;

let eventLog;
let speedSlider;
let speedValue;
let scenarioSelect;
let statusTitle;
let statusDescription;
let playBtn;
let pauseBtn;
let resetBtn;
let statsContainer;
let decreePanel;

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
    },
  };

  controller = createPaxosDemoController({
    state,
    visualizer,
    canCommunicate,
    plugins: [
      clusterRenderPlugin,
      createEventLogPlugin({ eventLog }),
      createEventCountsPlugin(),
      createVisualizeEventsPlugin({
        skip: new Set(['PartitionCreated', 'PartitionHealed']),
      }),
      createPartitionStatePlugin(),
      createDecreePanelPlugin({ statsContainer, decreePanel }),
      createPlaybackDelayPlugin(),
    ],
  });
}

async function resetScenario() {
  state.resetSimulation();
  if (controller) controller.reset();
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
  statusTitle.textContent = 'Ready';
  statusDescription.textContent = 'Click Play to start the demonstration';
  statusTitle.style.color = '#60a5fa';
}

async function playScenario() {
  const scenarioName = scenarioSelect.value;

  state.setRunning(true);
  playBtn.disabled = true;
  pauseBtn.disabled = false;
  scenarioSelect.disabled = true;

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
        state.setRunning(false);
        resolve();
      }, 30000);
    });

    statusTitle.textContent = 'Processing';
    statusDescription.textContent = 'Waiting for all events to visualize...';
    statusTitle.style.color = '#f59e0b';

    while (controller && (controller.eventQueue.length() > 0 || controller.eventQueue.isProcessing())) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  } catch (error) {
    console.error('Error starting scenario:', error);
    statusTitle.style.color = '#ef4444';
    statusTitle.textContent = 'Error';
    statusDescription.textContent = error.message;
  }

  state.setRunning(false);
  playBtn.disabled = false;
  pauseBtn.disabled = true;
  scenarioSelect.disabled = false;

  const snapshot = state.snapshot();
  const totalEvents = Object.values(snapshot.eventCounts).reduce((a, b) => a + b, 0);

  statusTitle.textContent = 'Complete';
  statusTitle.style.color = '#34d399';
  statusDescription.textContent = `Scenario finished - ${totalEvents} total events visualized`;
}

function pauseScenario() {
  state.setRunning(false);
  playBtn.disabled = false;
  pauseBtn.disabled = true;
  scenarioSelect.disabled = false;

  if (scenarioTimeout) {
    clearTimeout(scenarioTimeout);
  }

  statusTitle.textContent = 'Paused';
  statusTitle.style.color = '#f59e0b';
}

document.addEventListener('DOMContentLoaded', () => {
  eventLog = document.getElementById('eventLog');
  speedSlider = document.getElementById('speed');
  speedValue = document.getElementById('speedValue');
  scenarioSelect = document.getElementById('scenarioSelect');
  statusTitle = document.getElementById('statusTitle');
  statusDescription = document.getElementById('statusDescription');
  playBtn = document.getElementById('playBtn');
  pauseBtn = document.getElementById('pauseBtn');
  resetBtn = document.getElementById('resetBtn');
  statsContainer = document.getElementById('proposalStatsContainer');
  decreePanel = document.getElementById('decreePanel');

  visualizer = new PaxosVisualizer('basicProtocolSvg', {
    nodeRadius: 195,
    nodeCircleRadius: 26,
  });

  speedSlider.addEventListener('change', (e) => {
    const speed = parseFloat(e.target.value);
    speedValue.textContent = speed.toFixed(2) + 'x';
    state.setSpeed(speed);
  });

  playBtn.addEventListener('click', playScenario);
  pauseBtn.addEventListener('click', pauseScenario);
  resetBtn.addEventListener('click', resetScenario);

  const initialSpeed = parseFloat(speedSlider.value);
  state.setSpeed(initialSpeed);

  buildController();
  controller.connect();
  resetScenario();
});
