/**
 * Basic Protocol Demo (Refactored)
 * Real-time visualization of Paxos Basic Protocol using new modular foundation
 * Reduced from 682 to ~250 lines
 */

import { state } from '/demo-state.js';
import { getEventVisualizer } from '/event-visualizers.js';
import { EventQueue } from '/event-queue.js';

// UI elements
const eventLog = document.getElementById("eventLog");
const speedSlider = document.getElementById("speed");
const speedValue = document.getElementById("speedValue");
const scenarioSelect = document.getElementById("scenarioSelect");
const statusTitle = document.getElementById("statusTitle");
const statusDescription = document.getElementById("statusDescription");
const playBtn = document.getElementById("playBtn");
const pauseBtn = document.getElementById("pauseBtn");
const resetBtn = document.getElementById("resetBtn");

// Visualizer setup
const visualizer = new PaxosVisualizer("basicProtocolSvg", {
  nodeRadius: 195,
  nodeCircleRadius: 26,
});

// WebSocket and scenario state
let ws = null;
let scenarioTimeout = null;
let selectedNodeId = null;

// Event queue with custom handler
const eventQueue = new EventQueue(handleEvent, 50);

// Update speed display and state
speedSlider.addEventListener("change", (e) => {
  const speed = parseFloat(e.target.value);
  speedValue.textContent = speed.toFixed(2) + "x";
  state.setSpeed(speed);
});

function adjustNodeRadiusForViewport() {
  const svgContainer = document.getElementById("basicProtocolSvg");
  if (!svgContainer) return;

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

function setupWebSocket() {
  if (ws) ws.close();
  ws = new WebSocket(getWebSocketURL("/ws"));

  ws.onmessage = (event) => {
    const message = JSON.parse(event.data);

    if (message.ClusterInitialized) {
      state.initialize(message.ClusterInitialized);
      adjustNodeRadiusForViewport();
      visualizer.render(message.ClusterInitialized);
      updateCounts();
    } else if (message.Event) {
      const eventType = Object.keys(message.Event)[0];
      const eventData = message.Event[eventType];

      if (!eventData) return;

      // Only queue events if running
      if (state.snapshot().simulation.running) {
        eventQueue.push({
          eventType,
          eventData,
          created_at: eventData.created_at || performance.now()
        });
      }
    }
  };
}

/**
 * Handle a single event - called by EventQueue
 */
async function handleEvent(queuedEvent) {
  const { eventType, eventData } = queuedEvent;
  const viz = getEventVisualizer(eventType);

  if (!viz) return;

  // Log event
  addLog(viz.format(eventData), viz.color);

  // Track count
  state.incrementEventCount(eventType);
  updateCounts();

  // Get speed-adjusted animation duration
  const snapshot = state.snapshot();
  const speed = snapshot.simulation.speed;
  const duration = Math.max(200, (500 / speed) * 0.67);

  // Handle event-specific visualization
  switch (eventType) {
    case "Proposal":
      visualizer.setNodeState(eventData.id, "propose");
      visualizer.activateNode(eventData.id, viz.color);
      const proposalBeams = [];
      for (let i = 0; i < snapshot.cluster.total_nodes; i++) {
        if (i !== eventData.id && canCommunicate(eventData.id, i)) {
          proposalBeams.push(
            visualizer.drawBeam(eventData.id, i, viz.color, duration, "solid")
          );
        }
      }
      if (proposalBeams.length > 0) await Promise.all(proposalBeams);
      break;

    case "Promise":
      visualizer.setNodeState(eventData.id, "promise");
      visualizer.activateNode(eventData.id, viz.color);
      if (eventData.from !== undefined && eventData.from !== eventData.id) {
        await visualizer.drawBeam(eventData.id, eventData.from, viz.color, duration, "dashed");
      }
      break;

    case "Accept":
      visualizer.setNodeState(eventData.id, "accept");
      visualizer.activateNode(eventData.id, viz.color);
      const acceptBeams = [];
      if (eventData.quorum && Array.isArray(eventData.quorum)) {
        for (const nodeId of eventData.quorum) {
          if (nodeId !== eventData.id) {
            acceptBeams.push(
              visualizer.drawBeam(eventData.id, nodeId, viz.color, duration, "solid")
            );
          }
        }
      }
      if (acceptBeams.length > 0) await Promise.all(acceptBeams);
      break;

    case "Accepted":
      visualizer.setNodeState(eventData.id, "voted");
      visualizer.activateNode(eventData.id, viz.color);
      if (eventData.from !== undefined && eventData.from !== eventData.id) {
        await visualizer.drawBeam(eventData.id, eventData.from, viz.color, duration, "dotted");
      }
      break;

    case "LearnedValue":
      visualizer.setNodeState(eventData.id, "learn");
      visualizer.activateNode(eventData.id, viz.color);
      if (eventData.decree_num !== undefined) {
        state.addDecree(eventData.id, {
          decree_num: eventData.decree_num,
          decree: formatDecree(eventData),
          timestamp: Date.now()
        });
      }
      break;

    case "Success":
      // Highlight the original proposer if different from the learner broadcasting
      if (eventData.ballot_proposer !== undefined && eventData.ballot_proposer !== eventData.from) {
        visualizer.activateNode(eventData.ballot_proposer, "#fbbf24"); // Yellow highlight
      }
      visualizer.setNodeState(eventData.id, "success");
      visualizer.activateNode(eventData.id, viz.color);
      if (eventData.from !== undefined && canCommunicate(eventData.from, eventData.id)) {
        await visualizer.drawBeam(eventData.from, eventData.id, viz.color, duration, "dashed");
      }
      break;

    case "PartitionCreated":
      for (const nodeId of (eventData.partition_b || [])) {
        state.setNodePartitioned(nodeId, true);
        visualizer.setNodePartitioned(nodeId, true);
      }
      break;

    case "PartitionHealed":
      const allNodes = [...(eventData.partition_a || []), ...(eventData.partition_b || [])];
      for (const nodeId of allNodes) {
        state.setNodePartitioned(nodeId, false);
        visualizer.setNodePartitioned(nodeId, false);
      }
      break;
  }

  // Pause for readability
  await new Promise(r => setTimeout(r, Math.max(300, 400 / speed)));
}

function canCommunicate(from, to) {
  const snapshot = state.snapshot();
  const fromNode = snapshot.nodes.get(from);
  const toNode = snapshot.nodes.get(to);
  if (!fromNode || !toNode) return true;
  // Can communicate if both have same partition status
  return fromNode.partitioned === toNode.partitioned;
}

function formatDecree(event) {
  if (event.value === 'NOOP') return 'NOOP';
  if (event.value && typeof event.value === 'object' && event.value.EnactDecree) {
    return event.value.EnactDecree.law;
  }
  return `Decree #${event.decree_num}`;
}

function addLog(text, color) {
  const item = document.createElement("div");
  item.className = "event-item";
  item.style.borderLeftColor = color;
  item.textContent = text;
  eventLog.insertBefore(item, eventLog.firstChild);
  while (eventLog.children.length > 30) {
    eventLog.removeChild(eventLog.lastChild);
  }
}

function updateCounts() {
  const snapshot = state.snapshot();
  document.getElementById("nextBallotCount").textContent = snapshot.eventCounts.Proposal || 0;
  document.getElementById("lastVoteCount").textContent = snapshot.eventCounts.Promise || 0;
  document.getElementById("beginBallotCount").textContent = snapshot.eventCounts.Accept || 0;
  document.getElementById("votedCount").textContent = snapshot.eventCounts.Accepted || 0;
  document.getElementById("learnedCount").textContent = snapshot.eventCounts.LearnedValue || 0;
  document.getElementById("successCount").textContent = snapshot.eventCounts.Success || 0;

  updateProposalStats();
}

function updateProposalStats() {
  const snapshot = state.snapshot();
  if (!snapshot.cluster) return;

  const statsContainer = document.getElementById("proposalStatsContainer");
  if (!statsContainer) return;

  let html = "<div class='proposal-stats'>";
  for (let nodeId = 0; nodeId < snapshot.cluster.total_nodes; nodeId++) {
    const node = snapshot.nodes.get(nodeId);
    const decreeCount = node?.decrees.length || 0;
    const isSelected = selectedNodeId === nodeId;
    const selectClass = isSelected ? "selected" : "";
    html += `<div class='proposal-stat-item ${selectClass}' data-node-id='${nodeId}' onclick='selectNode(${nodeId})'><span class='node-id'>N${nodeId}:</span> ${decreeCount}</div>`;
  }
  html += "</div>";
  statsContainer.innerHTML = html;

  updateDecreeDisplay();
}

function selectNode(nodeId) {
  selectedNodeId = selectedNodeId === nodeId ? null : nodeId;
  updateProposalStats();
}

function updateDecreeDisplay() {
  const decreePanel = document.getElementById("decreePanel");
  if (!decreePanel) return;

  if (selectedNodeId === null) {
    decreePanel.innerHTML = "<p class='decree-hint'>Click a node to view its learned decrees</p>";
    return;
  }

  const snapshot = state.snapshot();
  const node = snapshot.nodes.get(selectedNodeId);
  if (!node || node.decrees.length === 0) {
    decreePanel.innerHTML = `<p class='decree-hint'>Node ${selectedNodeId} has not learned any decrees yet</p>`;
    return;
  }

  const sortedDecrees = [...node.decrees].sort((a, b) => a.decree_num - b.decree_num);
  let html = `<div class='decree-content'><div class='decree-node-label'>Node ${selectedNodeId} (${node.decrees.length} decrees)</div><div class='decree-list'>`;
  
  for (const decree of sortedDecrees) {
    html += `<div class='decree-item'><div class='decree-text'>[${decree.decree_num}] "${decree.decree}"</div></div>`;
  }
  
  html += `</div></div>`;
  decreePanel.innerHTML = html;
}

async function resetScenario() {
  state.resetSimulation();
  eventQueue.clear();
  selectedNodeId = null;
  updateCounts();
  eventLog.innerHTML = "";

  try {
    await fetch("/api/stop-scenario", { method: "POST" });
  } catch (error) {
    console.error("Error stopping scenario:", error);
  }

  playBtn.disabled = false;
  pauseBtn.disabled = true;
  scenarioSelect.disabled = false;
  statusTitle.textContent = "Ready";
  statusDescription.textContent = "Click Play to start the demonstration";
  statusTitle.style.color = "#60a5fa";
}

async function playScenario() {
  const scenarioName = scenarioSelect.value;

  state.setRunning(true);
  playBtn.disabled = true;
  pauseBtn.disabled = false;
  scenarioSelect.disabled = true;

  statusTitle.textContent = "Running";
  statusDescription.textContent = "Scenario in progress...";
  statusTitle.style.color = "#60a5fa";

  try {
    const response = await fetch("/api/start-scenario", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        node_count: 5,
        duration_secs: 30,
        scenario_type: scenarioName,
      }),
    });

    if (!response.ok) {
      throw new Error("Failed to start scenario");
    }

    // Wait for scenario to complete
    await new Promise((resolve) => {
      scenarioTimeout = setTimeout(() => {
        state.setRunning(false);
        resolve();
      }, 30000);
    });

    // Wait for event queue to drain
    statusTitle.textContent = "Processing";
    statusDescription.textContent = "Waiting for all events to visualize...";
    statusTitle.style.color = "#f59e0b";

    while (eventQueue.length() > 0 || eventQueue.isProcessing()) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  } catch (error) {
    console.error("Error starting scenario:", error);
    statusTitle.style.color = "#ef4444";
    statusTitle.textContent = "Error";
    statusDescription.textContent = error.message;
  }

  state.setRunning(false);
  playBtn.disabled = false;
  pauseBtn.disabled = true;
  scenarioSelect.disabled = false;

  const snapshot = state.snapshot();
  const totalEvents = Object.values(snapshot.eventCounts).reduce((a, b) => a + b, 0);

  statusTitle.textContent = "Complete";
  statusTitle.style.color = "#34d399";
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
  statusTitle.textContent = "Paused";
  statusTitle.style.color = "#f59e0b";
}

// Button event listeners
playBtn.addEventListener("click", playScenario);
pauseBtn.addEventListener("click", pauseScenario);
resetBtn.addEventListener("click", resetScenario);

// Initialize
document.addEventListener("DOMContentLoaded", () => {
  // Set initial speed from slider
  const initialSpeed = parseFloat(speedSlider.value);
  state.setSpeed(initialSpeed);
  setupWebSocket();
  resetScenario();
});
