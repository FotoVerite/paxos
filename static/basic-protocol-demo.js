/**
 * Basic Protocol Demo
 * Real-time visualization of Paxos Basic Protocol using actual cluster events
 */

const eventColorMap = {
  Proposal: { name: "NextBallot", color: "#60a5fa" },
  Promise: { name: "LastVote", color: "#ec4899" },
  Accept: { name: "BeginBallot", color: "#f59e0b" },
  Accepted: { name: "Voted", color: "#10b981" },
  LearnedValue: { name: "LearnedValue", color: "#34d399" },
  InitialDecree: { name: "InitialDecree", color: "#8b5cf6" },
  Success: { name: "Success", color: "#6366f1" },
};

const visualizer = new PaxosVisualizer("basicProtocolSvg", {
  nodeRadius: 195,
  nodeCircleRadius: 26,
  eventColors: {
    nextballot: "#60a5fa",
    lastvote: "#ec4899",
    beginballot: "#f59e0b",
    voted: "#10b981",
    success: "#6366f1",
  },
});

// Adjust nodeRadius based on actual SVG container size
function adjustNodeRadiusForViewport() {
  const svgContainer = document.getElementById("basicProtocolSvg");
  if (!svgContainer) return;

  const rect = svgContainer.getBoundingClientRect();
  const containerHeight = rect.height;

  // Adjust radius based on container height - leave padding
  if (containerHeight < 300) {
    visualizer.nodeRadius = 80;
  } else if (containerHeight < 400) {
    visualizer.nodeRadius = 120;
  } else {
    visualizer.nodeRadius = 195;
  }
}

const eventLog = document.getElementById("eventLog");
const speedSlider = document.getElementById("speed");
const speedValue = document.getElementById("speedValue");
const scenarioSelect = document.getElementById("scenarioSelect");
const statusTitle = document.getElementById("statusTitle");
const statusDescription = document.getElementById("statusDescription");
const playBtn = document.getElementById("playBtn");
const pauseBtn = document.getElementById("pauseBtn");
const resetBtn = document.getElementById("resetBtn");

let isRunning = false;
let clusterInfo = null;
let partitionedNodes = new Set(); // Track which nodes are partitioned
let eventCounts = {
  Proposal: 0,
  Promise: 0,
  Accept: 0,
  Accepted: 0,
  LearnedValue: 0,
  InitialDecree: 0,
  Success: 0,
};

// Per-node decree tracking
let nodeDecrees = {}; // node_id -> { count: number, decrees: [{ decree_num: number, decree: string, timestamp: number }] }
let selectedNodeId = null; // Currently selected node for decree display

let ws = null;
let scenarioTimeout = null;
let eventQueue = [];
let processingTimeout = null;
let isProcessing = false;

// Update speed display
speedSlider.addEventListener("change", (e) => {
  speedValue.textContent = parseFloat(e.target.value).toFixed(2) + "x";
});

// Cluster initialization - single point where clusterInfo is created
function initializeCluster(clusterInfoData) {
  clusterInfo = clusterInfoData;

  // Initialize decree tracking for all nodes
  nodeDecrees = {};
  for (let i = 0; i < clusterInfo.total_nodes; i++) {
    nodeDecrees[i] = { count: 0, decrees: [] };
  }

  // Adjust visualization for viewport
  adjustNodeRadiusForViewport();
  visualizer.render({ total_nodes: clusterInfo.total_nodes });

  console.log("Cluster initialized:", clusterInfo.total_nodes, "nodes");
}

// Helper functions
function canCommunicate(from, to) {
  // Check if both nodes are in same partition (both partitioned or both not partitioned)
  const fromPartitioned = partitionedNodes.has(from);
  const toPartitioned = partitionedNodes.has(to);
  // Can communicate if both have same partition status
  return fromPartitioned === toPartitioned;
}

function addEvent(text, color) {
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
  document.getElementById("nextBallotCount").textContent = eventCounts.Proposal;
  document.getElementById("lastVoteCount").textContent = eventCounts.Promise;
  document.getElementById("beginBallotCount").textContent = eventCounts.Accept;
  document.getElementById("votedCount").textContent = eventCounts.Accepted;
  document.getElementById("learnedCount").textContent = eventCounts.LearnedValue;
  document.getElementById("successCount").textContent = eventCounts.Success;

  // Only update proposal stats if cluster is initialized
  if (clusterInfo) {
    updateProposalStats();
  }
}

function updateProposalStats() {
  const statsContainer = document.getElementById("proposalStatsContainer");
  if (!statsContainer) return;

  let html = "<div class='proposal-stats'>";
  for (let nodeId = 0; nodeId < clusterInfo.total_nodes; nodeId++) {
    const count = nodeDecrees[nodeId]?.count || 0;
    const isSelected = selectedNodeId === nodeId;
    const selectClass = isSelected ? "selected" : "";
    html += `<div class='proposal-stat-item ${selectClass}' data-node-id='${nodeId}' onclick='selectNode(${nodeId})'><span class='node-id'>N${nodeId}:</span> ${count}</div>`;
  }
  html += "</div>";
  statsContainer.innerHTML = html;

  // Update decree display
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
     decreePanel.innerHTML =
       "<p class='decree-hint'>Click a node to view its learned decrees</p>";
     return;
   }

   const nodeInfo = nodeDecrees[selectedNodeId];
   if (!nodeInfo || nodeInfo.decrees.length === 0) {
     decreePanel.innerHTML = `<p class='decree-hint'>Node ${selectedNodeId} has not learned any decrees yet</p>`;
     return;
   }

   // Sort decrees by decree_num (ascending)
   const sortedDecrees = [...nodeInfo.decrees].sort((a, b) => a.decree_num - b.decree_num);
   
   let html = `<div class='decree-content'><div class='decree-node-label'>Node ${selectedNodeId} (${nodeInfo.decrees.length} decrees)</div><div class='decree-list'>`;
   
   for (const decree of sortedDecrees) {
     // Show decree number and indicate if it's a NOOP (e.g., decree that wasn't proposed by this node initially)
     html += `<div class='decree-item'><div class='decree-text'>[${decree.decree_num}] "${decree.decree}"</div></div>`;
   }
   
   html += `</div></div>`;
   decreePanel.innerHTML = html;
}

async function resetScenario() {
  isRunning = false;
  eventQueue = [];
  if (scenarioTimeout) {
    clearTimeout(scenarioTimeout);
  }

  // Stop the scenario on the server (which also deletes persisted state)
  try {
    await fetch("/api/stop-scenario", { method: "POST" });
  } catch (error) {
    console.error("Error stopping scenario:", error);
  }

  playBtn.disabled = false;
  pauseBtn.disabled = true;
  scenarioSelect.disabled = false;

  // Clear event counts and proposal state
   eventCounts = {
     Proposal: 0,
     Promise: 0,
     Accept: 0,
     Accepted: 0,
     LearnedValue: 0,
     InitialDecree: 0,
     Success: 0,
   };

  // Reset decree tracking (keep structure initialized by initializeCluster)
  if (clusterInfo) {
    for (let i = 0; i < clusterInfo.total_nodes; i++) {
      nodeDecrees[i] = { count: 0, decrees: [] };
    }
  }

  // Reset partition state for all nodes
  partitionedNodes.clear();
  if (clusterInfo) {
    for (let i = 0; i < clusterInfo.total_nodes; i++) {
      visualizer.setNodePartitioned(i, false);
    }
  }

  selectedNodeId = null;
  updateCounts();
  eventLog.innerHTML = "";
  statusTitle.textContent = "Ready";
  statusDescription.textContent = "Click Play to start the demonstration";
  statusTitle.style.color = "#60a5fa";
}

function setupWebSocket() {
   if (ws) {
     ws.close();
   }

   ws = new WebSocket(getWebSocketURL("/ws"));

  ws.onopen = () => {
    console.log("WebSocket connected");
  };

  ws.onmessage = (event) => {
    const message = JSON.parse(event.data);

    if (message.ClusterInitialized) {
      console.log("ClusterInitialized message received:", message);
      if (!clusterInfo) {
        initializeCluster(message.ClusterInitialized);
      } else {
        console.log("Ignoring duplicate ClusterInitialized message");
      }
    } else if (message.Event) {
      const eventType = Object.keys(message.Event)[0];

      // Handle partition events
      if (eventType === "PartitionCreated") {
        const event = message.Event.PartitionCreated;
        console.log("Network partitioned:", event);
        for (const nodeId of event.partition_b) {
          partitionedNodes.add(nodeId);
          visualizer.setNodePartitioned(nodeId, true);
        }
        statusDescription.textContent = `Partition: A=${JSON.stringify(
          event.partition_a
        )}, B=${JSON.stringify(event.partition_b)}`;
      } else if (eventType === "PartitionHealed") {
        console.log("Network healed");
        for (const nodeId of partitionedNodes) {
          visualizer.setNodePartitioned(nodeId, false);
        }
        partitionedNodes.clear();
        statusDescription.textContent = "Network healed - all nodes connected";
      } else {
        handlePaxosEvent(message.Event);
      }
    }
  };

  ws.onerror = (error) => {
    console.error("WebSocket error:", error);
    statusTitle.style.color = "#ef4444";
    statusTitle.textContent = "Connection Error";
  };

  ws.onclose = () => {
    console.log("WebSocket closed");
  };
}

function handlePaxosEvent(eventData) {
  if (!isRunning) {
    console.log("Ignoring event - scenario not running:", eventData);
    return;
  }

  const eventType = Object.keys(eventData)[0];
  const event = eventData[eventType];

  if (!event) return;

  const colorInfo = eventColorMap[eventType];
  if (!colorInfo) return;

  console.log(
    `Queueing ${eventType} event (queue length: ${eventQueue.length})`
  );
  // Queue the event for processing
  eventQueue.push({ eventType, event, colorInfo });

  // Debounce processing - wait 5ms for more events to arrive before batching
  if (processingTimeout) {
    clearTimeout(processingTimeout);
  }
  processingTimeout = setTimeout(() => {
    processEventQueue();
  }, 5);
}

async function processEventQueue() {
  if (isProcessing || eventQueue.length === 0) return;

  isProcessing = true;

  // Batch events by created_at timestamp (within 100 microseconds)
  const firstEvent = eventQueue[0];
  const batchTimestamp = firstEvent.event.created_at;
  const batch = [];
  const batchWindowMicros = 50; // 100 microseconds window

  while (
    eventQueue.length > 0 &&
    eventQueue[0].event.created_at - batchTimestamp < batchWindowMicros
  ) {
    batch.push(eventQueue.shift());
  }

  // Process all events in batch in parallel
  const promises = batch.map(({ eventType, event, colorInfo }) => {
    console.log(`Event: ${eventType}`, event);

    // Update counter
    eventCounts[eventType]++;

    // Add to log
    const logText = formatEventLog(eventType, event);
    addEvent(logText, colorInfo.color);

    // Visualize based on event type
    switch (eventType) {
      case "Proposal":
        return visualizeProposal(event, colorInfo.color).catch((e) =>
          console.error("Error visualizing proposal:", e)
        );
      case "Promise":
        return visualizePromise(event, colorInfo.color).catch((e) =>
          console.error("Error visualizing promise:", e)
        );
      case "Accept":
        return visualizeAccept(event, colorInfo.color).catch((e) =>
          console.error("Error visualizing accept:", e)
        );
      case "Accepted":
        return visualizeAccepted(event, colorInfo.color).catch((e) =>
          console.error("Error visualizing accepted:", e)
        );
      case "LearnedValue":
         // Track the actual learned decree value
         let decreeContent;
         if (event.value === "NOOP") {
           decreeContent = "NOOP";
         } else if (event.value?.EnactDecree?.law) {
           decreeContent = event.value.EnactDecree.law;
         } else {
           decreeContent = `Decree #${event.decree_num}`;
         }
         nodeDecrees[event.id].count = (nodeDecrees[event.id]?.count || 0) + 1;
         // Add to decrees array with decree number for sorting
         nodeDecrees[event.id].decrees.unshift({
           decree_num: event.decree_num,
           decree: decreeContent,
           timestamp: event.created_at,
         });
         return visualizeLearn(event, colorInfo.color).catch((e) =>
           console.error("Error visualizing learn:", e)
         );
      case "InitialDecree":
         // Track initial decrees from pre-populated ledger (no visualization needed)
         let initialDecreeContent;
         if (event.value === "NOOP") {
           initialDecreeContent = "NOOP";
         } else if (event.value?.EnactDecree?.law) {
           initialDecreeContent = event.value.EnactDecree.law;
         } else {
           initialDecreeContent = `Decree #${event.decree_num}`;
         }
         nodeDecrees[event.id].count = (nodeDecrees[event.id]?.count || 0) + 1;
         // Add to decrees array with decree number for sorting
         nodeDecrees[event.id].decrees.unshift({
           decree_num: event.decree_num,
           decree: initialDecreeContent,
           timestamp: event.created_at,
         });
         return Promise.resolve();
      case "Success":
        return visualizeSuccess(event, colorInfo.color).catch((e) => {
          console.error("Error visualizing success:", e);
        });
      default:
        return Promise.resolve();
    }
  });

  await Promise.all(promises);
  updateCounts();

  // Delay before next batch based on speed
  const speed = parseFloat(speedSlider.value);
  const delay = Math.max(100, 300 / speed);
  await new Promise((resolve) => setTimeout(resolve, delay));

  isProcessing = false;

  // Process next batch if there are more events
  if (eventQueue.length > 0) {
    processEventQueue();
  }
}

function formatDecree(event) {
  // Extract decree description from the value field
  if (
    event.value &&
    typeof event.value === "object" &&
    event.value.EnactDecree
  ) {
    return event.value.EnactDecree.law;
  }
  return `Decree #${event.decree_num}`;
}

function formatEventLog(eventType, event) {
  const colorInfo = eventColorMap[eventType];
  const name = colorInfo.name;

  switch (eventType) {
    case "Proposal":
      const decree = formatDecree(event);
      return `[${name}] Node ${event.id}: "${decree}"`;
    case "Promise":
      return `[${name}] Node ${event.id} → Node ${event.from}: Ballot ${event.ballot}`;
    case "Accept":
      return `[${name}] Node ${event.id}: Ballot ${event.ballot}, Decree #${event.decree_num}`;
    case "Accepted":
      return `[${name}] Node ${event.id} → Node ${event.from}: Voted on ballot ${event.ballot}`;
    case "LearnedValue":
       const decreeText = event.value?.EnactDecree?.law || `Decree #${event.decree_num}`;
       return `[${name}] Node ${event.id}: "${decreeText}"`;
     case "InitialDecree":
       const initialDecreeText = event.value === "NOOP" ? "NOOP" : (event.value?.EnactDecree?.law || `Decree #${event.decree_num}`);
       return `[${name}] Node ${event.id}: [${event.decree_num}] "${initialDecreeText}"`;
     case "Success":
      const proposerInfo =
        event.ballot_proposer !== undefined
          ? ` (proposed by node ${event.ballot_proposer})`
          : "";
      return `[${name}] Node ${event.id}: Decree #${event.decree_num} chosen${proposerInfo}`;
    default:
      return JSON.stringify(event);
  }
}

async function visualizeProposal(event, color) {
  visualizer.setNodeState(event.id, "propose");
  visualizer.activateNode(event.id, color);
  // When proposer proposes, it will send Prepare to all acceptors
  // We visualize this by drawing beams to all other nodes
  const speed = parseFloat(speedSlider.value);
  const duration = Math.max(200, (500 / speed) * 0.67);

  // Draw beams to all reachable nodes (skip partitioned targets and self)
  const beamPromises = [];
  for (let i = 0; i < clusterInfo.total_nodes; i++) {
    if (i !== event.id && canCommunicate(event.id, i)) {
      console.log(`Queue beam proposal: ${event.id} -> ${i}`);
      beamPromises.push(
        visualizer.drawBeam(event.id, i, color, duration, "solid")
      );
    }
  }
  if (beamPromises.length > 0) {
    console.log(`Waiting for ${beamPromises.length} beams to complete`);
    await Promise.all(beamPromises);
    console.log(`All beams for proposal ${event.id} complete`);
  }
}

async function visualizePromise(event, color) {
  visualizer.setNodeState(event.id, "promise");
  visualizer.activateNode(event.id, color);
  // Promise is sent from acceptor (event.id) back to proposer (event.from)
  if (event.from !== undefined && event.from !== event.id) {
    const speed = parseFloat(speedSlider.value);
    const duration = Math.max(200, (500 / speed) * 0.67);
    await visualizer.drawBeam(event.id, event.from, color, duration, "dashed");
  }
}

async function visualizeAccept(event, color) {
  visualizer.setNodeState(event.id, "accept");
  visualizer.activateNode(event.id, color);
  // Proposer sends Accept only to nodes in quorum that promised
  const speed = parseFloat(speedSlider.value);
  const duration = Math.max(200, (500 / speed) * 0.67);

  const beamPromises = [];
  if (event.quorum && Array.isArray(event.quorum)) {
    for (const nodeId of event.quorum) {
      if (nodeId !== event.id) {
        beamPromises.push(
          visualizer.drawBeam(event.id, nodeId, color, duration, "solid")
        );
      }
    }
  }
  await Promise.all(beamPromises);
}

async function visualizeAccepted(event, color) {
  visualizer.setNodeState(event.id, "voted");
  visualizer.activateNode(event.id, color);
  // Accepted is sent from acceptor (event.id) back to proposer/learner (event.from)
  if (event.from !== undefined && event.from !== event.id) {
    const speed = parseFloat(speedSlider.value);
    const duration = Math.max(200, (500 / speed) * 0.67);
    await visualizer.drawBeam(event.id, event.from, color, duration, "dotted");
  }
}

async function visualizeLearn(event, color) {
  visualizer.setNodeState(event.id, "learn");
  visualizer.activateNode(event.id, color);
  // Add a small delay for learn visualization
  await new Promise((resolve) => setTimeout(resolve, 100));
}

async function visualizeSuccess(event, color) {
  // Highlight the original proposer if different from the learner broadcasting
  if (
    event.ballot_proposer !== undefined &&
    event.ballot_proposer !== event.from
  ) {
    visualizer.activateNode(event.ballot_proposer, "#fbbf24"); // Highlight proposer in yellow
  }
  visualizer.activateNode(event.from, color);

  // Success: broadcast from the learner that reached quorum (event.from) to reachable nodes only
  if (event.from !== undefined && canCommunicate(event.from, event.id)) {
    const speed = parseFloat(speedSlider.value);
    const duration = Math.max(200, (500 / speed) * 0.67);
    await visualizer.drawBeam(event.from, event.id, color, duration, "dashed");
  }
}

async function playScenario() {
  if (isRunning) return;

  const scenarioName = scenarioSelect.value;

  isRunning = true;
  playBtn.disabled = true;
  pauseBtn.disabled = false;
  scenarioSelect.disabled = true;

  statusTitle.textContent = "Running";
  statusDescription.textContent = "Scenario in progress...";
  statusTitle.style.color = "#60a5fa";

  // Reset partition state before starting new scenario
  partitionedNodes.clear();
  if (clusterInfo) {
    for (let i = 0; i < clusterInfo.total_nodes; i++) {
      visualizer.setNodePartitioned(i, false);
    }
  }

  try {
    // Start the scenario on the backend
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
        isRunning = false;
        resolve();
      }, 30000); // 30 seconds
    });

    // Wait for event queue to drain
    statusTitle.textContent = "Processing";
    statusDescription.textContent = "Waiting for all events to visualize...";
    statusTitle.style.color = "#f59e0b";

    while (eventQueue.length > 0 || isProcessing) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  } catch (error) {
    console.error("Error starting scenario:", error);
    statusTitle.style.color = "#ef4444";
    statusTitle.textContent = "Error";
    statusDescription.textContent = error.message;
    isRunning = false;
  }

  playBtn.disabled = false;
  pauseBtn.disabled = true;
  scenarioSelect.disabled = false;
  statusTitle.textContent = "Complete";
  statusTitle.style.color = "#34d399";
  statusDescription.textContent = `Scenario finished - ${
    eventCounts.Proposal +
    eventCounts.Promise +
    eventCounts.Accept +
    eventCounts.Accepted +
    eventCounts.LearnedValue +
    eventCounts.InitialDecree +
    eventCounts.Success
  } total events visualized`;
}

function pauseScenario() {
  isRunning = false;
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
  setupWebSocket();
  resetScenario();
});
