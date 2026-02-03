/**
 * Partial Roles Demo
 * Visualizing heterogeneous node roles and role-aware routing
 */

import { state } from '/demo-state.js';
import { EVENT_VISUALIZERS, getEventVisualizer } from '/event-visualizers.js';
import { EventQueue } from '/event-queue.js';

const visualizer = new PaxosVisualizer("partialRolesSvg", {
    nodeRadius: 180,
    nodeCircleRadius: 24,
    layoutMode: 'circle'
});

let eventLog, speedSlider, speedValue, statusTitle, statusDescription, playBtn, pauseBtn, resetBtn, topologyDetails, circleLayoutBtn, gridLayoutBtn, learningStrategySelect, scenarioSelect, scenarioDetails;
let ws = null;
let scenarioTimeout = null;
let selectedNodeId = null;
let eventQueue;

const scenarioDescriptions = {
    "simple_happy_path": "A 5-node cluster with one designated leader. Watch the leader propose commands, all nodes reach consensus, and learners execute them in order.",
    "asymmetric_proposers": "Two dedicated proposer/learner nodes compete against 4 acceptor-only nodes. Proposers simultaneously propose values every iteration, demonstrating consensus with role separation.",
    "network_partition": "A network partition occurs and the cluster recovers. Watch how Paxos maintains safety even with partial connectivity, then resumes consensus."
};

function initializeElements() {
    eventLog = document.getElementById("eventLog");
    speedSlider = document.getElementById("speed");
    speedValue = document.getElementById("speedValue");
    statusTitle = document.getElementById("statusTitle");
    statusDescription = document.getElementById("statusDescription");
    playBtn = document.getElementById("playBtn");
    pauseBtn = document.getElementById("pauseBtn");
    resetBtn = document.getElementById("resetBtn");
    topologyDetails = document.getElementById("topologyDetails");
    circleLayoutBtn = document.getElementById("circleLayoutBtn");
    gridLayoutBtn = document.getElementById("gridLayoutBtn");
    learningStrategySelect = document.getElementById("learningStrategySelect");
    scenarioSelect = document.getElementById("scenarioSelect");
    scenarioDetails = document.getElementById("scenarioDetails");
    
    eventQueue = new EventQueue(handleEvent, 50);
    
    // Update speed display and state
    speedSlider.addEventListener("change", (e) => {
        const speed = parseFloat(e.target.value);
        speedValue.textContent = speed.toFixed(2) + "x";
        state.setSpeed(speed);
    });
    
    // Update scenario description
    scenarioSelect.addEventListener("change", (e) => {
        const scenarioValue = e.target.value;
        const description = scenarioDescriptions[scenarioValue] || "";
        scenarioDetails.textContent = description;
    });
    
    // Set initial scenario description
    const initialScenario = scenarioSelect.value;
    scenarioDetails.textContent = scenarioDescriptions[initialScenario] || "";
}

function setLayout(mode) {
    visualizer.setLayoutMode(mode);

    // Update button states
    document.getElementById('circleLayoutBtn').classList.toggle('active', mode === 'circle');
    document.getElementById('gridLayoutBtn').classList.toggle('active', mode === 'role-grouped');
}

function updateTopologyPanel() {
     const snapshot = state.snapshot();
     if (!snapshot.cluster) return;

     let stats = { P: 0, A: 0, L: 0 };
     let nodesByRole = { Proposers: [], Acceptors: [], Learners: [] };

     for (let i = 0; i < snapshot.cluster.total_nodes; i++) {
         const node = snapshot.nodes.get(i);
         if (!node || !node.role) continue;

         const roles = node.role.roles || [];

         if (roles.includes('Proposer')) { stats.P++; nodesByRole.Proposers.push(i); }
         if (roles.includes('Acceptor')) { stats.A++; nodesByRole.Acceptors.push(i); }
         if (roles.includes('Learner')) { stats.L++; nodesByRole.Learners.push(i); }
     }

    topologyDetails.innerHTML = `
        <div class="topology-stat-group">
            <p><strong>Total Nodes:</strong> ${snapshot.cluster.total_nodes}</p>
            <p><span class="role-tag role-p">P</span> Proposers: ${stats.P} (${nodesByRole.Proposers.join(', ')})</p>
            <p><span class="role-tag role-a">A</span> Acceptors: ${stats.A} (${nodesByRole.Acceptors.join(', ')})</p>
            <p><span class="role-tag role-l">L</span> Learners: ${stats.L} (${nodesByRole.Learners.join(', ')})</p>
            <p><strong>Quorum:</strong> ${snapshot.cluster.quorum_size} Acceptors</p>
            <p><strong>Strategy:</strong> Direct</p>
        </div>
    `;
}

function setupWebSocket() {
    if (ws) ws.close();
    ws = new WebSocket(getWebSocketURL("/ws"));

    ws.onmessage = (event) => {
        const message = JSON.parse(event.data);

        if (message.ClusterInitialized) {
            state.initialize(message.ClusterInitialized);
            visualizer.render(message.ClusterInitialized);
            updateTopologyPanel();
        } else if (message.Event) {
            const eventType = Object.keys(message.Event)[0];
            const eventData = message.Event[eventType];

            if (eventType === "NodeCapabilities") {
                state.setNodeCapabilities(eventData.id, eventData.roles, eventData.learning_strategy);
                visualizer.setNodeCapabilities(eventData.id, eventData.roles, eventData.learning_strategy);
                updateTopologyPanel();
                // Re-render if we have all nodes
                const snapshot = state.snapshot();
                if (snapshot.cluster && Object.keys(visualizer.nodeCapabilities).length === snapshot.cluster.total_nodes) {
                    visualizer.render(snapshot.cluster);
                }
            } else {
                handlePaxosEvent(message.Event);
            }
        }
    };
}

function handlePaxosEvent(eventData) {
    const snapshot = state.snapshot();
    if (!snapshot.simulation.running) return;
    
    const eventType = Object.keys(eventData)[0];
    const eventPayload = eventData[eventType];
    
    if (!eventPayload) return;
    
    eventQueue.push({
        eventType,
        eventData: eventPayload,
        created_at: eventPayload.created_at || performance.now()
    });
}

/**
 * Check if two nodes can communicate (no network partition)
 */
function canCommunicate(fromId, toId) {
    // TODO: Implement network partition checking if needed
    return true;
}

/**
 * Handle a single event - called by EventQueue
 */
async function handleEvent(queuedEvent) {
    const { eventType, eventData } = queuedEvent;
    const viz = getEventVisualizer(eventType);
    
    if (!viz) return;
    
    // Log event with formatted text from visualizer
    addLog(viz.format(eventData), viz.color);
    
    // Track count
    state.incrementEventCount(eventType);
    updateCounts();
    
    // Visualize using the visualizer's visualize method
    const snapshot = state.snapshot();
    const speed = snapshot.simulation.speed;
    const delayMs = Math.max(300, 400 / speed);
    
    // Call the visualizer's visualize method if available
    if (viz.visualize) {
        await viz.visualize(eventData, visualizer, state, canCommunicate);
    }
    
    // Additional custom logic for specific events
    switch (eventType) {
        case "LearnedValue":
            // Track decrees learned by each node
            if (eventData.decree_num !== undefined) {
                state.addDecree(eventData.id, {
                    decree_num: eventData.decree_num,
                    decree: formatDecree(eventData),
                    timestamp: Date.now()
                });
            }
            break;
    }
    
    await new Promise(r => setTimeout(r, delayMs));
}

function addLog(text, color) {
    const item = document.createElement("div");
    item.style.borderLeft = `3px solid ${color}`;
    item.style.paddingLeft = "8px";
    item.style.marginBottom = "4px";
    item.textContent = text;
    eventLog.insertBefore(item, eventLog.firstChild);
}

function updateCounts() {
     const snapshot = state.snapshot();
     const countMap = {
         'Proposal': 'nextBallotCount',
         'Promise': 'lastVoteCount',
         'Accept': 'beginBallotCount',
         'Accepted': 'votedCount',
         'LearnedValue': 'learnedCount'
     };
     for (const [eventType, count] of Object.entries(snapshot.eventCounts)) {
         const elementId = countMap[eventType];
         if (elementId) {
             const el = document.getElementById(elementId);
             if (el) el.textContent = count;
         }
     }
     updateProposalStats();
}

function formatDecree(event) {
     if (event.value === 'NOOP') return 'NOOP';
     if (event.value && typeof event.value === 'object' && event.value.EnactDecree) {
         return event.value.EnactDecree.law;
     }
     return `Decree #${event.decree_num}`;
}

function updateProposalStats() {
     const snapshot = state.snapshot();
     if (!snapshot.cluster) return;

     const statsContainer = document.getElementById("proposalStatsContainer");
     if (!statsContainer) return;

     let learnerItems = [];
     for (let nodeId = 0; nodeId < snapshot.cluster.total_nodes; nodeId++) {
         const node = snapshot.nodes.get(nodeId);
         
         // Only show nodes with the Learner role
         if (!node?.role?.roles || !node.role.roles.includes('Learner')) {
             continue;
         }
         
         const decreeCount = node?.decrees.length || 0;
         const isSelected = selectedNodeId === nodeId;
         const selectClass = isSelected ? "selected" : "";
         learnerItems.push(`<div class='proposal-stat-item ${selectClass}' data-node-id='${nodeId}' onclick='selectNode(${nodeId})'><span class='node-id'>N${nodeId}:</span> ${decreeCount}</div>`);
     }
     
     if (learnerItems.length === 0) {
         statsContainer.style.display = 'none';
     } else {
         statsContainer.style.display = 'block';
         statsContainer.innerHTML = `<div class='proposal-stats'>${learnerItems.join('')}</div>`;
     }

     updateDecreeDisplay();
}

function selectNode(nodeId) {
     selectedNodeId = selectedNodeId === nodeId ? null : nodeId;
     updateProposalStats();
}

// Expose to global scope for onclick handlers
window.selectNode = selectNode;

function updateDecreeDisplay() {
     const decreePanel = document.getElementById("decreePanel");
     if (!decreePanel) return;

     if (selectedNodeId === null) {
         decreePanel.innerHTML = "<p class='decree-hint'>Click a learner node to view its decrees</p>";
         return;
     }

     const snapshot = state.snapshot();
     const node = snapshot.nodes.get(selectedNodeId);
     
     // Check if node is a learner
     if (!node?.role?.roles || !node.role.roles.includes('Learner')) {
         decreePanel.innerHTML = `<p class='decree-hint'>Node ${selectedNodeId} is not a learner</p>`;
         return;
     }
     
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

async function playScenario() {
     state.setRunning(true);
     playBtn.disabled = true;
     pauseBtn.disabled = false;

     try {
          const scenarioValue = scenarioSelect.value;
          const strategy = learningStrategySelect.value;
          
          // Set node count based on scenario type
          let nodeCount = 9; // default for partial_roles and asymmetric_proposers
          if (scenarioValue === "simple_happy_path") {
              nodeCount = 5; // simple_happy_path uses 5 nodes
          }
          
          const payload = {
              node_count: nodeCount,
              duration_secs: 60,
              scenario_type: scenarioValue,
              learning_strategy: strategy,
          };
         
         const response = await fetch("/api/start-scenario", {
             method: "POST",
             headers: { "Content-Type": "application/json" },
             body: JSON.stringify(payload),
         });

         if (!response.ok) {
             throw new Error("Failed to start scenario");
         }

         statusTitle.textContent = "Running";
         statusTitle.style.color = "#60a5fa";

         // Wait for scenario to complete (60 seconds)
         await new Promise((resolve) => {
             scenarioTimeout = setTimeout(() => {
                 state.setRunning(false);
                 resolve();
             }, 60000);
         });

         // Wait for event queue to drain
         statusTitle.textContent = "Processing";
         statusTitle.style.color = "#f59e0b";

         while (eventQueue.length() > 0 || eventQueue.isProcessing()) {
             await new Promise((resolve) => setTimeout(resolve, 100));
         }

         // Scenario complete
         const snapshot = state.snapshot();
         const totalEvents = Object.values(snapshot.eventCounts).reduce((a, b) => a + b, 0);

         statusTitle.textContent = "Complete";
         statusTitle.style.color = "#34d399";
         statusDescription.textContent = `Scenario finished - ${totalEvents} total events visualized`;
     } catch (e) {
         console.error(e);
         state.setRunning(false);
         statusTitle.style.color = "#ef4444";
         statusTitle.textContent = "Error";
         statusDescription.textContent = e.message;
     }

     playBtn.disabled = false;
     pauseBtn.disabled = true;
}

async function resetScenario() {
     state.resetSimulation();
     eventQueue.clear();
     if (scenarioTimeout) {
         clearTimeout(scenarioTimeout);
     }
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
     statusTitle.textContent = "Ready";
     statusDescription.textContent = "Experience role-aware routing in a heterogeneous cluster.";
     statusTitle.style.color = "#60a5fa";
}

function pauseScenario() {
    state.setRunning(false);
    playBtn.disabled = false;
    pauseBtn.disabled = true;
    statusTitle.textContent = "Paused";
    statusTitle.style.color = "#f59e0b";
}

document.addEventListener("DOMContentLoaded", () => {
    initializeElements();
    
    // Set button click handlers
    playBtn.onclick = playScenario;
    pauseBtn.onclick = pauseScenario;
    resetBtn.onclick = resetScenario;
    circleLayoutBtn.onclick = () => setLayout('circle');
    gridLayoutBtn.onclick = () => setLayout('role-grouped');
    
    // Set initial speed from slider
    const initialSpeed = parseFloat(speedSlider.value);
    state.setSpeed(initialSpeed);
    setupWebSocket();
});
