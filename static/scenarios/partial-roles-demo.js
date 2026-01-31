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

const eventLog = document.getElementById("eventLog");
const speedSlider = document.getElementById("speed");
const speedValue = document.getElementById("speedValue");
const statusTitle = document.getElementById("statusTitle");
const statusDescription = document.getElementById("statusDescription");
const playBtn = document.getElementById("playBtn");
const pauseBtn = document.getElementById("pauseBtn");
const resetBtn = document.getElementById("resetBtn");
const topologyDetails = document.getElementById("topologyDetails");
const circleLayoutBtn = document.getElementById("circleLayoutBtn");
const gridLayoutBtn = document.getElementById("gridLayoutBtn");

let ws = null;
let scenarioTimeout = null;

// Event queue with custom handler
const eventQueue = new EventQueue(handleEvent, 50);

// Update speed display and state
speedSlider.addEventListener("change", (e) => {
    const speed = parseFloat(e.target.value);
    speedValue.textContent = speed.toFixed(2) + "x";
    state.setSpeed(speed);
});

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
    
    // Visualize (custom logic for partial-roles-demo)
    const snapshot = state.snapshot();
    const speed = snapshot.simulation.speed;
    const delayMs = Math.max(300, 400 / speed);
    
    switch (eventType) {
        case "Proposal":
            visualizer.activateNode(eventData.id, viz.color);
            break;
        case "Promise":
            if (eventData.from !== undefined && eventData.from !== eventData.id) {
                await visualizer.drawBeam(eventData.id, eventData.from, viz.color, 350, 'dashed');
            }
            break;
        case "Accept":
            const acceptBeams = (eventData.quorum || []).map(to =>
                visualizer.drawBeam(eventData.id, to, viz.color, 350, 'solid')
            );
            await Promise.all(acceptBeams);
            break;
        case "Accepted":
            if (eventData.from !== undefined && eventData.from !== eventData.id) {
                await visualizer.drawBeam(eventData.id, eventData.from, viz.color, 350, 'dotted');
            }
            break;
        case "Learn":
             visualizer.setNodeState(eventData.id, "learn");
             visualizer.activateNode(eventData.id, viz.color);
             break;
        
        case "LearnedValue":
             visualizer.setNodeState(eventData.id, "learned");
             visualizer.activateNode(eventData.id, viz.color);
             break;
         
        case "Success":
             visualizer.setNodeState(eventData.id, "success");
             visualizer.activateNode(eventData.id, viz.color);
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
}

async function playScenario() {
     state.setRunning(true);
     playBtn.disabled = true;
     pauseBtn.disabled = false;

     try {
         const response = await fetch("/api/start-scenario", {
             method: "POST",
             headers: { "Content-Type": "application/json" },
             body: JSON.stringify({
                 node_count: 9,
                 duration_secs: 60,
                 scenario_type: "partial_roles",
             }),
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

playBtn.onclick = playScenario;
pauseBtn.onclick = pauseScenario;
resetBtn.onclick = resetScenario;
circleLayoutBtn.onclick = () => setLayout('circle');
gridLayoutBtn.onclick = () => setLayout('role-grouped');

document.addEventListener("DOMContentLoaded", () => {
    // Set initial speed from slider
    const initialSpeed = parseFloat(speedSlider.value);
    state.setSpeed(initialSpeed);
    setupWebSocket();
});
