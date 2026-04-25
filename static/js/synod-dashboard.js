const tabs = document.querySelectorAll(".tab");
const panels = {
  live: document.querySelector("#livePanel"),
  controller: document.querySelector("#controllerPanel"),
  events: document.querySelector("#eventsPanel"),
};

const activeNodes = document.querySelector("#activeNodes");
const configId = document.querySelector("#configId");
const leaderId = document.querySelector("#leaderId");
const lastApplied = document.querySelector("#lastApplied");
const clusterSlot = document.querySelector("#clusterSlot");
const heatRow = document.querySelector("#heatRow");
const requestList = document.querySelector("#requestList");
const ledgerList = document.querySelector("#ledgerList");

tabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    const target = tab.dataset.tab;
    tabs.forEach((item) => item.classList.toggle("is-active", item === tab));
    Object.entries(panels).forEach(([name, panel]) => {
      panel.classList.toggle("hidden", name !== target);
    });
  });
});

async function refreshStatus() {
  const response = await fetch("/synod/api/status");
  if (!response.ok) return;

  const status = await response.json();
  activeNodes.textContent = status.active_nodes;
  renderRoomState(status.state);
  if (!status.active_configuration) {
    configId.textContent = "none";
    leaderId.textContent = "leader pending";
    return;
  }

  configId.textContent = status.active_configuration.configuration_id;
  leaderId.textContent = `leader ${status.active_configuration.leader.slice(0, 8)}`;
}

function renderRoomState(state) {
  if (!state) return;
  lastApplied.textContent = state.last_applied;
  clusterSlot.textContent = `cluster slot ${state.cluster_slot}`;
  renderHeat(state.heat || []);
  renderRequests(state.requests || []);
  renderLedger(state.recent || []);
}

function renderHeat(heat) {
  heatRow.replaceChildren();
  heat.forEach(({ emoji, count }) => {
    const item = document.createElement("span");
    item.className = "dashboard-heat-item";
    item.textContent = `${emoji} ${count}`;
    heatRow.appendChild(item);
  });
}

function renderRequests(requests) {
  requestList.replaceChildren();
  requests.slice(0, 24).forEach((request) => {
    const row = document.createElement("div");
    row.className = `dashboard-row stage-${request.stage}`;
    row.innerHTML = `
      <strong>${request.emoji} ${request.stage}</strong>
      <span>client ${request.client_id.slice(0, 8)} · req ${request.request_id}</span>
      <span>proposed ${request.proposed_slot ?? "n/a"} · applied ${request.applied_slot ?? "pending"}</span>
    `;
    requestList.appendChild(row);
  });
}

function renderLedger(recent) {
  ledgerList.replaceChildren();
  [...recent].reverse().slice(0, 24).forEach((entry) => {
    const row = document.createElement("div");
    row.className = "dashboard-row";
    row.innerHTML = `
      <strong>slot ${entry.slot} · ${entry.emoji}</strong>
      <span>count ${entry.count} · req ${entry.request_id ?? "n/a"}</span>
      <span>${entry.client_id ? `client ${entry.client_id.slice(0, 8)}` : "system"}</span>
    `;
    ledgerList.appendChild(row);
  });
}

refreshStatus();
setInterval(refreshStatus, 1500);
