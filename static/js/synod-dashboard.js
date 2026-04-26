const ROOM = new URLSearchParams(location.search).get("room") || "main";

// ── DOM refs ──────────────────────────────────────────────────────────────
const roomLabel    = document.querySelector("#roomLabel");
const statusLamp   = document.querySelector("#statusLamp");
const headerStatus = document.querySelector("#headerStatus");
const headerSlot   = document.querySelector("#headerSlot");
const headerNodes  = document.querySelector("#headerNodes");
const headerApplied = document.querySelector("#headerApplied");

const statSlot    = document.querySelector("#statSlot");
const statApplied = document.querySelector("#statApplied");
const statNodes   = document.querySelector("#statNodes");
const statRate    = document.querySelector("#statRate");
const streamFeed  = document.querySelector("#streamFeed");
const streamScores = document.querySelector("#streamScores");

const synodConfigId = document.querySelector("#synodConfigId");
const synodFields   = document.querySelector("#synodFields");
const synodNodeGrid = document.querySelector("#synodNodeGrid");

const latestCommit = document.querySelector("#latestCommit");
const latestEmoji  = document.querySelector("#latestEmoji");
const latestSlot   = document.querySelector("#latestSlot");
const ledgerBody   = document.querySelector("#ledgerBody");

roomLabel.textContent = ROOM;

// ── Tabs ──────────────────────────────────────────────────────────────────
document.querySelectorAll(".dash-tab").forEach(tab => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".dash-tab").forEach(t => t.classList.remove("is-active"));
    document.querySelectorAll(".dash-panel").forEach(p => p.classList.add("hidden"));
    tab.classList.add("is-active");
    document.querySelector(`#tab-${tab.dataset.tab}`).classList.remove("hidden");
  });
});

// ── Rate tracking ─────────────────────────────────────────────────────────
let recentTs = [];

function trackCommit() {
  const now = Date.now();
  recentTs = [...recentTs.filter(t => now - t < 2000), now];
}

setInterval(() => {
  const now = Date.now();
  recentTs = recentTs.filter(t => now - t < 2000);
  statRate.textContent = (recentTs.length / 2).toFixed(1);
}, 500);

// ── State ─────────────────────────────────────────────────────────────────
let lastKnownSlot = -1;
let ledgerEntries = [];
let feedInitialised = false;

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

// ── Render ────────────────────────────────────────────────────────────────
function applyStatus(status) {
  const nodes  = status.active_nodes;
  const config = status.active_configuration;
  const state  = status.state;

  headerNodes.textContent = nodes;
  statNodes.textContent   = nodes;

  if (state) {
    headerSlot.textContent    = state.cluster_slot;
    headerApplied.textContent = state.last_applied;
    statSlot.textContent      = state.cluster_slot;
    statApplied.textContent   = state.last_applied;

    if (state.heat?.length) renderScores(state.heat);

    if (state.recent?.length) {
      if (!feedInitialised) {
        // Seed feed without animation
        streamFeed.replaceChildren();
        const recent = [...state.recent].reverse();
        recent.forEach(e => addChip(e, false));
        ledgerEntries = recent;
        renderLedger();
        lastKnownSlot = state.recent[state.recent.length - 1].slot;
        feedInitialised = true;
      } else {
        const newEntries = state.recent
          .filter(e => e.slot > lastKnownSlot)
          .sort((a, b) => a.slot - b.slot);
        for (const e of newEntries) {
          trackCommit();
          addChip(e, true);
          lastKnownSlot = e.slot;
          ledgerEntries.unshift(e);
          if (ledgerEntries.length > 80) ledgerEntries.pop();
        }
        if (newEntries.length) renderLedger();
      }
    }
  }

  if (config) {
    renderSynodConfig(config, nodes, status.clients || []);
  }
}

function addChip(entry, animate) {
  if (animate) {
    // Demote previous showcase entry to the feed grid
    if (latestCommit.classList.contains("visible") && latestEmoji.textContent) {
      const prevEmoji = latestEmoji.textContent;
      const prevSlot  = latestSlot.textContent;
      const chip = document.createElement("div");
      chip.className = "commit-chip";
      chip.title = `slot ${prevSlot}`;
      chip.innerHTML = `<span class="chip-emoji">${prevEmoji}</span>
<span class="chip-slot">${prevSlot}</span>`;
      streamFeed.prepend(chip);
    }

    // Promote new entry to showcase zone
    latestEmoji.textContent = entry.emoji;
    latestSlot.textContent  = `#${String(entry.slot).padStart(3, "0")}`;
    latestCommit.classList.remove("visible");
    void latestCommit.offsetWidth; // reflow to restart animation
    latestCommit.classList.add("visible");

    statSlot.classList.add("flash");
    setTimeout(() => statSlot.classList.remove("flash"), 400);
  } else {
    const chip = document.createElement("div");
    chip.className = "commit-chip";
    chip.title = `slot ${entry.slot}`;
    chip.innerHTML = `<span class="chip-emoji">${entry.emoji}</span>
<span class="chip-slot">#${String(entry.slot).padStart(3, "0")}</span>`;
    streamFeed.appendChild(chip);
  }

  while (streamFeed.children.length > 60) streamFeed.removeChild(streamFeed.lastChild);
}

function renderScores(heat) {
  const sorted   = [...heat].sort((a, b) => b.count - a.count);
  const maxCount = sorted[0]?.count || 1;
  const leadEmoji = sorted[0]?.count > 0 ? sorted[0].emoji : null;

  streamScores.replaceChildren();
  sorted.forEach(({ emoji, count }) => {
    const pct      = (count / maxCount) * 100;
    const isLeader = emoji === leadEmoji && count > 0;
    const row = document.createElement("div");
    row.className = "score-row";
    row.innerHTML = `
      <span class="score-emoji">${emoji}</span>
      <div class="score-track"><div class="score-fill${isLeader ? " leader" : ""}" style="width:${pct}%"></div></div>
      <span class="score-count${isLeader ? " leader" : ""}">${count}</span>`;
    streamScores.appendChild(row);
  });
}

function renderSynodConfig(config, activeNodes, clients = []) {
  synodConfigId.textContent = config.configuration_id;

  synodFields.innerHTML = `
    <div>
      <div class="synod-field-key">leader</div>
      <div class="synod-field-val">${config.leader}</div>
    </div>
    <div>
      <div class="synod-field-key">acceptors</div>
      <div class="synod-field-val plain">${config.acceptors.length}</div>
    </div>
    <div>
      <div class="synod-field-key">replicas</div>
      <div class="synod-field-val plain">${config.replicas.length}</div>
    </div>
    <div>
      <div class="synod-field-key">start index</div>
      <div class="synod-field-val plain">${config.start_index}</div>
    </div>
    <div>
      <div class="synod-field-key">active nodes</div>
      <div class="synod-field-val plain">${activeNodes}</div>
    </div>`;

  synodNodeGrid.replaceChildren();

  const clientByNode = new Map(
    clients
      .filter(c => c.client_name || c.client_id)
      .map(c => [c.node_id.toString(), c.client_name || c.client_id.slice(0, 8)])
  );

  const addNode = (id, role) => {
    const clientName = clientByNode.get(id.toString());
    const el = document.createElement("div");
    el.className = `synod-node${role === "leader" ? " is-leader" : role === "replica" ? " is-replica" : ""}`;
    el.innerHTML = `
      <div class="synod-node-role">${role}</div>
      <div class="synod-node-id">${id.slice(0, 8)}<span class="dim">…${id.slice(-4)}</span></div>
      ${clientName ? `<div class="synod-node-client">${escapeHtml(clientName)}</div>` : ""}`;
    synodNodeGrid.appendChild(el);
  };

  addNode(config.leader, "leader");
  config.replicas.forEach(id => { if (id !== config.leader) addNode(id, "replica"); });
  config.acceptors.forEach(id => {
    if (id !== config.leader && !config.replicas.includes(id)) addNode(id, "acceptor");
  });
}


function renderLedger() {
  ledgerBody.replaceChildren();
  if (!ledgerEntries.length) {
    ledgerBody.innerHTML = `<tr><td colspan="5" class="empty-state">no entries yet</td></tr>`;
    return;
  }
  ledgerEntries.slice(0, 60).forEach(e => {
    const tr = document.createElement("tr");
    const clientName = e.client_id ? escapeHtml(e.client_id) : "—";
    tr.innerHTML = `
      <td class="col-slot">#${String(e.slot).padStart(3, "0")}</td>
      <td class="col-emoji">${e.emoji}</td>
      <td class="col-client">${clientName}</td>
      <td class="col-req">${e.request_id ?? "—"}</td>
      <td class="col-count">${e.count}</td>`;
    ledgerBody.appendChild(tr);
  });
}

// ── Connection ────────────────────────────────────────────────────────────
function setConnectionState(state) {
  statusLamp.className   = `dash-lamp ${state}`;
  headerStatus.textContent = state === "active" ? "active" : state === "pending" ? "reconnecting" : "error";
}

function socketUrl() {
  const proto  = location.protocol === "https:" ? "wss:" : "ws:";
  const params = new URLSearchParams({ room: ROOM });
  return `${proto}//${location.host}/synod/ws?${params}`;
}

function connect() {
  const ws = new WebSocket(socketUrl());

  ws.addEventListener("open", () => {
    setConnectionState("active");
    setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "heartbeat" }));
      }
    }, 15000);
  });

  ws.addEventListener("message", event => {
    const msg = JSON.parse(event.data);
    if (msg.type === "joined") {
      applyStatus({ active_nodes: msg.session.active_nodes, active_configuration: null, state: msg.session.state });
    } else if (msg.type === "room_state") {
      applyStatus(msg.status);
    } else if (msg.type === "heartbeat") {
      headerNodes.textContent = msg.heartbeat.active_nodes;
      statNodes.textContent   = msg.heartbeat.active_nodes;
    }
  });

  ws.addEventListener("close", () => {
    setConnectionState("pending");
    setTimeout(connect, 3000);
  });

  ws.addEventListener("error", () => setConnectionState("error"));
}

async function poll() {
  try {
    const res = await fetch(`/synod/api/status?room=${encodeURIComponent(ROOM)}`);
    if (res.ok) applyStatus(await res.json());
  } catch (_) {}
}

connect();
poll();
setInterval(poll, 2000);
