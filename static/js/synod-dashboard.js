const ROOM = new URLSearchParams(location.search).get("room") || "main";
const VISUALIZER_ENABLED = false;
const REQUEST_TERMINAL_TTL_MS = 6000;

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
const activityPulse = document.querySelector("#activityPulse");
const activityPipeline = document.querySelector("#activityPipeline");
const activityRequests = document.querySelector("#activityRequests");
const activityReorders = document.querySelector("#activityReorders");
const activityConfigLog = document.querySelector("#activityConfigLog");
const pipelineWindow = document.querySelector("#pipelineWindow");

roomLabel.textContent = ROOM;

// ── Tabs ──────────────────────────────────────────────────────────────────
document.querySelectorAll(".dash-tab").forEach(tab => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".dash-tab").forEach(t => t.classList.remove("is-active"));
    document.querySelectorAll(".dash-panel").forEach(p => p.classList.add("hidden"));
    tab.classList.add("is-active");
    document.querySelector(`#tab-${tab.dataset.tab}`).classList.remove("hidden");
    if (VISUALIZER_ENABLED && tab.dataset.tab === "vis") {
      vizConfigId = null; // force re-render now that SVG has real dimensions
      const lastConfig = window.__lastConfig;
      if (lastConfig) initViz(lastConfig);
    }
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
  renderActivity();
}, 500);

// ── State ─────────────────────────────────────────────────────────────────
let lastKnownSlot = -1;
let ledgerEntries = [];
let feedInitialised = false;
let latestStatus = null;
let previousConfig = null;
let requestStates = new Map();
let configEvents = [];

// ── Visualizer ────────────────────────────────────────────────────────────
let viz = null;
let vizConfigId = null;
let vizNodeIndex = new Map(); // uuid → numeric index
let vizLeaderIdx = null;
let vizReplicaIdxs = [];
let vizAcceptorIdxs = [];

const COMET_COLORS = [
  '#f87171','#fb923c','#fbbf24','#a3e635',
  '#34d399','#38bdf8','#818cf8','#e879f9','#f472b6'
];

function emojiColor(emoji) {
  let h = 0;
  for (const cp of emoji) h = (Math.imul(h, 31) + cp.codePointAt(0)) | 0;
  return COMET_COLORS[Math.abs(h) % COMET_COLORS.length];
}

function initViz(config) {
  if (!VISUALIZER_ENABLED) return;
  const svgEl = document.getElementById('synodVis');
  if (!svgEl) return;

  // Don't render while the panel is hidden — getBoundingClientRect returns zero
  // and all nodes would pile up at (0,0). Tab click will re-trigger with real dims.
  const rect = svgEl.getBoundingClientRect();
  if (rect.width < 10 || rect.height < 10) return;

  if (vizConfigId === config.configuration_id) return;

  const isReconfig = viz !== null;
  vizConfigId = config.configuration_id;

  // Build stable UUID→index map (sort for consistency)
  const allUuids = [...new Set([config.leader, ...config.replicas, ...config.acceptors])].sort();
  vizNodeIndex = new Map(allUuids.map((uuid, i) => [uuid, i]));

  vizLeaderIdx    = vizNodeIndex.get(config.leader) ?? null;
  vizReplicaIdxs  = config.replicas.map(uuid => vizNodeIndex.get(uuid)).filter(i => i !== undefined);
  vizAcceptorIdxs = config.acceptors.map(uuid => vizNodeIndex.get(uuid)).filter(i => i !== undefined);

  const nodeRadius = Math.min(rect.width, rect.height) * 0.4;

  // Pin SVG coordinate system to its CSS pixel size so user-units match pixels
  svgEl.setAttribute('width',  rect.width);
  svgEl.setAttribute('height', rect.height);

  viz = new PaxosVisualizer('synodVis', {
    nodeRadius,
    nodeCircleRadius: 7,
    nodeStateOffsetY: 999,
    topologyGradientColor: '#1e3a5f',
    nodeLabelFormatter: () => '',
  });

  allUuids.forEach((uuid, i) => {
    const roles = [];
    if (uuid === config.leader)              roles.push('Leader');
    if (config.replicas.includes(uuid))      roles.push('Replica');
    if (config.acceptors.includes(uuid))     roles.push('Acceptor');
    viz.setNodeCapabilities(i, roles.length ? roles : ['Acceptor'], null);
  });

  viz.render({ total_nodes: allUuids.length });

  if (vizLeaderIdx !== null) viz.setLeader(vizLeaderIdx);

  if (isReconfig) flashReconfig(svgEl, allUuids.length);
}

function flashReconfig(svgEl, nodeCount) {
  // Flash all nodes amber then fade a RECONFIG label
  for (let i = 0; i < nodeCount; i++) {
    viz.flashNode(i, '#ffb347', { motion: 'proposal', resetDelay: 900 });
  }

  const ns = 'http://www.w3.org/2000/svg';
  const label = document.createElementNS(ns, 'text');
  label.setAttribute('x', '50%');
  label.setAttribute('y', '50%');
  label.setAttribute('text-anchor', 'middle');
  label.setAttribute('dominant-baseline', 'middle');
  label.setAttribute('fill', '#ffb347');
  label.setAttribute('font-size', '20');
  label.setAttribute('font-family', 'monospace');
  label.setAttribute('letter-spacing', '4');
  label.setAttribute('opacity', '1');
  label.textContent = 'RECONFIGURING';
  svgEl.appendChild(label);

  let op = 1;
  const fade = setInterval(() => {
    op -= 0.04;
    label.setAttribute('opacity', String(Math.max(0, op)));
    if (op <= 0) { clearInterval(fade); label.remove(); }
  }, 40);
}

function drawEmojiComet(fromIdx, toIdx, emoji, color, options = {}) {
  const svgEl = document.getElementById('synodVis');
  if (!viz || !svgEl) return;

  const from = viz.nodeElements[fromIdx];
  const to   = viz.nodeElements[toIdx];
  if (!from || !to) return;

  const opacity    = options.opacity    ?? 1.0;
  const fontSize   = options.fontSize   ?? 16;
  const glowRadius = options.glowRadius ?? 5;

  const dx = to.x - from.x;
  const dy = to.y - from.y;
  const dist = Math.sqrt(dx * dx + dy * dy);
  let cx = from.x + dx / 2;
  let cy = from.y + dy / 2;
  if (dist > 0.0001) {
    const curveOffset = 36 + ((fromIdx * 7 + toIdx * 11) % 24);
    cx += (-dy / dist) * curveOffset;
    cy += ( dx / dist) * curveOffset;
  }

  const ns = 'http://www.w3.org/2000/svg';
  const text = document.createElementNS(ns, 'text');
  text.setAttribute('text-anchor', 'middle');
  text.setAttribute('dominant-baseline', 'middle');
  text.setAttribute('font-size', String(fontSize));
  text.setAttribute('opacity', String(opacity));
  text.style.filter = `drop-shadow(0 0 ${glowRadius}px ${color})`;
  text.textContent = emoji;

  let beamsLayer = svgEl.querySelector('#paxos-beams');
  (beamsLayer || svgEl).appendChild(text);

  const duration = 480;
  const tickMs = 16;
  const totalSteps = Math.ceil(duration / tickMs);
  let step = 0;

  const interval = setInterval(() => {
    step++;
    const t  = Math.min(step / totalSteps, 1);
    const mt = 1 - t;
    text.setAttribute('x', mt*mt*from.x + 2*mt*t*cx + t*t*to.x);
    text.setAttribute('y', mt*mt*from.y + 2*mt*t*cy + t*t*to.y);

    if (t >= 1) {
      clearInterval(interval);
      let op = opacity;
      const fade = setInterval(() => {
        op -= 0.18;
        text.setAttribute('opacity', Math.max(0, op));
        if (op <= 0) { clearInterval(fade); text.remove(); }
      }, 28);
    }
  }, tickMs);
}

function animateCommit(entry, clientNodeMap) {
  if (!viz || vizLeaderIdx === null) return;

  const color       = emojiColor(entry.emoji);
  const clientUuid  = clientNodeMap.get(entry.client_id);
  const clientIdx   = clientUuid != null ? (vizNodeIndex.get(clientUuid) ?? vizLeaderIdx) : vizLeaderIdx;

  // Phase 1 — client node → leader (skip if client IS the leader)
  if (clientIdx !== vizLeaderIdx) {
    viz.flashNode(clientIdx, color, { motion: 'reply', resetDelay: 200 });
    drawEmojiComet(clientIdx, vizLeaderIdx, entry.emoji, color, { opacity: 0.45, fontSize: 13 });
  }

  // Phase 2 — leader → acceptors (Accept phase)
  setTimeout(() => {
    viz.flashNode(vizLeaderIdx, color, { motion: 'proposal', resetDelay: 300 });
    viz.drawBeamsTo(vizLeaderIdx, vizAcceptorIdxs, color, 340, 'solid', 25, { motion: 'proposal' });
    vizAcceptorIdxs.forEach((idx, j) => {
      setTimeout(() => drawEmojiComet(vizLeaderIdx, idx, entry.emoji, color, { opacity: 0.65, fontSize: 14 }), j * 25);
    });
  }, 180);

  // Phase 3 — acceptors → leader (votes / quorum replies)
  setTimeout(() => {
    viz.drawBeamsFrom(vizAcceptorIdxs, vizLeaderIdx, color, 260, 'dashed', 30, { motion: 'reply' });
  }, 460);

  // Phase 4 — leader → replicas (commit broadcast)
  setTimeout(() => {
    viz.flashNode(vizLeaderIdx, color, { motion: 'success', resetDelay: 600 });
    vizReplicaIdxs.forEach((idx, j) => {
      setTimeout(() => drawEmojiComet(vizLeaderIdx, idx, entry.emoji, color, { opacity: 1.0, fontSize: 17, glowRadius: 8 }), j * 20);
    });
  }, 650);
}

function animateEntries(entries, clients, config) {
  if (!VISUALIZER_ENABLED) return;
  if (!viz || !config) return;
  const clientNodeMap = new Map((clients || []).map(c => [c.client_id, c.node_id]));
  entries.forEach((entry, i) => setTimeout(() => animateCommit(entry, clientNodeMap), i * 120));
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function shortId(value) {
  if (!value) return "—";
  const text = String(value);
  return text.length > 8 ? text.slice(0, 8) : text;
}

function requestKey(request) {
  return `${request.client_id}:${request.request_id}`;
}

function isTerminalStage(stage) {
  return stage === "applied" || stage === "failed";
}

function clientLabel(value) {
  if (!value) return "—";
  return value.client_name || (value.client_id ? shortId(value.client_id) : "—");
}

function formatConfigId(config) {
  return config?.configuration_id ? shortId(config.configuration_id) : "none";
}

function sameList(a = [], b = []) {
  if (a.length !== b.length) return false;
  const lhs = [...a].sort();
  const rhs = [...b].sort();
  return lhs.every((value, index) => value === rhs[index]);
}

function pushConfigEvent(kind, detail, tone = "info") {
  const previous = configEvents[0];
  if (previous?.kind === kind && previous?.detail === detail) return;
  configEvents.unshift({
    kind,
    detail,
    tone,
    at: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
  });
  configEvents = configEvents.slice(0, 24);
}

function activityKindLabel(kind) {
  return String(kind || "activity").replaceAll("_", " ");
}

function activityTone(kind) {
  if (kind === "client_joined") return "good";
  if (kind === "heartbeat_expired" || kind === "leader_removed" || kind === "room_idle") return "warn";
  return "info";
}

function clientLabelForId(clientId) {
  const client = latestStatus?.clients?.find(item => item.client_id === clientId);
  return client ? clientLabel(client) : clientId ? shortId(clientId) : null;
}

function pushBackendActivity(event) {
  if (!event) return;
  const label = activityKindLabel(event.kind);
  const clientName = clientLabelForId(event.client_id) || event.client_name;
  let detail = "";
  if (event.kind === "client_joined") {
    detail = `${clientName || shortId(event.client_id)} · node ${shortId(event.node_id)}`;
  } else if (event.kind === "heartbeat_expired") {
    detail = `${clientName || shortId(event.client_id)} · node ${shortId(event.node_id)} expired`;
  } else if (event.kind === "leader_removed") {
    detail = `${shortId(event.previous_leader)} → ${shortId(event.next_leader)}`;
  } else if (event.kind === "room_idle") {
    detail = "all nodes decommissioned";
  } else if (event.kind === "configuration_changed") {
    detail = `cfg ${shortId(event.configuration_id)} · leader ${shortId(event.next_leader)}`;
  } else {
    detail = event.message || "";
  }
  if (event.checkpoint_slot != null) {
    detail = `${detail}${detail ? " · " : ""}checkpoint ${event.checkpoint_slot}`;
  }
  if (!detail && event.node_id) detail = `node ${shortId(event.node_id)}`;
  pushConfigEvent(label, detail || "backend activity", activityTone(event.kind));
}

function pruneRequestStates(now = Date.now()) {
  for (const [key, request] of requestStates) {
    if (isTerminalStage(request.stage) && now - (request._seen_at || 0) > REQUEST_TERMINAL_TTL_MS) {
      requestStates.delete(key);
    }
  }
}

function upsertRequestState(request, { snapshot = false } = {}) {
  if (!request?.client_id || request.request_id == null) return;
  const key = requestKey(request);
  const existing = requestStates.get(key);
  if (snapshot && isTerminalStage(request.stage) && !existing) return;
  const seenAt = snapshot && existing ? existing._seen_at : Date.now();
  requestStates.set(key, { ...request, _seen_at: seenAt });
  pruneRequestStates();
  if (requestStates.size > 120) {
    const oldest = [...requestStates.entries()]
      .sort((a, b) => (a[1]._seen_at || 0) - (b[1]._seen_at || 0))[0]?.[0];
    if (oldest) requestStates.delete(oldest);
  }
}

function mergeSnapshotRequests(requests = []) {
  requests.forEach(request => upsertRequestState(request, { snapshot: true }));
}

function observeConfigChanges(config, activeNodes) {
  if (config) {
    if (!previousConfig) {
      pushConfigEvent("configuration active", `cfg ${formatConfigId(config)} · leader ${shortId(config.leader)}`, "good");
    } else if (previousConfig.configuration_id !== config.configuration_id) {
      pushConfigEvent(
        "configuration changed",
        `${formatConfigId(previousConfig)} → ${formatConfigId(config)} · start ${config.start_index}`,
        "warn"
      );
    } else {
      if (previousConfig.leader !== config.leader) {
        pushConfigEvent("leader changed", `${shortId(previousConfig.leader)} → ${shortId(config.leader)}`, "warn");
      }
      if (!sameList(previousConfig.replicas, config.replicas)) {
        pushConfigEvent("replica set changed", `${previousConfig.replicas.length} → ${config.replicas.length}`, "info");
      }
      if (!sameList(previousConfig.acceptors, config.acceptors)) {
        pushConfigEvent("acceptor set changed", `${previousConfig.acceptors.length} → ${config.acceptors.length}`, "info");
      }
      if (previousConfig.start_index !== config.start_index) {
        pushConfigEvent("start index advanced", `${previousConfig.start_index} → ${config.start_index}`, "good");
      }
    }
    previousConfig = {
      configuration_id: config.configuration_id,
      leader: config.leader,
      replicas: [...config.replicas],
      acceptors: [...config.acceptors],
      start_index: config.start_index,
    };
  }

  if (!config && activeNodes === 0 && previousConfig) {
    previousConfig = null;
  }
}

function renderActivity() {
  if (!activityPulse || !activityPipeline || !activityRequests || !activityReorders || !activityConfigLog) return;
  pruneRequestStates();

  const status = latestStatus;
  const state = status?.state;
  const config = status?.active_configuration;
  const requests = [...requestStates.values()]
    .sort((a, b) => (b._seen_at ?? 0) - (a._seen_at ?? 0))
    .slice(0, 10);
  const stageCounts = { proposed: 0, accepted: 0, pending: 0, applied: 0, failed: 0 };
  requestStates.forEach(request => {
    if (stageCounts[request.stage] != null) stageCounts[request.stage] += 1;
  });
  const liveRequests = [...requestStates.values()].filter(request => !isTerminalStage(request.stage));
  const inflightCount = liveRequests.length;
  const oldestLiveMs = liveRequests.reduce((oldest, request) => {
    if (!request._seen_at) return oldest;
    return oldest == null ? request._seen_at : Math.min(oldest, request._seen_at);
  }, null);
  const oldestLiveAge = oldestLiveMs == null ? "—" : `${Math.max(0, Math.floor((Date.now() - oldestLiveMs) / 1000))}s`;
  if (pipelineWindow) {
    pipelineWindow.textContent = `applied/failed clear after ${REQUEST_TERMINAL_TTL_MS / 1000}s`;
  }

  activityPulse.innerHTML = `
    <div><span>config</span><strong>${formatConfigId(config)}</strong></div>
    <div><span>leader</span><strong>${shortId(config?.leader)}</strong></div>
    <div><span>clients</span><strong>${status?.clients?.length ?? 0}</strong></div>
    <div><span>nodes</span><strong>${status?.active_nodes ?? 0}</strong></div>
    <div><span>in flight</span><strong>${inflightCount}</strong></div>
    <div><span>oldest</span><strong>${oldestLiveAge}</strong></div>
    <div><span>slot</span><strong>${state?.cluster_slot ?? "—"}</strong></div>
    <div><span>checkpoint</span><strong>${config?.checkpoint_slot ?? "—"}</strong></div>`;

  activityPipeline.innerHTML = [
    ["proposed", "live"],
    ["accepted", "live"],
    ["pending", "live"],
    ["applied", "recent"],
    ["failed", "recent"],
  ]
    .map(([stage, scope]) => `
      <div class="pipeline-step stage-${stage}">
        <span>${stage}</span>
        <strong>${stageCounts[stage]}</strong>
        <small>${scope}</small>
      </div>`)
    .join("");

  activityRequests.replaceChildren();
  if (!requests.length) {
    activityRequests.innerHTML = `<div class="empty-state compact">waiting for requests</div>`;
  } else {
    requests.slice(0, 8).forEach(request => {
      const row = document.createElement("div");
      row.className = `activity-row stage-${request.stage}`;
      row.innerHTML = `
        <span class="activity-emoji">${escapeHtml(request.emoji)}</span>
        <div>
          <strong>${escapeHtml(clientLabel(request))}</strong>
          <span>req ${request.request_id} · node ${shortId(request.assigned_node)}</span>
        </div>
        <em>${request.stage}</em>`;
      activityRequests.appendChild(row);
    });
  }

  const appliedByRequest = new Map(
    (state?.recent || [])
      .filter(entry => entry.client_id && entry.request_id != null)
      .map(entry => [`${entry.client_id}:${entry.request_id}`, entry])
  );
  const reorders = requests
    .map(request => {
      const applied = appliedByRequest.get(requestKey(request));
      const expected = request.proposed_slot ?? request.slot;
      const appliedSlot = request.applied_slot ?? applied?.slot;
      const shift = Number.isFinite(expected) && Number.isFinite(appliedSlot) ? appliedSlot - expected : null;
      return { request, expected, appliedSlot, shift };
    })
    .filter(item => item.request.stage === "accepted" || item.request.stage === "pending" || (item.shift !== null && item.shift !== 0))
    .slice(0, 8);

  activityReorders.replaceChildren();
  if (!reorders.length) {
    activityReorders.innerHTML = `<div class="empty-state compact">no slot shifts yet</div>`;
  } else {
    reorders.forEach(({ request, expected, appliedSlot, shift }) => {
      const row = document.createElement("div");
      row.className = `activity-row ${shift ? "is-shifted" : ""}`;
      row.innerHTML = `
        <span class="activity-emoji">${escapeHtml(request.emoji)}</span>
        <div>
          <strong>${escapeHtml(clientLabel(request))}</strong>
          <span>expected ${expected ?? "—"} · applied ${appliedSlot ?? "—"}</span>
        </div>
        <em>${shift == null ? request.stage : shift > 0 ? `+${shift}` : String(shift)}</em>`;
      activityReorders.appendChild(row);
    });
  }

  activityConfigLog.replaceChildren();
  if (!configEvents.length) {
    activityConfigLog.innerHTML = `<div class="empty-state compact">waiting for configuration</div>`;
  } else {
    configEvents.slice(0, 10).forEach(event => {
      const row = document.createElement("div");
      row.className = `activity-event tone-${event.tone}`;
      row.innerHTML = `
        <time>${event.at}</time>
        <div>
          <strong>${escapeHtml(event.kind)}</strong>
          <span>${escapeHtml(event.detail)}</span>
        </div>`;
      activityConfigLog.appendChild(row);
    });
  }
}

// ── Render ────────────────────────────────────────────────────────────────
function applyStatus(status) {
  const nodes  = status.active_nodes;
  const config = status.active_configuration;
  const state  = status.state;
  latestStatus = status;
  mergeSnapshotRequests(state?.requests || []);
  observeConfigChanges(config, nodes);

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
        if (newEntries.length) {
          renderLedger();
          if (config) animateEntries(newEntries, status.clients || [], config);
        }
      }
    }
  }

  if (config) {
    window.__lastConfig = config;
    renderSynodConfig(config, nodes, status.clients || []);
    initViz(config);
  }
  renderActivity();
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
      <div class="synod-field-key">checkpoint slot</div>
      <div class="synod-field-val plain">${config.checkpoint_slot ?? "—"}</div>
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
    const clientName = e.client_name ? escapeHtml(e.client_name) : e.client_id ? escapeHtml(e.client_id.slice(0, 8)) : "—";
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
    ws.send(JSON.stringify({ type: "subscribe_observer" }));
  });

  ws.addEventListener("message", event => {
    const msg = JSON.parse(event.data);
    if (msg.type === "joined") {
      applyStatus({ active_nodes: msg.session.active_nodes, active_configuration: null, state: msg.session.state });
    } else if (msg.type === "room_state") {
      applyStatus(msg.status);
    } else if (msg.type === "request_state") {
      upsertRequestState(msg.request);
      renderActivity();
      return;
    } else if (msg.type === "activity") {
      pushBackendActivity(msg.event);
      renderActivity();
      return;
    } else if (msg.type === "applied") {
      renderActivity();
      return;
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

connect();
