const ROOM = new URLSearchParams(location.search).get("room") || "main";
const CLIENT_KEY = `synod.${ROOM}.client_id`;
const REQUEST_KEY = `synod.${ROOM}.request_id`;
const NAME_KEY = `synod.${ROOM}.character_name`;
const HEARTBEAT_MS = 15000;
const MAX_LOCAL_ROLLS = 3;
const dieFaces = ["⚀", "⚁", "⚂", "⚃", "⚄", "⚅"];

const fallbackEmojiPool = ["🦀", "📦", "🔒", "🧵", "⚙️", "🧪"];

const nameCorpus = [
  "borrow", "checker", "lifetime", "ownership", "rustacean",
  "mutex", "channel", "thread", "spawner", "closure",
  "iterator", "receiver", "sender", "runtime", "executor",
  "reactor", "allocate", "serialize", "atomic", "scheduler",
  "formatter", "scanner", "tokenizer", "optimizer", "linker",
  "handler", "watcher", "tracer", "builder", "logger",
  "mapper", "emitter", "walker", "seeker", "worker",
  "welder", "hauler", "runner", "keeper", "guard",
  "warden", "drainer", "wrapper", "hasher", "merger",
  "folder", "poller", "waker", "joiner", "binder",
  "forker", "locker", "mover", "cloner", "dropper",
  "freezer", "caster", "aligner", "patcher", "pusher",
  "puller", "feeder", "reader", "writer", "loader",
  "driver", "holder", "catcher", "wrangler", "hooker",
  "flusher", "sealer", "stealer", "leaker", "pinning",
  "boxing", "arcing", "linking", "forking", "locking",
];

function buildMarkov(words, order = 2) {
  const chain = {};
  for (const word of words) {
    const w = "\x00".repeat(order) + word + "\x00";
    for (let i = order; i < w.length; i++) {
      const key = w.slice(i - order, i);
      (chain[key] ??= []).push(w[i]);
    }
  }
  return chain;
}

function markovWord(chain, order = 2, min = 4, max = 9) {
  for (let attempt = 0; attempt < 12; attempt++) {
    let key = "\x00".repeat(order);
    let out = "";
    for (let i = 0; i < max + order * 2; i++) {
      const opts = chain[key];
      if (!opts) break;
      const ch = opts[Math.floor(Math.random() * opts.length)];
      if (ch === "\x00") { if (out.length >= min) break; else continue; }
      out += ch;
      key = (key + ch).slice(-order);
    }
    if (out.length >= min) return out[0].toUpperCase() + out.slice(1);
  }
  return null;
}

const nameChain = buildMarkov(nameCorpus);

function generateName() {
  for (let attempt = 0; attempt < 20; attempt++) {
    const a = markovWord(nameChain);
    const b = markovWord(nameChain);
    if (a && b && a !== b) return `${a} ${b}`;
  }
  return "Ferris Bureau";
}

const rotations = {
  "🦀": "rotateX(-18deg) rotateY(24deg)",
  "📦": "rotateX(-18deg) rotateY(204deg)",
  "🔒": "rotateX(-18deg) rotateY(-66deg)",
  "🧵": "rotateX(-18deg) rotateY(114deg)",
  "⚙️": "rotateX(-108deg) rotateY(24deg)",
  "🧪": "rotateX(72deg) rotateY(24deg)",
};

const clientName = document.querySelector("#clientName");
const clientBadge = document.querySelector("#clientBadge");
const nodeLine = document.querySelector("#nodeLine");
const activeNodes = document.querySelector("#activeNodes");
const clusterSlot = document.querySelector("#clusterSlot");
const lastApplied = document.querySelector("#lastApplied");
const poolSize = document.querySelector("#poolSize");
const emojiCube = document.querySelector("#emojiCube");
const submitButton = document.querySelector("#submitButton");
const submitButtonText = submitButton?.querySelector(".submit-button-text");
const statusLine = document.querySelector("#statusLine");
const streamStage = document.querySelector("#streamStage");
const rollRail = document.querySelector("#rollRail");
const rollRailSummary = document.querySelector("#rollRailSummary");
const rollTrack = document.querySelector("#rollTrack");
const rollViewer = document.querySelector("#rollViewer");
const rollViewerFocus = document.querySelector("#rollViewerFocus");
const rollViewerTable = document.querySelector("#rollViewerTable");

let session = null;
let emojiPool = fallbackEmojiPool;
let spin = 0;
let lastSeenSequence = 0;
let heartbeatTimer = null;
let roomSocket = null;
let proposalQueue = [];
let processingProposals = false;
let cacheGeneration = 0;
let laneRefs = new Map();
let pendingRequests = new Map();
let rollHistory = [];
let selectedRollId = null;

const requiredElements = [
  clientName,
  clientBadge,
  nodeLine,
  activeNodes,
  clusterSlot,
  lastApplied,
  poolSize,
  emojiCube,
  submitButton,
  submitButtonText,
  statusLine,
  streamStage,
  rollRail,
  rollRailSummary,
  rollTrack,
  rollViewer,
  rollViewerFocus,
  rollViewerTable,
];

function pick(list) {
  return list[Math.floor(Math.random() * list.length)];
}

function pickDieFace() {
  return pick(dieFaces) || "⚄";
}

function initials(name) {
  return name
    .split(/\s+/)
    .map((part) => part[0])
    .join("")
    .slice(0, 2)
    .toUpperCase();
}

function setupIdentity() {
  const stored = localStorage.getItem(NAME_KEY);
  const name = stored || generateName();
  localStorage.setItem(NAME_KEY, name);
  clientName.textContent = name;
  clientBadge.textContent = initials(name);
}

function nextRequestId() {
  const current = Number(localStorage.getItem(REQUEST_KEY) || "0") + 1;
  localStorage.setItem(REQUEST_KEY, String(current));
  return current;
}

function isFreshRoomState(state) {
  return Number(state?.cluster_slot || 0) === 0
    && Number(state?.last_applied || 0) === 0;
}

function shouldResetTransientRoomCache(previousClientId, nextSession) {
  if (!nextSession) return false;
  if (previousClientId && nextSession.client_id !== previousClientId) return true;
  return lastSeenSequence > 0 && isFreshRoomState(nextSession.state);
}

function resetTransientRoomCache() {
  cacheGeneration += 1;
  localStorage.removeItem(REQUEST_KEY);
  proposalQueue = [];
  processingProposals = false;
  pendingRequests = new Map();
  rollHistory = [];
  selectedRollId = null;
  lastSeenSequence = 0;
  laneRefs = new Map();
  streamStage?.replaceChildren();
  renderRollRail();
}

function pickEmoji() {
  return pick(emojiPool) || "🦀";
}

function rollTo(emoji) {
  spin += 1;
  emojiCube.style.transform = `${rotations[emoji] || rotations["🦀"]} rotateZ(${spin * 360}deg)`;
}

function setTriggerState(state, label) {
  submitButton.classList.remove("is-idle", "is-arming", "is-submitting");
  submitButton.classList.add(`is-${state}`);
  submitButtonText.textContent = label;
}

function renderRoomState(state, { animateRecent = true } = {}) {
  if (!state) return;

  clusterSlot.textContent = String(state.cluster_slot);
  lastApplied.textContent = String(state.last_applied);
  renderActivityLanes(state.heat || [], state.recent || []);

  for (const event of state.recent || []) {
    if (animateRecent && event.sequence > lastSeenSequence) {
      addStreamEmoji(event.emoji, event.count);
    }
    lastSeenSequence = Math.max(lastSeenSequence, event.sequence);
  }
}

function renderNewRoomEvents(state) {
  if (!state) return;

  clusterSlot.textContent = String(state.cluster_slot);
  lastApplied.textContent = String(state.last_applied);
  renderActivityLanes(state.heat || [], state.recent || []);

  for (const event of state.recent || []) {
    if (event.sequence > lastSeenSequence) {
      addStreamEmoji(event.emoji, event.count);
      lastSeenSequence = event.sequence;
    }
  }
}

function requestMatchesLocalSubmit(request) {
  if (request.client_id !== session?.client_id) return false;
  const local = pendingRequests.get(request.request_id)
    || rollHistory.find((item) => item.requestId === request.request_id);
  return local?.emoji === request.emoji;
}

function currentExpectedSlot() {
  const visibleSlot = Number(clusterSlot.textContent || "0");
  if (Number.isFinite(visibleSlot)) return visibleSlot;
  return Number(lastApplied.textContent || "0") + 1;
}

function slotShift(roll) {
  if (!Number.isFinite(roll.appliedSlot) || !Number.isFinite(roll.expectedSlot)) return null;
  return roll.appliedSlot - roll.expectedSlot;
}

function shiftLabel(shift) {
  if (shift === null) return "pending";
  if (shift === 0) return "+0";
  return shift > 0 ? `+${shift}` : String(shift);
}

function rollTone(roll) {
  if (roll.status === "failed") return "failed";
  const shift = slotShift(roll);
  if (shift === null) return "pending";
  if (shift === 0) return "even";
  if (Math.abs(shift) <= 2) return "shifted";
  return "wide";
}

function createRollRecord({ emoji, requestId, expectedSlot }) {
  const roll = {
    emoji,
    requestId,
    expectedSlot,
    proposedSlot: null,
    appliedSlot: null,
    status: "pending",
  };
  rollHistory.unshift(roll);
  rollHistory = rollHistory.slice(0, MAX_LOCAL_ROLLS);
  selectedRollId = requestId;
  pendingRequests.set(requestId, roll);
  renderRollRail();
  return roll;
}

function updateRollFromRequest(request) {
  if (!requestMatchesLocalSubmit(request)) return;
  const roll = pendingRequests.get(request.request_id)
    || rollHistory.find((item) => item.requestId === request.request_id);
  if (!roll) return;

  roll.proposedSlot = request.proposed_slot ?? request.slot ?? roll.proposedSlot;
  roll.appliedSlot = request.applied_slot ?? roll.appliedSlot;
  roll.status = request.stage;
  selectedRollId = request.request_id;
  if (request.stage === "applied" || request.stage === "failed") {
    pendingRequests.delete(request.request_id);
  }
  renderRollRail();
}

function updateRollFromApplied(entry) {
  if (!entry?.request_id) return;
  const roll = pendingRequests.get(entry.request_id)
    || rollHistory.find((item) => item.requestId === entry.request_id);
  if (!roll || entry.client_id !== session?.client_id) return;

  roll.appliedSlot = entry.slot;
  roll.status = "applied";
  selectedRollId = entry.request_id;
  pendingRequests.delete(entry.request_id);
  renderRollRail();
}

function failRoll(requestId, message) {
  const roll = pendingRequests.get(requestId)
    || rollHistory.find((item) => item.requestId === requestId);
  if (!roll) return;
  roll.status = "failed";
  roll.error = message;
  selectedRollId = requestId;
  pendingRequests.delete(requestId);
  renderRollRail();
}

function renderRollRail() {
  if (!rollRail || !rollTrack) return;

  rollRail.classList.toggle("is-empty", rollHistory.length === 0);
  rollViewer.hidden = rollHistory.length === 0;
  rollRailSummary.textContent = rollHistory.length === 0
    ? "no local rolls"
    : `${rollHistory.length} local ${rollHistory.length === 1 ? "roll" : "rolls"}`;

  rollTrack.replaceChildren();
  for (const roll of rollHistory.slice().reverse()) {
    const shift = slotShift(roll);
    const marker = document.createElement("button");
    marker.type = "button";
    marker.className = "roll-marker";
    marker.dataset.tone = rollTone(roll);
    marker.dataset.selected = String(roll.requestId === selectedRollId);
    marker.setAttribute(
      "aria-label",
      `${roll.emoji} expected slot ${roll.expectedSlot}, ${shift === null ? roll.status : `applied ${roll.appliedSlot}`}`
    );
    marker.innerHTML = `<span>${roll.emoji}</span><strong>${shiftLabel(shift)}</strong>`;
    marker.addEventListener("click", () => {
      selectedRollId = roll.requestId;
      renderRollRail();
    });
    rollTrack.appendChild(marker);
  }

  const selected = rollHistory.find((roll) => roll.requestId === selectedRollId) || rollHistory[0];
  if (!selected) return;

  const selectedShift = slotShift(selected);
  rollViewerFocus.textContent = selectedShift === null
    ? `${selected.emoji} expected slot ${selected.expectedSlot}; waiting for applied slot.`
    : `${selected.emoji} expected ${selected.expectedSlot}, applied ${selected.appliedSlot} (${shiftLabel(selectedShift)}).`;

  rollViewerTable.replaceChildren();
  const header = document.createElement("div");
  header.className = "roll-viewer-row is-header";
  header.innerHTML = "<span></span><span>expected</span><span>applied</span><strong>shift</strong>";
  rollViewerTable.appendChild(header);
  for (const roll of rollHistory.slice(0, MAX_LOCAL_ROLLS)) {
    const shift = slotShift(roll);
    const row = document.createElement("div");
    row.className = "roll-viewer-row";
    row.dataset.tone = rollTone(roll);
    row.innerHTML = `
      <span>${roll.emoji}</span>
      <span>${roll.expectedSlot}</span>
      <span>${roll.appliedSlot ?? "..."}</span>
      <strong>${shiftLabel(shift)}</strong>
    `;
    rollViewerTable.appendChild(row);
  }
}

function renderRequestState(request) {
  if (!requestMatchesLocalSubmit(request)) return;
  updateRollFromRequest(request);

  if (request.stage === "applied") {
    statusLine.textContent = `${request.emoji} applied at slot ${request.applied_slot ?? request.slot}.`;
    setTriggerState("idle", proposalQueue.length > 0 ? `queued ${proposalQueue.length}` : "roll dice");
    return;
  }
  if (request.stage === "failed") {
    statusLine.textContent = request.error || `${request.emoji} failed.`;
    setTriggerState("idle", "roll dice");
    return;
  }

  statusLine.textContent = `${request.emoji} ${request.stage}.`;
}

function renderAppliedEvent(entry) {
  updateRollFromApplied(entry);
  if (entry.sequence > lastSeenSequence) {
    addStreamEmoji(entry.emoji, entry.count);
    lastSeenSequence = entry.sequence;
  }
}

function addStreamEmoji(emoji, count) {
  const lane = laneRefs.get(emoji);
  if (!lane) return;

  const burst = document.createElement("div");
  burst.className = "stream-burst";
  lane.body.appendChild(burst);
  burst.addEventListener("animationend", () => burst.remove());

  const item = document.createElement("div");
  item.className = "floating-emoji";
  item.textContent = emoji;
  item.style.setProperty("--size", `${24 + Math.min(count || 0, 14) * 2}px`);
  item.style.setProperty("--drift", `${(Math.random() - 0.5) * 18}px`);
  item.style.setProperty("--tilt", `${(Math.random() - 0.5) * 18}deg`);
  item.style.setProperty("--rise", `${124 + Math.random() * 34}px`);
  lane.body.appendChild(item);

  lane.root.classList.remove("pulse");
  void lane.root.offsetWidth;
  lane.root.classList.add("pulse");
  item.addEventListener("animationend", () => item.remove());
  window.setTimeout(() => lane.root.classList.remove("pulse"), 480);
}

function ensureActivityScaffold() {
  if (!streamStage) return;
  if (laneRefs.size > 0) return;

  streamStage.replaceChildren();
  emojiPool.forEach((emoji) => {
    const lane = document.createElement("div");
    lane.className = "stream-lane";
    lane.dataset.emoji = emoji;

    const sigil = document.createElement("div");
    sigil.className = "stream-sigil";
    sigil.textContent = emoji;

    const well = document.createElement("div");
    well.className = "stream-well";

    const fill = document.createElement("div");
    fill.className = "stream-fill";

    const body = document.createElement("div");
    body.className = "stream-lane-body";

    const meta = document.createElement("div");
    meta.className = "stream-meta";

    const symbol = document.createElement("span");
    symbol.className = "stream-symbol";
    symbol.textContent = emoji;

    const count = document.createElement("strong");
    count.className = "stream-count";
    count.textContent = "0";

    meta.append(symbol, count);
    well.append(fill, body);
    lane.append(sigil, well, meta);
    streamStage.appendChild(lane);
    laneRefs.set(emoji, { root: lane, body, fill, count, sigil });
  });
}

function renderActivityLanes(heat, recent) {
  if (!streamStage) return;
  const hasPoolChanged =
    laneRefs.size !== emojiPool.length ||
    emojiPool.some((emoji) => !laneRefs.has(emoji));

  if (hasPoolChanged) {
    laneRefs = new Map();
  }

  ensureActivityScaffold();

  const heatMap = new Map(heat.map(({ emoji, count }) => [emoji, count]));
  const rankMap = new Map(
    [...heat]
      .sort((a, b) => b.count - a.count || emojiPool.indexOf(a.emoji) - emojiPool.indexOf(b.emoji))
      .map(({ emoji }, index) => [emoji, index + 1])
  );
  const max = Math.max(...heat.map((entry) => entry.count), 1);
  const hasActivity = recent.length > 0 || heat.some((entry) => entry.count > 0);
  streamStage.classList.toggle("has-activity", hasActivity);

  emojiPool.forEach((emoji) => {
    const lane = laneRefs.get(emoji);
    if (!lane) return;
    const count = heatMap.get(emoji) || 0;
    const ratio = count / max;
    lane.root.style.setProperty("--heat", String(ratio));
    lane.root.dataset.rank = String(rankMap.get(emoji) || emojiPool.length);
    lane.count.textContent = String(count);
    lane.root.classList.toggle("is-hot", count > 0 && count === max);
  });
}

async function joinRoom() {
  submitButton.disabled = true;
  const clientId = localStorage.getItem(CLIENT_KEY);
  const clientName = localStorage.getItem(NAME_KEY);
  const previousClientId = session?.client_id || clientId;
  const response = await fetch(`/synod/api/join?room=${encodeURIComponent(ROOM)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ client_id: clientId, client_name: clientName }),
  });

  if (!response.ok) {
    throw new Error(await response.text());
  }

  const nextSession = await response.json();
  if (shouldResetTransientRoomCache(previousClientId, nextSession)) {
    resetTransientRoomCache();
  }
  session = nextSession;
  localStorage.setItem(CLIENT_KEY, session.client_id);
  emojiPool = session.emoji_pool.length ? session.emoji_pool : fallbackEmojiPool;
  nodeLine.textContent = `node ${session.assigned_node.slice(0, 8)} · client ${session.client_id.slice(0, 8)}`;
  activeNodes.textContent = session.active_nodes;
  poolSize.textContent = `${emojiPool.length} in pool`;
  rollTo(emojiPool[0] || "🦀");
  renderRoomState(session.state, { animateRecent: false });
  statusLine.textContent = "Ready.";
  setTriggerState("idle", "roll dice");
  submitButton.disabled = false;
  connectRoomSocket();
}

async function sendHeartbeat() {
  if (!session) return;
  const response = await fetch(`/synod/api/heartbeat?room=${encodeURIComponent(ROOM)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ client_id: session.client_id }),
  });

  if (response.status === 404) {
    session = null;
    resetTransientRoomCache();
    statusLine.textContent = "Room session expired. Rejoining.";
    await joinRoom();
    return;
  }

  if (!response.ok) return;
  const heartbeat = await response.json();
  activeNodes.textContent = heartbeat.active_nodes;
}

function startHeartbeat() {
  if (heartbeatTimer) {
    window.clearInterval(heartbeatTimer);
  }
  sendHeartbeat().catch(() => {});
  heartbeatTimer = window.setInterval(() => {
    sendHeartbeat().catch(() => {});
  }, HEARTBEAT_MS);
}

function socketUrl() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const params = new URLSearchParams({ room: ROOM });
  return `${protocol}//${window.location.host}/synod/ws?${params}`;
}

function connectRoomSocket() {
  if (!window.WebSocket || !session) {
    startHeartbeat();
    return;
  }
  if (heartbeatTimer) {
    window.clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
  if (roomSocket) {
    roomSocket.close();
  }

  roomSocket = new WebSocket(socketUrl());
  roomSocket.addEventListener("message", (event) => {
    const message = JSON.parse(event.data);
    if (message.type === "joined") {
      const nextSession = message.session;
      if (shouldResetTransientRoomCache(session?.client_id, nextSession)) {
        resetTransientRoomCache();
      }
      session = nextSession;
      localStorage.setItem(CLIENT_KEY, session.client_id);
      emojiPool = session.emoji_pool.length ? session.emoji_pool : fallbackEmojiPool;
      nodeLine.textContent = `node ${session.assigned_node.slice(0, 8)} · client ${session.client_id.slice(0, 8)}`;
      activeNodes.textContent = session.active_nodes;
      renderRoomState(session.state, { animateRecent: false });
      return;
    }
    if (message.type === "heartbeat") {
      const heartbeat = message.heartbeat;
      activeNodes.textContent = heartbeat.active_nodes;
      return;
    }
    if (message.type === "room_state") {
      const status = message.status;
      activeNodes.textContent = status.active_nodes;
      renderNewRoomEvents(status.state);
      return;
    }
    if (message.type === "request_state") {
      renderRequestState(message.request);
      return;
    }
    if (message.type === "applied") {
      renderAppliedEvent(message.entry);
      return;
    }
    if (message.type === "error") {
      statusLine.textContent = message.message || message.Error?.message || "socket error";
    }
  });

  roomSocket.addEventListener("open", () => {
    roomSocket.send(JSON.stringify({
      type: "join_client",
      client_id: session.client_id,
      client_name: localStorage.getItem(NAME_KEY),
    }));
    heartbeatTimer = window.setInterval(() => {
      if (roomSocket?.readyState === WebSocket.OPEN) {
        roomSocket.send(JSON.stringify({ type: "heartbeat" }));
      }
    }, HEARTBEAT_MS);
  });

  roomSocket.addEventListener("close", () => {
    if (heartbeatTimer) {
      window.clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
    window.setTimeout(() => {
      if (session) connectRoomSocket();
    }, 2000);
  });
}

function submitPull() {
  if (!session) return;

  const emoji = pickEmoji();
  const requestId = nextRequestId();
  const expectedSlot = currentExpectedSlot();
  submitButton.dataset.face = pickDieFace();
  setTriggerState("arming", "rolling");
  rollTo(emoji);
  proposalQueue.push({ emoji, requestId, expectedSlot });

  if (!processingProposals) {
    drainProposalQueue();
  }
}

async function drainProposalQueue() {
  const generation = cacheGeneration;
  processingProposals = true;
  while (proposalQueue.length > 0 && generation === cacheGeneration) {
    const { emoji, requestId, expectedSlot } = proposalQueue.shift();
    const activeSession = session;
    if (!activeSession) break;
    const queued = proposalQueue.length;
    createRollRecord({ emoji, requestId, expectedSlot });
    setTriggerState("submitting", queued > 0 ? `queued ${queued + 1}` : "submitting");
    statusLine.textContent = queued > 0
      ? `${emoji} proposed. (${queued} queued)`
      : `${emoji} proposed.`;

    const response = await fetch(`/synod/api/proposals?room=${encodeURIComponent(ROOM)}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        client_id: activeSession.client_id,
        request_id: requestId,
        emoji,
      }),
    });

    if (generation !== cacheGeneration) break;

    if (!response.ok) {
      const message = await response.text();
      statusLine.textContent = message;
      failRoll(requestId, message);
      setTriggerState("idle", "roll dice");
      continue;
    }

    const receipt = await response.json();
    if (generation !== cacheGeneration) break;
    updateRollFromRequest(receipt.status);
    statusLine.textContent = `${receipt.emoji} ${receipt.status.stage} at node ${receipt.assigned_node.slice(0, 8)}.`;
    setTriggerState("idle", proposalQueue.length > 0 ? `queued ${proposalQueue.length}` : "roll dice");
  }
  processingProposals = false;
  setTriggerState("idle", "roll dice");
}

if (requiredElements.every(Boolean)) {
  setupIdentity();
  submitButton.dataset.face = pickDieFace();
  setTriggerState("idle", "joining");
  submitButton.addEventListener("click", submitPull);
  joinRoom().catch((err) => {
    statusLine.textContent = err.message;
    setTriggerState("idle", "offline");
    submitButton.disabled = true;
  });
}
