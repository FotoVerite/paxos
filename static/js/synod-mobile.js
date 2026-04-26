const ROOM = new URLSearchParams(location.search).get("room") || "main";
const CLIENT_KEY = `synod.${ROOM}.client_id`;
const REQUEST_KEY = `synod.${ROOM}.request_id`;
const NAME_KEY = `synod.${ROOM}.character_name`;
const HEARTBEAT_MS = 15000;
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

let session = null;
let emojiPool = fallbackEmojiPool;
let spin = 0;
let lastSeenSequence = 0;
let heartbeatTimer = null;
let roomSocket = null;
let proposalQueue = [];
let processingProposals = false;
let laneRefs = new Map();

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

function addStreamEmoji(emoji, count) {
  const lane = laneRefs.get(emoji);
  if (!lane) return;

  const item = document.createElement("div");
  item.className = "floating-emoji";
  item.textContent = emoji;
  item.style.setProperty("--size", `${24 + Math.min(count || 0, 14) * 2}px`);
  item.style.setProperty("--drift", `${(Math.random() - 0.5) * 12}px`);
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
  const empty = document.createElement("div");
  empty.className = "stream-empty";
  empty.textContent = "quiet chamber, waiting for the next proposal";
  streamStage.appendChild(empty);

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

async function refreshStatus({ animateRecent = true } = {}) {
  const response = await fetch(`/synod/api/status?room=${encodeURIComponent(ROOM)}`);
  if (!response.ok) return;
  const status = await response.json();
  activeNodes.textContent = status.active_nodes;
  renderNewRoomEvents(status.state, { animateRecent });
}

async function waitForRequest(requestId) {
  const params = new URLSearchParams({ client_id: session.client_id, room: ROOM });

  for (let attempt = 0; attempt < 18; attempt += 1) {
    const response = await fetch(`/synod/api/requests/${requestId}?${params}`);
    if (response.ok) {
      const request = await response.json();
      if (request.stage === "applied") {
        statusLine.textContent = `${request.emoji} applied at slot ${request.applied_slot ?? request.slot}.`;
        await refreshStatus();
        return;
      }
      if (request.stage === "failed") {
        statusLine.textContent = request.error || `${request.emoji} failed.`;
        return;
      }
      statusLine.textContent = `${request.emoji} ${request.stage}.`;
    }
    await new Promise((resolve) => window.setTimeout(resolve, 220));
  }

  statusLine.textContent = "Still pending. Paxos is being Paxos.";
}

async function joinRoom() {
  submitButton.disabled = true;
  const clientId = localStorage.getItem(CLIENT_KEY);
  const clientName = localStorage.getItem(NAME_KEY);
  const response = await fetch(`/synod/api/join?room=${encodeURIComponent(ROOM)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ client_id: clientId, client_name: clientName }),
  });

  if (!response.ok) {
    throw new Error(await response.text());
  }

  session = await response.json();
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
    localStorage.removeItem(CLIENT_KEY);
    session = null;
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
  if (session?.client_id) {
    params.set("client_id", session.client_id);
  }
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
      session = message.session;
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
    if (message.type === "error") {
      statusLine.textContent = message.message || message.Error?.message || "socket error";
    }
  });

  roomSocket.addEventListener("open", () => {
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
  submitButton.dataset.face = pickDieFace();
  setTriggerState("arming", "rolling");
  rollTo(emoji);
  proposalQueue.push({ emoji, requestId });

  if (!processingProposals) {
    drainProposalQueue();
  }
}

async function drainProposalQueue() {
  processingProposals = true;
  while (proposalQueue.length > 0) {
    const { emoji, requestId } = proposalQueue.shift();
    const queued = proposalQueue.length;
    setTriggerState("submitting", queued > 0 ? `queued ${queued + 1}` : "submitting");
    statusLine.textContent = queued > 0
      ? `${emoji} proposed. (${queued} queued)`
      : `${emoji} proposed.`;

    const response = await fetch(`/synod/api/proposals?room=${encodeURIComponent(ROOM)}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        client_id: session.client_id,
        request_id: requestId,
        emoji,
      }),
    });

    if (!response.ok) {
      statusLine.textContent = await response.text();
      setTriggerState("idle", "roll dice");
      continue;
    }

    const receipt = await response.json();
    statusLine.textContent = `${receipt.emoji} ${receipt.status.stage} at node ${receipt.assigned_node.slice(0, 8)}.`;
    await waitForRequest(requestId);
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
  window.setInterval(() => refreshStatus().catch(() => {}), 1500);
}
