const CLIENT_KEY = "synod.main.client_id";
const REQUEST_KEY = "synod.main.request_id";
const NAME_KEY = "synod.main.character_name";

const fallbackEmojiPool = ["🦀", "📦", "🔒", "🧵", "⚙️", "🧪"];
const names = [
  "Crate Runner",
  "Borrow Checker",
  "Thread Keeper",
  "Mutex Rider",
  "Cargo Pilot",
  "Trait Caller",
  "Ferris Bureau",
  "Lifetime Clerk",
];

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
const statusLine = document.querySelector("#statusLine");
const streamStage = document.querySelector("#streamStage");
const heatRow = document.querySelector("#heatRow");

let session = null;
let emojiPool = fallbackEmojiPool;
let spin = 0;
let lastSeenSequence = 0;

function pick(list) {
  return list[Math.floor(Math.random() * list.length)];
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
  const name = stored || pick(names);
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

function renderRoomState(state, { animateRecent = true } = {}) {
  if (!state) return;

  clusterSlot.textContent = String(state.cluster_slot);
  lastApplied.textContent = String(state.last_applied);
  renderHeat(state.heat || []);

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
  renderHeat(state.heat || []);

  for (const event of state.recent || []) {
    if (event.sequence > lastSeenSequence) {
      addStreamEmoji(event.emoji, event.count);
      lastSeenSequence = event.sequence;
    }
  }
}

function addStreamEmoji(emoji, count) {
  const item = document.createElement("div");
  item.className = "floating-emoji";
  item.textContent = emoji;
  item.style.setProperty("--x", `${12 + Math.random() * 76}%`);
  item.style.setProperty("--size", `${24 + Math.min(count || 0, 14) * 2}px`);
  streamStage.appendChild(item);
  item.addEventListener("animationend", () => item.remove());
}

function renderHeat(heat) {
  heatRow.replaceChildren();
  heat.forEach(({ emoji, count }) => {
    const pill = document.createElement("div");
    pill.className = "heat-pill";

    const symbol = document.createElement("span");
    symbol.textContent = emoji;
    const value = document.createElement("strong");
    value.textContent = String(count);

    pill.append(symbol, value);
    heatRow.appendChild(pill);
  });
}

async function refreshStatus({ animateRecent = true } = {}) {
  const response = await fetch("/synod/api/status");
  if (!response.ok) return;
  const status = await response.json();
  activeNodes.textContent = status.active_nodes;
  renderNewRoomEvents(status.state, { animateRecent });
}

async function waitForRequest(requestId) {
  const params = new URLSearchParams({ client_id: session.client_id });

  for (let attempt = 0; attempt < 18; attempt += 1) {
    const response = await fetch(`/synod/api/requests/${requestId}?${params}`);
    if (response.ok) {
      const request = await response.json();
      if (request.stage === "applied") {
        statusLine.textContent = `${request.emoji} applied at slot ${request.slot}.`;
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
  const clientId = localStorage.getItem(CLIENT_KEY);
  const response = await fetch("/synod/api/join", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ client_id: clientId }),
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
}

async function submitPull() {
  if (!session || submitButton.disabled) return;

  const emoji = pickEmoji();
  const requestId = nextRequestId();
  rollTo(emoji);
  submitButton.disabled = true;
  statusLine.textContent = `${emoji} proposed.`;

  const response = await fetch("/synod/api/proposals", {
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
    submitButton.disabled = false;
    return;
  }

  const receipt = await response.json();
  statusLine.textContent = `${receipt.emoji} ${receipt.status.stage} at node ${receipt.assigned_node.slice(0, 8)}.`;
  await waitForRequest(requestId);
  submitButton.disabled = false;
}

setupIdentity();
submitButton.addEventListener("click", submitPull);
joinRoom().catch((err) => {
  statusLine.textContent = err.message;
  submitButton.disabled = true;
});
window.setInterval(() => refreshStatus().catch(() => {}), 1500);
