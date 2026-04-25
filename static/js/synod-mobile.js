const CLIENT_KEY = "synod.main.client_id";
const REQUEST_KEY = "synod.main.request_id";

const clientName = document.querySelector("#clientName");
const activeNodes = document.querySelector("#activeNodes");
const poolSize = document.querySelector("#poolSize");
const emojiDie = document.querySelector("#emojiDie");
const submitButton = document.querySelector("#submitButton");
const statusLine = document.querySelector("#statusLine");

let session = null;
let emojiPool = [];

function nextRequestId() {
  const current = Number(localStorage.getItem(REQUEST_KEY) || "0") + 1;
  localStorage.setItem(REQUEST_KEY, String(current));
  return current;
}

function pickEmoji() {
  return emojiPool[Math.floor(Math.random() * emojiPool.length)] || "🦀";
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
  emojiPool = session.emoji_pool;
  clientName.textContent = session.client_id.slice(0, 8);
  activeNodes.textContent = session.active_nodes;
  poolSize.textContent = `${emojiPool.length} in pool`;
  emojiDie.textContent = emojiPool[0] || "🦀";
  statusLine.textContent = "Ready.";
}

async function submitPull() {
  if (!session || submitButton.disabled) return;

  const emoji = pickEmoji();
  emojiDie.textContent = emoji;
  emojiDie.classList.remove("is-rolling");
  void emojiDie.offsetWidth;
  emojiDie.classList.add("is-rolling");
  submitButton.disabled = true;
  statusLine.textContent = `${emoji} proposed.`;

  const response = await fetch("/synod/api/proposals", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      client_id: session.client_id,
      request_id: nextRequestId(),
      emoji,
    }),
  });

  if (!response.ok) {
    statusLine.textContent = await response.text();
    submitButton.disabled = false;
    return;
  }

  const receipt = await response.json();
  statusLine.textContent = `${receipt.emoji} accepted by node ${receipt.assigned_node.slice(0, 8)}.`;
  submitButton.disabled = false;
}

submitButton.addEventListener("click", submitPull);
joinRoom().catch((err) => {
  statusLine.textContent = err.message;
  submitButton.disabled = true;
});
