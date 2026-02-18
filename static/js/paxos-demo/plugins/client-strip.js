function shortClientLabel(clientId, fallbackIndex = 0) {
  return `C${fallbackIndex}`;
}

function extractClientInfo(cmd) {
  if (!cmd || typeof cmd !== 'object') return null;

  if (cmd.ClientRequest && typeof cmd.ClientRequest === 'object') {
    const inner = cmd.ClientRequest;
    return {
      clientId: inner.client_id || null,
      requestId: inner.request_id ?? null,
      operation: inner.cmd || null,
    };
  }

  return {
    clientId: null,
    requestId: null,
    operation: cmd,
  };
}

function summarizeOperation(operation) {
  if (!operation || typeof operation !== 'object') return 'cmd';
  if (operation.PUT) {
    const { key, value } = operation.PUT;
    return `PUT ${key}=${value}`;
  }
  if (operation.GET) {
    return `GET ${operation.GET.key}`;
  }
  if (operation.ADD) {
    const { key, value } = operation.ADD;
    return `ADD ${key}+${value}`;
  }
  const [kind] = Object.keys(operation);
  return kind || 'cmd';
}

function applyOperation(kv, operation) {
  if (!operation || typeof operation !== 'object') return;
  if (operation.PUT) {
    kv.set(String(operation.PUT.key), Number(operation.PUT.value));
    return;
  }
  if (operation.ADD) {
    const key = String(operation.ADD.key);
    const next = (kv.get(key) || 0) + Number(operation.ADD.value || 0);
    kv.set(key, next);
  }
}

export function createClientStripPlugin({
  container,
  feed,
  snapshotContainer,
  maxFeed = 10,
} = {}) {
  const clients = new Map();
  const seenLearned = new Set();
  const pendingRequests = new Set();
  const latestRequestBySlot = new Map();
  let fallbackClientCounter = 0;
  let selectedClientKey = null;

  function ensureClient(rawClientId) {
    let key = rawClientId || 'fallback-0';
    if (!rawClientId) {
      fallbackClientCounter = Math.max(fallbackClientCounter, 1);
    }
    if (!clients.has(key)) {
      const label = shortClientLabel(rawClientId, clients.size);
      clients.set(key, {
        label,
        sends: 0,
        learned: 0,
        pending: 0,
        lastReq: null,
        kv: new Map(),
      });
      if (selectedClientKey === null) {
        selectedClientKey = key;
      }
    }
    return key;
  }

  function renderSnapshot() {
    if (!snapshotContainer) return;
    if (!selectedClientKey || !clients.has(selectedClientKey)) {
      snapshotContainer.innerHTML = '<div class="client-empty">Select a client for snapshot</div>';
      return;
    }

    const client = clients.get(selectedClientKey);
    const rows = Array.from(client.kv.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([key, value]) => `<div class="client-kv-row"><span class="k">${key}</span><span class="v">${value}</span></div>`)
      .join('');

    snapshotContainer.innerHTML = `
      <div class="client-snapshot-head">${client.label} KV Snapshot</div>
      <div class="client-snapshot-meta">sent ${client.sends} • learned ${client.learned} • pending ${client.pending}</div>
      <div class="client-kv-list">${rows || '<div class="client-empty">No applied commands yet</div>'}</div>
    `;
  }

  function render() {
    if (!container) return;
    const rows = Array.from(clients.entries())
      .map(([key, c]) => {
        const selected = key === selectedClientKey ? ' selected' : '';
        return `<button type="button" class="client-pill${selected}" data-client-key="${key}"><span class="client-id">${c.label}</span><span class="client-meta">${c.sends}/${c.learned}</span></button>`;
      })
      .join('');
    container.innerHTML = rows || '<div class="client-empty">No clients yet</div>';
    renderSnapshot();
  }

  function pushFeed(line) {
    if (!feed) return;
    const entry = document.createElement('div');
    entry.className = 'client-feed-item';
    entry.textContent = line;
    feed.prepend(entry);
    while (feed.children.length > maxFeed) {
      feed.removeChild(feed.lastChild);
    }
  }

  function handleContainerClick(event) {
    const target = event.target.closest('[data-client-key]');
    if (!target || !container.contains(target)) return;
    const key = target.dataset.clientKey;
    if (!key || !clients.has(key)) return;
    selectedClientKey = key;
    render();
  }

  return {
    init() {
      if (container) {
        container.addEventListener('click', handleContainerClick);
      }
      render();
    },

    onReset() {
      clients.clear();
      seenLearned.clear();
      pendingRequests.clear();
      latestRequestBySlot.clear();
      fallbackClientCounter = 0;
      selectedClientKey = null;
      if (feed) feed.innerHTML = '';
      render();
    },

    onEvent({ eventType, eventData }, ctx) {
      if (!eventData) return;

      if (eventType === 'PmmcPropose') {
        const info = extractClientInfo(eventData.cmd);
        const key = ensureClient(info?.clientId);
        const client = clients.get(key);
        if (!client) return;

        client.sends += 1;
        client.lastReq = info?.requestId ?? null;
        const requestKey = info?.requestId !== null && info?.requestId !== undefined
          ? `${info.clientId}:${info.requestId}`
          : null;
        const slotKey = info?.clientId !== null && info?.clientId !== undefined
          ? `${info.clientId}:slot:${eventData.slot ?? 'unknown'}`
          : null;

        if (requestKey && slotKey) {
          const previousRequest = latestRequestBySlot.get(slotKey);
          if (previousRequest && previousRequest !== requestKey && pendingRequests.has(previousRequest)) {
            pendingRequests.delete(previousRequest);
            client.pending = Math.max(0, client.pending - 1);
          }
          latestRequestBySlot.set(slotKey, requestKey);
        }

        if (requestKey && !seenLearned.has(requestKey) && !pendingRequests.has(requestKey)) {
          pendingRequests.add(requestKey);
          client.pending += 1;
        }

        const target = ctx?.nodeLabels?.node
          ? ctx.nodeLabels.node(eventData.id)
          : `Node ${eventData.id}`;
        const op = summarizeOperation(info?.operation || eventData.cmd);
        const reqText = info?.requestId !== null && info?.requestId !== undefined
          ? ` r${info.requestId}`
          : '';
        pushFeed(`${client.label}${reqText} -> ${target}: ${op}`);
        render();
        return;
      }

      if (eventType === 'LearnedValue') {
        const info = extractClientInfo(eventData.value);
        if (!info || !info.clientId) return;
        const learnedKey = info.requestId !== null && info.requestId !== undefined
          ? `${info.clientId}:${info.requestId}`
          : `${info.clientId}:decree:${eventData.decree_num ?? 'unknown'}`;
        if (seenLearned.has(learnedKey)) return;
        seenLearned.add(learnedKey);

        const key = ensureClient(info.clientId);
        const client = clients.get(key);
        if (!client) return;

        client.learned += 1;
        if (pendingRequests.has(learnedKey)) {
          pendingRequests.delete(learnedKey);
          client.pending = Math.max(0, client.pending - 1);
        }
        applyOperation(client.kv, info.operation);
        const op = summarizeOperation(info.operation);
        pushFeed(`${client.label} applied: ${op}`);
        render();
      }
    },
  };
}
