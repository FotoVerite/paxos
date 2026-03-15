function shortClientLabel(clientId, fallbackIndex = 0) {
  if (typeof clientId === 'string' && clientId.length >= 4) {
    return `C${fallbackIndex}`;
  }
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
  return null;
}

function normalizeClientId(clientId) {
  if (typeof clientId !== 'string') return null;
  const normalized = clientId.trim().toLowerCase();
  return normalized.length > 0 ? normalized : null;
}

function isUserClientOperation(operation) {
  if (!operation || typeof operation !== 'object') return false;
  return Boolean(operation.PUT || operation.GET || operation.ADD);
}

function summarizeOperation(operation) {
  if (typeof operation === 'string') return operation;
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

function isStopOperation(operation) {
  if (!operation) return false;
  if (typeof operation === 'string') return operation === 'STOP';
  if (typeof operation !== 'object') return false;
  if (operation.ClientRequest && typeof operation.ClientRequest === 'object') {
    return isStopOperation(operation.ClientRequest.cmd);
  }
  return Object.prototype.hasOwnProperty.call(operation, 'STOP');
}

function commandLabel(cmd) {
  if (!cmd) return 'none';
  if (typeof cmd === 'string') return cmd;
  if (cmd.ClientRequest && typeof cmd.ClientRequest === 'object') {
    return commandLabel(cmd.ClientRequest.cmd);
  }
  if (typeof cmd !== 'object') return String(cmd);
  const [kind] = Object.keys(cmd);
  return kind || 'unknown';
}

function normalizeOutcome(outcome) {
  return String(outcome || '').toLowerCase();
}

function statusText(token) {
  if (token.learned) return 'applied';
  if (token.state === 'stopped') return 'stopped';
  if (token.state === 'queued') return 'queued';
  if (token.state === 'padded') return 'padded';
  if (token.state === 'cached') return 'cached';
  if (token.state === 'admitted') return 'admitted';
  return 'sent';
}

function statusClass(token) {
  if (token.learned) return 'applied';
  if (token.state === 'stopped') return 'stopped';
  if (token.state === 'queued') return 'queued';
  if (token.state === 'padded') return 'padded';
  if (token.state === 'cached') return 'cached';
  if (token.state === 'admitted') return 'admitted';
  return 'sent';
}

export function createClientLanesPlugin({
  container,
  maxRequestsPerClient = 8,
} = {}) {
  const clients = new Map();

  function ensureClient(clientId) {
    if (!clientId) return null;
    if (!clients.has(clientId)) {
      clients.set(clientId, {
        id: clientId,
        label: shortClientLabel(clientId, clients.size),
        requests: new Map(),
        order: [],
      });
    }
    return clients.get(clientId);
  }

  function makeRequestKey(clientId, requestId, fallbackSlot) {
    if (requestId !== null && requestId !== undefined) {
      return `${clientId}:${requestId}`;
    }
    return `${clientId}:slot:${fallbackSlot ?? 'na'}`;
  }

  function touchRequest(client, key) {
    const idx = client.order.indexOf(key);
    if (idx >= 0) {
      client.order.splice(idx, 1);
    }
    client.order.unshift(key);
    if (client.order.length > maxRequestsPerClient) {
      const trimmed = client.order.splice(maxRequestsPerClient);
      for (const stale of trimmed) {
        client.requests.delete(stale);
      }
    }
  }

  function ensureRequest(client, key, defaults) {
    if (!client.requests.has(key)) {
      client.requests.set(key, {
        key,
        requestId: defaults.requestId ?? null,
        slot: defaults.slot ?? null,
        op: defaults.op ?? 'cmd',
        state: 'sent',
        learned: false,
        pulseUntil: 0,
        updatedAt: Date.now(),
      });
    }
    const request = client.requests.get(key);
    if (defaults.requestId !== null && defaults.requestId !== undefined) {
      request.requestId = defaults.requestId;
    }
    if (defaults.slot !== null && defaults.slot !== undefined) {
      request.slot = defaults.slot;
    }
    if (defaults.op) {
      request.op = defaults.op;
    }
    request.updatedAt = Date.now();
    request.pulseUntil = Date.now() + 750;
    touchRequest(client, key);
    return request;
  }

  function render() {
    if (!container) return;
    if (clients.size === 0) {
      container.innerHTML =
        '<div class="client-lanes-empty">No client traffic yet</div>';
      return;
    }

    const rows = Array.from(clients.values())
      .sort((a, b) => a.label.localeCompare(b.label))
      .map((client) => {
        const tokens = client.order
          .map((key) => client.requests.get(key))
          .filter(Boolean)
          .map((token) => {
            const pulseClass = token.pulseUntil > Date.now() ? ' pulse' : '';
            const klass = statusClass(token);
            const reqText =
              token.requestId !== null && token.requestId !== undefined
                ? `r${token.requestId}`
                : `s${token.slot ?? '-'}`;
            const recvClass = token.learned ? 'on' : 'off';
            return `
              <div class="client-lane-token ${klass}${pulseClass}">
                <div class="client-lane-token-head">
                  <span class="req">${reqText}</span>
                  <span class="state">${statusText(token)}</span>
                </div>
                <div class="client-lane-op">${token.op}</div>
                <div class="client-lane-flow">
                  <span class="flow-step on">send</span>
                  <span class="flow-arrow">→</span>
                  <span class="flow-step ${klass === 'sent' ? 'off' : 'on'}">gate</span>
                  <span class="flow-arrow">→</span>
                  <span class="flow-step ${recvClass}">apply</span>
                </div>
              </div>
            `;
          })
          .join('');

        return `
          <div class="client-lane-row">
            <div class="client-lane-head">
              <span class="client-lane-id">${client.label}</span>
              <span class="client-lane-count">${client.order.length}</span>
            </div>
            <div class="client-lane-track">${tokens || '<div class="client-lanes-empty">No requests</div>'}</div>
          </div>
        `;
      })
      .join('');

    container.innerHTML = rows;
  }

  return {
    init() {
      render();
    },

    onReset() {
      clients.clear();
      render();
    },

    onEvent({ eventType, eventData }) {
      if (!eventData) return;

      if (eventType === 'PmmcPropose') {
        const info = extractClientInfo(eventData.cmd);
        if (!info) return;
        if (isStopOperation(info.operation)) return;
        if (!isUserClientOperation(info.operation)) return;
        const normalizedClientId = normalizeClientId(info.clientId);
        if (!normalizedClientId) return;
        const client = ensureClient(normalizedClientId);
        if (!client) return;

        const key = makeRequestKey(
          normalizedClientId,
          info.requestId,
          eventData.slot,
        );
        ensureRequest(client, key, {
          requestId: info.requestId,
          slot: eventData.slot,
          op: summarizeOperation(info.operation),
        });
        render();
        return;
      }

      if (eventType === 'ReconfigurationProposalObserved') {
        const info = extractClientInfo(eventData.original_cmd);
        if (!info) return;
        if (isStopOperation(info.operation)) return;
        if (!isUserClientOperation(info.operation)) return;
        const normalizedClientId = normalizeClientId(info.clientId);
        if (!normalizedClientId) return;
        const client = ensureClient(normalizedClientId);
        if (!client) return;

        const key = makeRequestKey(
          normalizedClientId,
          info.requestId,
          eventData.slot,
        );
        const request = ensureRequest(client, key, {
          requestId: info.requestId,
          slot: eventData.slot,
          op: summarizeOperation(info.operation),
        });
        const outcome = normalizeOutcome(eventData.outcome);
        if (outcome === 'stopped') {
          request.state = 'stopped';
        } else if (outcome === 'queued') {
          request.state = 'queued';
        } else if (outcome === 'cachedresponse') {
          request.state = 'cached';
        } else if (outcome === 'admitted') {
          const original = commandLabel(eventData.original_cmd);
          const effective = commandLabel(eventData.effective_cmd);
          request.state = original !== effective && effective === 'NOOP' ? 'padded' : 'admitted';
        }
        render();
        return;
      }

      if (eventType === 'LearnedValue') {
        const info = extractClientInfo(eventData.value);
        if (!info) return;
        if (isStopOperation(info.operation)) return;
        if (!isUserClientOperation(info.operation)) return;
        const normalizedClientId = normalizeClientId(info.clientId);
        if (!normalizedClientId) return;
        const client = ensureClient(normalizedClientId);
        if (!client) return;

        const key = makeRequestKey(
          normalizedClientId,
          info.requestId,
          eventData.decree_num,
        );
        const request = ensureRequest(client, key, {
          requestId: info.requestId,
          slot: eventData.decree_num,
          op: summarizeOperation(info.operation),
        });
        request.learned = true;
        request.state = 'applied';
        request.pulseUntil = Date.now() + 900;
        render();
      }
    },
  };
}
