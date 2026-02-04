/**
 * Paxos Demo Controller
 * Centralizes WebSocket, queueing, and plugin dispatch for demos
 */

import { EventQueue } from '/event-queue.js';

export function createPaxosDemoController({
  state,
  visualizer,
  plugins = [],
  canCommunicate = () => true,
  websocketPath = '/ws',
  queueWindow = 50,
}) {
  const context = {
    state,
    visualizer,
    canCommunicate,
  };

  let ws = null;
  const eventQueue = new EventQueue(handleEvent, queueWindow);

  function safeCall(label, fn, ...args) {
    try {
      return fn(...args);
    } catch (err) {
      console.error(`[paxos-demo] ${label} error:`, err);
      return null;
    }
  }

  function initPlugins() {
    for (const plugin of plugins) {
      if (plugin && typeof plugin.init === 'function') {
        safeCall('plugin.init', plugin.init, context);
      }
    }
  }

  async function handleEvent(queuedEvent) {
    const { eventType, eventData } = queuedEvent;
    const event = { eventType, eventData };

    for (const plugin of plugins) {
      if (!plugin || typeof plugin.onEvent !== 'function') {
        continue;
      }
      try {
        await plugin.onEvent(event, context);
      } catch (err) {
        console.error(`[paxos-demo] plugin.onEvent error for ${eventType}:`, err);
      }
    }
  }

  function handleMessage(message) {
    if (message.ClusterInitialized) {
      state.initialize(message.ClusterInitialized);

      for (const plugin of plugins) {
        if (plugin && typeof plugin.onCluster === 'function') {
          safeCall('plugin.onCluster', plugin.onCluster, message.ClusterInitialized, context);
        }
      }
      return;
    }

    if (message.Event) {
      const eventType = Object.keys(message.Event)[0];
      const eventData = message.Event[eventType];
      if (!eventData) return;

      if (state.snapshot().simulation.running) {
        eventQueue.push({
          eventType,
          eventData,
          created_at: eventData.created_at || performance.now(),
        });
      }
    }
  }

  function connect() {
    if (ws) ws.close();
    ws = new WebSocket(getWebSocketURL(websocketPath));
    ws.onmessage = (event) => {
      const message = JSON.parse(event.data);
      handleMessage(message);
    };
    return ws;
  }

  function disconnect() {
    if (ws) {
      ws.close();
      ws = null;
    }
  }

  function reset() {
    eventQueue.clear();
    for (const plugin of plugins) {
      if (plugin && typeof plugin.onReset === 'function') {
        safeCall('plugin.onReset', plugin.onReset, context);
      }
    }
  }

  initPlugins();

  return {
    connect,
    disconnect,
    reset,
    eventQueue,
    get socket() {
      return ws;
    },
  };
}
