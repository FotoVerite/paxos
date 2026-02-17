/**
 * Event Reader
 * Owns WebSocket connection and batching/ingestion controls.
 */

import { EventQueue } from '/event-queue.js';

export function createEventReader({
  websocketPath = '/ws',
  frameWindowMicros = 50,
  onCluster,
  onBatch,
  onMessage,
}) {
  let ws = null;
  let captureEnabled = false;
  let generation = 0;
  let ingestionPaused = false;
  let resumeOnCluster = false;

  const BOOTSTRAP_EVENTS = new Set([
    'NodeCapabilities',
    'LeaderElected',
    'InitialDecree',
    'BatchInitialDecrees',
    'LedgerDump',
  ]);

  const eventQueue = new EventQueue(null, frameWindowMicros, {
    handleBatch: handleBatch,
  });

  function normalizeTimestamp(value) {
    if (typeof value === 'number') {
      return value;
    }
    return Math.round(performance.now() * 1000);
  }

  async function handleBatch(batch) {
    if (!batch || batch.length === 0) return;
    const batchGeneration = batch[0]?.generation;
    if (batchGeneration !== undefined && batchGeneration !== generation) {
      return;
    }
    if (typeof onBatch === 'function') {
      await onBatch(batch);
    }
  }

  function handleMessage(message) {
    if (message.ClusterInitialized) {
      if (typeof onCluster === 'function') {
        onCluster(message.ClusterInitialized);
      }
      if (resumeOnCluster) {
        resumeIngestion();
      }
      return;
    }

    if (message.Event) {
      const eventType = Object.keys(message.Event)[0];
      const eventData = message.Event[eventType];
      if (!eventData) return;
      if (ingestionPaused) {
        if (!resumeOnCluster) return;
        if (!BOOTSTRAP_EVENTS.has(eventType)) return;
      }
      if (!captureEnabled) return;

      eventQueue.push({
        eventType,
        eventData,
        created_at: normalizeTimestamp(eventData.created_at),
        generation,
      });
      return;
    }

    if (message.Message && typeof onMessage === 'function') {
      if (ingestionPaused || !captureEnabled) return;
      onMessage(message.Message);
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

  function invalidate() {
    generation += 1;
    eventQueue.clear();
  }

  function pauseIngestion(options = {}) {
    ingestionPaused = true;
    resumeOnCluster = Boolean(options.untilClusterInitialized);
  }

  function resumeIngestion() {
    ingestionPaused = false;
    resumeOnCluster = false;
  }

  function setCaptureEnabled(nextValue) {
    captureEnabled = Boolean(nextValue);
  }

  function setFrameWindowMicros(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric <= 0) return;
    eventQueue.batchWindow = numeric;
  }

  function getGeneration() {
    return generation;
  }

  return {
    connect,
    disconnect,
    invalidate,
    pauseIngestion,
    resumeIngestion,
    setCaptureEnabled,
    setFrameWindowMicros,
    eventQueue,
    getGeneration,
    get socket() {
      return ws;
    },
  };
}
