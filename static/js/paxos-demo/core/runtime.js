/**
 * Paxos Demo Runtime
 * Orchestrates reader, playback, and plugins.
 */

import { createEventReader } from '/js/paxos-demo/core/event-reader.js';
import { createPlaybackEngine } from '/js/paxos-demo/core/playback-engine.js';

export function createPaxosDemoController({
  state,
  visualizer,
  plugins = [],
  canCommunicate = () => true,
  websocketPath = '/ws',
  frameWindowMicros = 50,
  onPlaybackUpdate = null,
  stepBackMode = 'quiet',
}) {
  const context = {
    state,
    visualizer,
    canCommunicate,
    playbackMode: 'live',
  };

  let clusterInfo = null;

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

  function notifyFrameStart(batch, index) {
    for (const plugin of plugins) {
      if (plugin && typeof plugin.onFrameStart === 'function') {
        safeCall('plugin.onFrameStart', plugin.onFrameStart, batch, index, context);
      }
    }
  }

  function notifyRestore({ targetIndex }) {
    for (const plugin of plugins) {
      if (plugin && typeof plugin.onRestore === 'function') {
        safeCall('plugin.onRestore', plugin.onRestore, { targetIndex }, context);
      }
    }
  }

  function notifyPlayback(playbackState) {
    if (playbackState?.playerMode === 'follow') {
      context.playbackMode = 'live';
    } else if (playbackState?.playerMode === 'paused') {
      context.playbackMode = 'paused';
    }

    if (typeof onPlaybackUpdate === 'function') {
      onPlaybackUpdate(playbackState);
    }

    for (const plugin of plugins) {
      if (plugin && typeof plugin.onPlaybackUpdate === 'function') {
        safeCall('plugin.onPlaybackUpdate', plugin.onPlaybackUpdate, playbackState, context);
      }
    }
  }

  async function dispatchEvent(queuedEvent, options = {}) {
    if (!queuedEvent) return;
    if (queuedEvent.generation !== undefined && queuedEvent.generation !== reader.getGeneration()) {
      return;
    }

    const previousMode = context.playbackMode;
    if (options.playbackMode) {
      context.playbackMode = options.playbackMode;
    }

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

    if (options.playbackMode) {
      context.playbackMode = previousMode;
    }
  }

  function handleCluster(cluster) {
    clusterInfo = cluster;
    playback.clearHistory();
    state.initialize(cluster);

    for (const plugin of plugins) {
      if (plugin && typeof plugin.onCluster === 'function') {
        safeCall('plugin.onCluster', plugin.onCluster, cluster, context);
      }
    }

    playback.captureBaseSnapshot();
  }

  function resetPlaybackView(options = {}) {
    if (visualizer && typeof visualizer.clearScheduledResets === 'function') {
      visualizer.clearScheduledResets();
    }
    if (visualizer && typeof visualizer.clearBeams === 'function') {
      visualizer.clearBeams();
    }
    state.resetSimulation();
    if (options.notifyReset !== false) {
      for (const plugin of plugins) {
        if (plugin && typeof plugin.onReset === 'function') {
          safeCall('plugin.onReset', plugin.onReset, context);
        }
      }
    }

    if (clusterInfo) {
      for (const plugin of plugins) {
        if (plugin && typeof plugin.onCluster === 'function') {
          safeCall('plugin.onCluster', plugin.onCluster, clusterInfo, context);
        }
      }
    }
  }

  function resetView() {
    resetPlaybackView();
  }

  const playback = createPlaybackEngine({
    state,
    visualizer,
    dispatchEvent,
    onFrameStart: notifyFrameStart,
    onRestore: notifyRestore,
    onPlaybackUpdate: notifyPlayback,
    resetPlaybackView,
    getGeneration: () => reader.getGeneration(),
    stepBackMode,
  });

  const reader = createEventReader({
    websocketPath,
    frameWindowMicros,
    onCluster: handleCluster,
    onBatch: async (batch) => {
      await playback.addBatch(batch, {
        onBatchCaptured: (capturedBatch, index) => {
          for (const plugin of plugins) {
            if (plugin && typeof plugin.onBatchCaptured === 'function') {
              safeCall('plugin.onBatchCaptured', plugin.onBatchCaptured, capturedBatch, index, context);
            }
          }
        },
      });
    },
  });

  function invalidate(options = {}) {
    reader.invalidate();
    if (options.clearHistory) {
      playback.clearHistory();
    }
  }

  function reset(options = {}) {
    if (options.invalidate !== false) {
      invalidate({ clearHistory: options.clearHistory });
    } else if (options.clearHistory) {
      playback.clearHistory();
    }
    resetView();
  }

  initPlugins();

  return {
    connect: reader.connect,
    disconnect: reader.disconnect,
    reset,
    invalidate,
    pauseIngestion: reader.pauseIngestion,
    resumeIngestion: reader.resumeIngestion,
    setCaptureEnabled: reader.setCaptureEnabled,
    pause: playback.pause,
    resume: playback.resume,
    stepForward: playback.stepForward,
    stepBack: playback.stepBack,
    replayTo: playback.replayTo,
    getPlaybackState: playback.getPlaybackState,
    getCapturedEventCount: playback.getCapturedEventCount,
    hasPendingBatches: playback.hasPendingBatches,
    hasPendingFrames: playback.hasPendingBatches,
    setFrameWindowMicros: reader.setFrameWindowMicros,
    eventQueue: reader.eventQueue,
    get socket() {
      return reader.socket;
    },
  };
}
