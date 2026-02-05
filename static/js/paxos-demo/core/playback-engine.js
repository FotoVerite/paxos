/**
 * Playback Engine
 * Manages batches, cursor position, and stepping/replay behavior.
 */

import { cloneStateSnapshot, restoreSnapshotState } from '/js/paxos-demo/core/snapshot-utils.js';

export function createPlaybackEngine({
  state,
  visualizer,
  dispatchEvent,
  onFrameStart,
  onRestore,
  onPlaybackUpdate,
  resetPlaybackView,
  getGeneration,
  stepBackMode = 'quiet',
}) {
  let batches = [];
  let batchSnapshots = [];
  let cursor = -1;
  let playerMode = 'follow'; // follow | paused
  let totalCapturedEvents = 0;
  let applyChain = Promise.resolve();
  let applyingIndex = null;
  let stepInProgress = false;
  let baseSnapshot = null;

  function notifyPlayback() {
    if (typeof onPlaybackUpdate === 'function') {
      onPlaybackUpdate({
        batchCount: batches.length,
        cursor,
        playerMode,
        isBusy: stepInProgress || applyingIndex !== null,
        totalCapturedEvents,
      });
    }
  }

  function setPlayerMode(nextMode) {
    playerMode = nextMode;
    notifyPlayback();
  }

  function captureSnapshot(index) {
    batchSnapshots[index] = cloneStateSnapshot(state.snapshot());
  }

  function captureBaseSnapshot() {
    baseSnapshot = cloneStateSnapshot(state.snapshot());
  }

  function clearHistory() {
    batches = [];
    batchSnapshots = [];
    cursor = -1;
    totalCapturedEvents = 0;
    notifyPlayback();
  }

  async function applyBatch(index, options = {}) {
    if (index < 0 || index >= batches.length) {
      return;
    }

    if (!options.force && playerMode !== 'follow') {
      return;
    }

    const batch = batches[index];
    if (!batch || batch.length === 0) {
      return;
    }

    const batchGeneration = batch[0]?.generation;
    if (batchGeneration !== undefined && typeof getGeneration === 'function') {
      if (batchGeneration !== getGeneration()) {
        return;
      }
    }

    const playbackMode = options.playbackMode || (playerMode === 'follow' ? 'live' : 'paused');
    if (typeof onFrameStart === 'function') {
      onFrameStart(batch, index);
    }

    applyingIndex = index;
    try {
      if (playbackMode === 'step') {
        for (const event of batch) {
          await dispatchEvent(event, { playbackMode });
        }
      } else {
        const promises = batch.map((event) => dispatchEvent(event, { playbackMode }));
        await Promise.all(promises);
      }
    } finally {
      applyingIndex = null;
    }

    cursor = index;
    captureSnapshot(index);
    notifyPlayback();
  }

  function queueApplyBatch(index, options = {}) {
    applyChain = applyChain.then(() => applyBatch(index, options)).catch((err) => {
      console.error('[paxos-demo] apply batch error:', err);
    });
    return applyChain;
  }

  async function addBatch(batch, options = {}) {
    if (!batch || batch.length === 0) return;
    batches.push(batch);
    totalCapturedEvents += batch.length;
    notifyPlayback();

    if (typeof options.onBatchCaptured === 'function') {
      options.onBatchCaptured(batch, batches.length - 1);
    }

    if (playerMode === 'follow') {
      await queueApplyBatch(batches.length - 1);
    }
  }

  async function pause(options = {}) {
    setPlayerMode('paused');
    const waitForBatch = options.waitForBatch !== false && options.waitForFrame !== false;
    if (waitForBatch) {
      await applyChain;
    }
  }

  async function resume() {
    setPlayerMode('follow');

    for (let i = cursor + 1; i < batches.length; i++) {
      await queueApplyBatch(i);
    }
  }

  async function replayTo(targetIndex) {
    await pause({ waitForBatch: true });
    resetPlaybackView({ notifyReset: false });

    if (!baseSnapshot) {
      cursor = -1;
      notifyPlayback();
      return;
    }

    if (targetIndex < 0) {
      restoreSnapshotState(baseSnapshot, state, visualizer);
      cursor = -1;
      notifyPlayback();
      return;
    }

    restoreSnapshotState(baseSnapshot, state, visualizer);

    for (let i = 0; i <= targetIndex && i < batches.length; i++) {
      await applyBatch(i, { force: true, playbackMode: 'step' });
    }

    cursor = Math.min(targetIndex, batches.length - 1);
    notifyPlayback();
  }

  async function stepForward() {
    if (stepInProgress) return;
    stepInProgress = true;
    notifyPlayback();
    try {
      await pause({ waitForBatch: true });
      const baseIndex = applyingIndex !== null ? applyingIndex : cursor;
      if (baseIndex + 1 >= batches.length) return;
      const targetIndex = baseIndex + 1;
      const prevIndex = targetIndex - 1;

      if (prevIndex >= 0 && batchSnapshots[prevIndex]) {
        restoreSnapshotState(batchSnapshots[prevIndex], state, visualizer);
        if (typeof onRestore === 'function') {
          onRestore({ targetIndex: prevIndex });
        }
      } else {
        resetPlaybackView({ notifyReset: false });
        if (baseSnapshot) {
          restoreSnapshotState(baseSnapshot, state, visualizer);
        }
        if (typeof onRestore === 'function') {
          onRestore({ targetIndex: -1 });
        }
      }

      await applyBatch(targetIndex, { force: true, playbackMode: 'step' });
    } finally {
      stepInProgress = false;
      notifyPlayback();
    }
  }

  async function stepBack() {
    if (stepInProgress) return;
    stepInProgress = true;
    notifyPlayback();
    try {
      await pause({ waitForBatch: true });
      const baseIndex = applyingIndex !== null ? applyingIndex : cursor;
      const targetIndex = baseIndex - 1;
      const playbackMode = stepBackMode === 'full' ? 'step' : 'step-back';

      if (targetIndex < 0) {
        resetPlaybackView({ notifyReset: false });
        if (baseSnapshot) {
          restoreSnapshotState(baseSnapshot, state, visualizer);
        }
        cursor = -1;
        notifyPlayback();
        if (typeof onRestore === 'function') {
          onRestore({ targetIndex: -1 });
        }
        return;
      }

      const prevIndex = targetIndex - 1;
      if (prevIndex >= 0 && batchSnapshots[prevIndex]) {
        restoreSnapshotState(batchSnapshots[prevIndex], state, visualizer);
        if (typeof onRestore === 'function') {
          onRestore({ targetIndex: prevIndex });
        }
      } else {
        resetPlaybackView({ notifyReset: false });
        if (baseSnapshot) {
          restoreSnapshotState(baseSnapshot, state, visualizer);
        }
        if (typeof onRestore === 'function') {
          onRestore({ targetIndex: -1 });
        }
      }

      await applyBatch(targetIndex, { force: true, playbackMode });
    } finally {
      stepInProgress = false;
      notifyPlayback();
    }
  }

  function getPlaybackState() {
    return {
      batchCount: batches.length,
      cursor,
      playerMode,
      isBusy: stepInProgress || applyingIndex !== null,
      totalCapturedEvents,
    };
  }

  function getCapturedEventCount() {
    return totalCapturedEvents;
  }

  function hasPendingBatches() {
    return cursor < batches.length - 1;
  }

  return {
    addBatch,
    pause,
    resume,
    replayTo,
    stepForward,
    stepBack,
    clearHistory,
    captureBaseSnapshot,
    getPlaybackState,
    getCapturedEventCount,
    hasPendingBatches,
    setPlayerMode,
  };
}
