/**
 * Event Log Plugin
 * Renders event messages with color accents
 */

import { getEventVisualizer } from '/event-visualizers.js';

export function createEventLogPlugin({ eventLog, limit = 30 } = {}) {
  const items = [];
  let cursor = -1;

  function updateFutureClasses() {
    for (const item of items) {
      const isFuture = item.frameIndex > cursor;
      item.node.classList.toggle('future', isFuture);
    }
    scrollToActive();
  }

  function scrollToActive() {
    if (!eventLog) return;
    const activeItem = items.find((item) => item.frameIndex <= cursor);
    if (!activeItem) return;
    activeItem.node.scrollIntoView({ block: 'nearest' });
  }

  return {
    onBatchCaptured(batch, batchIndex) {
      if (!eventLog) return;
      for (const entry of batch) {
        const viz = getEventVisualizer(entry.eventType);
        if (!viz || !viz.format) continue;

        const node = document.createElement('div');
        node.className = 'event-item';
        node.style.borderLeftColor = viz.color;
        node.textContent = viz.format(entry.eventData);
        eventLog.insertBefore(node, eventLog.firstChild);

        items.unshift({ node, frameIndex: batchIndex });

        if (items.length > limit) {
          const removed = items.pop();
          if (removed?.node?.parentNode) {
            removed.node.parentNode.removeChild(removed.node);
          }
        }
      }
      updateFutureClasses();
    },

    onRestore({ targetIndex }) {
      if (!eventLog) return;
      cursor = targetIndex;
      updateFutureClasses();
    },

    onPlaybackUpdate(playbackState) {
      if (!eventLog) return;
      cursor = playbackState.cursor ?? cursor;
      updateFutureClasses();
    },

    onReset() {
      if (!eventLog) return;
      items.length = 0;
      cursor = -1;
      eventLog.innerHTML = '';
    },
  };
}
