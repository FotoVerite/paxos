/**
 * Event Log Plugin
 * Renders event messages with color accents
 */

import { getEventVisualizer } from '/event-visualizers.js';

export function createEventLogPlugin({ eventLog, limit = 30 } = {}) {
  return {
    onEvent({ eventType, eventData }) {
      if (!eventLog) return;
      const viz = getEventVisualizer(eventType);
      if (!viz || !viz.format) return;

      const item = document.createElement('div');
      item.className = 'event-item';
      item.style.borderLeftColor = viz.color;
      item.textContent = viz.format(eventData);
      eventLog.insertBefore(item, eventLog.firstChild);

      while (eventLog.children.length > limit) {
        eventLog.removeChild(eventLog.lastChild);
      }
    },

    onReset() {
      if (eventLog) {
        eventLog.innerHTML = '';
      }
    },
  };
}
