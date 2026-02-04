/**
 * Visualize Events Plugin
 * Delegates rendering to the shared event visualizers
 */

import { getEventVisualizer } from '/event-visualizers.js';

export function createVisualizeEventsPlugin({ skip = new Set() } = {}) {
  return {
    async onEvent({ eventType, eventData }, ctx) {
      if (skip.has(eventType)) return;
      const viz = getEventVisualizer(eventType);
      if (!viz || !viz.visualize) return;
      await viz.visualize(eventData, ctx.visualizer, ctx.state, ctx.canCommunicate);
    },
  };
}
