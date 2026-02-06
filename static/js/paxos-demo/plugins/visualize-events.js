/**
 * Visualize Events Plugin
 * Delegates rendering to the shared event visualizers
 */

import { getEventVisualizer } from '/event-visualizers.js';

const NODE_STATE_LABELS = {
  Proposal: 'propose',
  Promise: 'promise',
  Accept: 'accept',
  Accepted: 'voted',
  Learn: 'learn',
  LearnedValue: 'learn',
  Success: 'success',
};

function createQuietVisualizer(visualizer) {
  const noop = () => {};
  const noopAsync = () => Promise.resolve();
  return {
    setNodeState: (nodeId, state) => visualizer.setNodeState(nodeId, state),
    activateNode: noop,
    resetNodeToRoleColor: noop,
    scheduleNodeReset: noop,
    drawBeam: noopAsync,
    drawBeamsTo: noopAsync,
    drawBeamsFrom: noopAsync,
    setNodePartitioned: noop,
    setLeader: noop,
  };
}

export function createVisualizeEventsPlugin({ skip = new Set() } = {}) {
  return {
    async onEvent({ eventType, eventData }, ctx) {
      if (skip.has(eventType)) return;
      const viz = getEventVisualizer(eventType);
      if (!viz || !viz.visualize) return;
      const stateLabel = NODE_STATE_LABELS[eventType];
      if (stateLabel && eventData?.id !== undefined) {
        ctx.state.setNodeState(eventData.id, stateLabel);
      }
      if (eventType === 'LeaderElected' && eventData?.id !== undefined) {
        ctx.state.setLeader(eventData.id);
      }

      const playbackMode = ctx.playbackMode;
      const visualizer =
        playbackMode === 'step-back'
          ? createQuietVisualizer(ctx.visualizer)
          : ctx.visualizer;

      await viz.visualize(eventData, visualizer, ctx.state, ctx.canCommunicate);
    },
  };
}
