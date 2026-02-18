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
  PmmcPropose: 'propose',
  PmmcP1A: 'p1a',
  PmmcP1B: 'p1b',
  PmmcP2A: 'p2a',
  PmmcP2B: 'vote',
  PmmcAdopted: 'adopted',
  PmmcPreempted: 'preempt',
  PmmcHeartbeat: 'heart',
  PmmcAck: 'ack',
  LeaderSteppedDown: 'passive',
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
    clearLeader: noop,
  };
}

export function createVisualizeEventsPlugin({ skip = new Set() } = {}) {
  return {
    async onEvent({ eventType, eventData }, ctx) {
      if (skip.has(eventType)) return;
      const viz = getEventVisualizer(eventType);
      if (!viz || !viz.visualize) return;
      const stateLabel = NODE_STATE_LABELS[eventType];
      if (stateLabel) {
        const stateNodeId =
          eventData?.id ??
          eventData?.from ??
          eventData?.to;
        if (stateNodeId !== undefined && stateNodeId !== null) {
          ctx.state.setNodeState(stateNodeId, stateLabel);
          if (ctx.visualizer && typeof ctx.visualizer.setNodeState === 'function') {
            ctx.visualizer.setNodeState(stateNodeId, stateLabel);
          }
        }
      }
      if (eventType === 'LeaderElected' && eventData?.id !== undefined) {
        ctx.state.setLeader(eventData.id);
      }
      if (eventType === 'LeaderSteppedDown' && eventData?.id !== undefined) {
        const snapshot = ctx.state.snapshot();
        if (snapshot.leaderId === eventData.id) {
          ctx.state.setLeader(null);
        }
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
