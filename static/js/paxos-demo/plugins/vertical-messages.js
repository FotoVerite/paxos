import {
  drawBeamsTo,
  flashNodeWithMotion,
  getBeamDuration,
  scheduleNodeReset
} from '/js/paxos-demo/visualizers/shared.js';

const MESSAGE_COLORS = {
  PROPOSE: '#60a5fa',
  P1A: '#f59e0b',
  P1B: '#eab308',
  P2A: '#f87171',
  P2B: '#22c55e',
  ACCEPTED: '#14b8a6',
  ACK: '#06b6d4',
  PREEMPT: '#ef4444'
};

function unpack(payload) {
  const messageContainer = payload?.message;
  if (!messageContainer || typeof messageContainer !== 'object') return [null, null];
  const [type] = Object.keys(messageContainer);
  return [type || null, type ? messageContainer[type] : null];
}

async function drawDirectBeam(visualizer, from, to, color, duration, motion, canCommunicate) {
  if (
    from === undefined ||
    to === undefined ||
    from === null ||
    to === null ||
    typeof visualizer?.drawBeam !== 'function' ||
    (typeof canCommunicate === 'function' && !canCommunicate(from, to))
  ) {
    return;
  }
  await visualizer.drawBeam(from, to, color, duration, 'solid', {
    motion,
    curveOffset: 20,
  });
}

export function createVerticalMessagePlugin() {
  return {
    async onMessage(payload, ctx) {
      const [type, message] = unpack(payload);
      if (!type || !message) return;

      const color = MESSAGE_COLORS[type];
      if (!color) return;

      const duration = getBeamDuration(ctx.state.snapshot(), 160);
      const indexes = Array.isArray(payload?.indexes) ? payload.indexes : [];

      switch (type) {
        case 'PROPOSE':
          flashNodeWithMotion(ctx.visualizer, message.from, color, {
            motion: 'proposal',
            resetDelay: duration + 60,
          });
          await drawBeamsTo(
            ctx.visualizer,
            message.from,
            indexes,
            color,
            duration,
            'solid',
            { motion: 'proposal', curveOffset: 30 }
          );
          scheduleNodeReset(ctx.visualizer, message.from, duration + 60);
          break;
        case 'P1A':
        case 'P2A':
        case 'ACCEPTED':
          flashNodeWithMotion(ctx.visualizer, message.from, color, {
            motion: 'proposal',
            resetDelay: duration + 60,
          });
          await drawBeamsTo(
            ctx.visualizer,
            message.from,
            indexes,
            color,
            duration,
            'solid',
            { motion: 'proposal', curveOffset: 42, staggerMs: 16 }
          );
          scheduleNodeReset(ctx.visualizer, message.from, duration + 60);
          break;
        case 'P1B':
        case 'P2B':
        case 'ACK':
        case 'PREEMPT':
          flashNodeWithMotion(ctx.visualizer, message.from, color, {
            motion: 'reply',
            resetDelay: duration + 40,
          });
          await drawDirectBeam(
            ctx.visualizer,
            message.from,
            message.to,
            color,
            duration,
            type === 'PREEMPT' ? 'failure' : 'reply',
            ctx.canCommunicate
          );
          scheduleNodeReset(ctx.visualizer, message.from, duration + 40);
          break;
        default:
          break;
      }
    },
  };
}
