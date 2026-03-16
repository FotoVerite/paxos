import {
  drawBeamsTo,
  flashNodeWithMotion,
  getBeamDuration,
  getLeaderTargets,
  getReachableTargets,
  nodeText,
  scheduleNodeReset
} from '/js/paxos-demo/visualizers/shared.js';

function ballotText(ballot) {
  if (!ballot || typeof ballot !== 'object') return 'unknown';
  const epoch = ballot.epoch ?? '?';
  const number = ballot.number ?? '?';
  return `e${epoch}:b${number}`;
}

export const PMMC_EVENT_VISUALIZERS = {
  PmmcPropose: {
    color: '#60a5fa',
    name: 'PMMC Propose',
    format(event, labels) {
      return `[PMMC Propose] ${nodeText(labels, event.id)} slot ${event.slot}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      flashNodeWithMotion(visualizer, event.id, this.color, {
        motion: 'proposal',
        resetDelay: 360,
      });
      const snapshot = state.snapshot();
      const duration = getBeamDuration(snapshot, 180);
      let targets = getLeaderTargets(snapshot, event.id, canCommunicate);
      if (targets.length === 0) {
        targets = getReachableTargets(
          event.id,
          snapshot.cluster?.total_nodes,
          canCommunicate
        );
      }
      await drawBeamsTo(visualizer, event.id, targets, this.color, duration, 'solid', {
        motion: 'proposal',
        staggerMs: 18,
        curveOffset: 54,
      });
      scheduleNodeReset(visualizer, event.id, duration + 70);
    }
  },

  PmmcP1A: {
    color: '#f59e0b',
    name: 'P1A',
    format(event, labels) {
      return `[P1A] ${nodeText(labels, event.from)} ballot ${ballotText(event.ballot)}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      flashNodeWithMotion(visualizer, event.from, this.color, {
        motion: 'proposal',
        resetDelay: 420,
      });
      const snapshot = state.snapshot();
      const duration = getBeamDuration(snapshot, 260);
      const targets = getReachableTargets(
        event.from,
        snapshot.cluster?.total_nodes,
        canCommunicate
      );
      await drawBeamsTo(visualizer, event.from, targets, this.color, duration, 'solid', {
        motion: 'proposal',
        staggerMs: 24,
        curveOffset: 68,
      });
      scheduleNodeReset(visualizer, event.from, duration + 90);
    }
  },

  PmmcP1B: {
    color: '#eab308',
    name: 'P1B',
    format(event, labels) {
      return `[P1B] ${nodeText(labels, event.from)} -> ${nodeText(labels, event.to)} ballot ${ballotText(event.ballot)}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      flashNodeWithMotion(visualizer, event.from, this.color, {
        motion: 'reply',
        resetDelay: 260,
      });
      const duration = getBeamDuration(state.snapshot(), 150);
      if (canCommunicate(event.from, event.to)) {
        await visualizer.drawBeam(event.from, event.to, this.color, duration, 'dashed', {
          motion: 'reply',
          curveOffset: 24,
        });
      }
      scheduleNodeReset(visualizer, event.from, duration + 50);
    }
  },

  PmmcP2A: {
    color: '#f87171',
    name: 'P2A',
    format(event, labels) {
      return `[P2A] ${nodeText(labels, event.from)} slot ${event.pvalue.slot}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      flashNodeWithMotion(visualizer, event.from, this.color, {
        motion: 'proposal',
        resetDelay: 440,
      });
      const snapshot = state.snapshot();
      const duration = getBeamDuration(snapshot, 240);
      const targets = getReachableTargets(
        event.from,
        snapshot.cluster?.total_nodes,
        canCommunicate
      );
      await drawBeamsTo(visualizer, event.from, targets, this.color, duration, 'solid', {
        motion: 'proposal',
        staggerMs: 20,
        curveOffset: 48,
      });
      scheduleNodeReset(visualizer, event.from, duration + 80);
    }
  },

  PmmcP2B: {
    color: '#10b981',
    name: 'P2B',
    format(event, labels) {
      return `[P2B] ${nodeText(labels, event.from)} -> ${nodeText(labels, event.to)} slot ${event.pvalue.slot}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      flashNodeWithMotion(visualizer, event.from, this.color, {
        motion: 'reply',
        resetDelay: 280,
      });
      const duration = getBeamDuration(state.snapshot(), 170);
      if (canCommunicate(event.from, event.to)) {
        await visualizer.drawBeam(event.from, event.to, this.color, duration, 'dotted', {
          motion: 'reply',
          curveOffset: 18,
        });
      }
      scheduleNodeReset(visualizer, event.from, duration + 50);
    }
  },

  PmmcAdopted: {
    color: '#22c55e',
    name: 'Adopted',
    format(event, labels) {
      return `[ADOPTED] ${nodeText(labels, event.from)} -> ${nodeText(labels, event.to)} ballot ${ballotText(event.ballot)}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      flashNodeWithMotion(visualizer, event.to, this.color, {
        motion: 'success',
        resetDelay: 620,
      });
      const duration = getBeamDuration(state.snapshot(), 140);
      if (canCommunicate(event.from, event.to)) {
        await visualizer.drawBeam(event.from, event.to, this.color, duration, 'solid', {
          motion: 'success',
          curveOffset: 34,
        });
      }
      visualizer.setLeader(event.to);
      scheduleNodeReset(visualizer, event.to, duration + 260);
    }
  },

  PmmcPreempted: {
    color: '#ef4444',
    name: 'Preempted',
    format(event, labels) {
      return `[PREEMPT] ${nodeText(labels, event.from)} -> ${nodeText(labels, event.to)} ballot ${ballotText(event.ballot)}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      if (typeof visualizer.pulseNode === 'function') {
        visualizer.pulseNode(event.to, this.color, {
          resetDelay: 260,
          glowRadius: 10,
          glowOpacity: 0.7,
        });
      }
      const duration = getBeamDuration(state.snapshot(), 110);
      if (canCommunicate(event.from, event.to)) {
        await visualizer.drawBeam(event.from, event.to, this.color, duration, 'dashed', {
          motion: 'failure',
          curveOffset: 12,
        });
      }
      scheduleNodeReset(visualizer, event.to, duration + 50);
    }
  },

  PmmcHeartbeat: {
    color: '#a78bfa',
    name: 'Heartbeat',
    format(event, labels) {
      return `[HEARTBEAT] ${nodeText(labels, event.from)} ballot ${ballotText(event.ballot)}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      flashNodeWithMotion(visualizer, event.from, this.color, {
        motion: 'reply',
        resetDelay: 220,
      });
      const snapshot = state.snapshot();
      const duration = getBeamDuration(snapshot, 150);
      const targets = getReachableTargets(
        event.from,
        snapshot.cluster?.total_nodes,
        canCommunicate
      );
      await drawBeamsTo(visualizer, event.from, targets, this.color, duration, 'dashed', {
        motion: 'reply',
        staggerMs: 12,
        curveOffset: 16,
      });
      scheduleNodeReset(visualizer, event.from, duration + 50);
    }
  },

  PmmcAck: {
    color: '#06b6d4',
    name: 'Ack',
    format(event, labels) {
      return `[ACK] ${nodeText(labels, event.from)} -> ${nodeText(labels, event.to)} slot ${event.slot}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      flashNodeWithMotion(visualizer, event.from, this.color, {
        motion: 'reply',
        resetDelay: 240,
      });
      const duration = getBeamDuration(state.snapshot(), 140);
      if (canCommunicate(event.from, event.to)) {
        await visualizer.drawBeam(event.from, event.to, this.color, duration, 'dotted', {
          motion: 'reply',
          curveOffset: 14,
        });
      }
      scheduleNodeReset(visualizer, event.from, duration + 50);
    }
  }
};
