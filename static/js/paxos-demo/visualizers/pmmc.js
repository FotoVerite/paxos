import {
  drawBeamsTo,
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
      visualizer.activateNode(event.id, this.color);
      const snapshot = state.snapshot();
      const duration = getBeamDuration(snapshot, 260);
      let targets = getLeaderTargets(snapshot, event.id, canCommunicate);
      if (targets.length === 0) {
        targets = getReachableTargets(
          event.id,
          snapshot.cluster?.total_nodes,
          canCommunicate
        );
      }
      await drawBeamsTo(visualizer, event.id, targets, this.color, duration, 'solid');
      scheduleNodeReset(visualizer, event.id, duration + 50);
    }
  },

  PmmcP1A: {
    color: '#f59e0b',
    name: 'P1A',
    format(event, labels) {
      return `[P1A] ${nodeText(labels, event.from)} ballot ${ballotText(event.ballot)}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.activateNode(event.from, this.color);
      const snapshot = state.snapshot();
      const duration = getBeamDuration(snapshot, 420);
      const targets = getReachableTargets(
        event.from,
        snapshot.cluster?.total_nodes,
        canCommunicate
      );
      await drawBeamsTo(visualizer, event.from, targets, this.color, duration, 'solid');
      scheduleNodeReset(visualizer, event.from, duration + 50);
    }
  },

  PmmcP1B: {
    color: '#eab308',
    name: 'P1B',
    format(event, labels) {
      return `[P1B] ${nodeText(labels, event.from)} -> ${nodeText(labels, event.to)} ballot ${ballotText(event.ballot)}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.activateNode(event.from, this.color);
      const duration = getBeamDuration(state.snapshot(), 240);
      if (canCommunicate(event.from, event.to)) {
        await visualizer.drawBeam(event.from, event.to, this.color, duration, 'dashed');
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
      visualizer.activateNode(event.from, this.color);
      const snapshot = state.snapshot();
      const duration = getBeamDuration(snapshot, 420);
      const targets = getReachableTargets(
        event.from,
        snapshot.cluster?.total_nodes,
        canCommunicate
      );
      await drawBeamsTo(visualizer, event.from, targets, this.color, duration, 'solid');
      scheduleNodeReset(visualizer, event.from, duration + 50);
    }
  },

  PmmcP2B: {
    color: '#10b981',
    name: 'P2B',
    format(event, labels) {
      return `[P2B] ${nodeText(labels, event.from)} -> ${nodeText(labels, event.to)} slot ${event.pvalue.slot}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.activateNode(event.from, this.color);
      const duration = getBeamDuration(state.snapshot(), 350);
      if (canCommunicate(event.from, event.to)) {
        await visualizer.drawBeam(event.from, event.to, this.color, duration, 'dotted');
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
      const duration = getBeamDuration(state.snapshot(), 160);
      if (canCommunicate(event.from, event.to)) {
        await visualizer.drawBeam(event.from, event.to, this.color, duration, 'solid');
      }
      visualizer.setLeader(event.to);
      scheduleNodeReset(visualizer, event.to, duration + 50);
    }
  },

  PmmcPreempted: {
    color: '#ef4444',
    name: 'Preempted',
    format(event, labels) {
      return `[PREEMPT] ${nodeText(labels, event.from)} -> ${nodeText(labels, event.to)} ballot ${ballotText(event.ballot)}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      const duration = getBeamDuration(state.snapshot(), 160);
      if (canCommunicate(event.from, event.to)) {
        await visualizer.drawBeam(event.from, event.to, this.color, duration, 'dashed');
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
      const snapshot = state.snapshot();
      const duration = getBeamDuration(snapshot, 240);
      const targets = getReachableTargets(
        event.from,
        snapshot.cluster?.total_nodes,
        canCommunicate
      );
      await drawBeamsTo(visualizer, event.from, targets, this.color, duration, 'dashed');
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
      const duration = getBeamDuration(state.snapshot(), 260);
      if (canCommunicate(event.from, event.to)) {
        await visualizer.drawBeam(event.from, event.to, this.color, duration, 'dotted');
      }
      scheduleNodeReset(visualizer, event.from, duration + 50);
    }
  }
};
