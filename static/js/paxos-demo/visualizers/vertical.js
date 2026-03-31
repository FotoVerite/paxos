import {
  flashNodeWithMotion,
  getBeamDuration,
  listText,
  nodeText,
  scheduleNodeReset
} from '/js/paxos-demo/visualizers/shared.js';

function ballotText(ballot) {
  if (!ballot || typeof ballot !== 'object') return 'unknown';
  const epoch = ballot.epoch ?? '?';
  const number = ballot.number ?? '?';
  return `e${epoch}:b${number}`;
}

export const VERTICAL_EVENT_VISUALIZERS = {
  VerticalConfigurationInstalled: {
    color: '#0f766e',
    name: 'ConfigInstalled',
    format(event, labels) {
      return `[CONFIG] installed ${event.configuration_id} leader=${nodeText(labels, event.leader)} replicas=${listText(labels, event.replicas)} acceptors=${listText(labels, event.acceptors)}`;
    },
    async visualize(event, visualizer) {
      if (typeof visualizer.setNodeState !== 'function') return;
      for (const replicaId of event.replicas || []) {
        visualizer.setNodeState(replicaId, 'install');
      }
      for (const acceptorId of event.acceptors || []) {
        visualizer.setNodeState(acceptorId, 'accept');
      }
      scheduleNodeReset(visualizer, event.leader, 260);
    }
  },

  VerticalActivationStarted: {
    color: '#f59e0b',
    name: 'ActivationStart',
    format(event, labels) {
      return `[ACTIVATE] ${nodeText(labels, event.leader)} started sync for ${event.configuration_id} @${ballotText(event.ballot)}`;
    },
    async visualize(event, visualizer) {
      flashNodeWithMotion(visualizer, event.leader, this.color, {
        motion: 'proposal',
        resetDelay: 360,
      });
      if (typeof visualizer.setNodeState === 'function') {
        visualizer.setNodeState(event.leader, 'sync');
      }
      scheduleNodeReset(visualizer, event.leader, 360);
    }
  },

  VerticalActivationReady: {
    color: '#22c55e',
    name: 'ActivationReady',
    format(event, labels) {
      return `[READY] ${nodeText(labels, event.leader)} activated ${event.configuration_id} @${ballotText(event.ballot)}`;
    },
    async visualize(event, visualizer) {
      if (typeof visualizer.setLeader === 'function') {
        visualizer.setLeader(event.leader);
      }
      flashNodeWithMotion(visualizer, event.leader, this.color, {
        motion: 'success',
        resetDelay: 420,
      });
      if (typeof visualizer.setNodeState === 'function') {
        visualizer.setNodeState(event.leader, 'active');
      }
      scheduleNodeReset(visualizer, event.leader, 420);
    }
  },

  VerticalActivationSuperseded: {
    color: '#ef4444',
    name: 'ActivationSuperseded',
    format(event, labels) {
      return `[SUPERSEDED] ${nodeText(labels, event.leader)} lost ${event.configuration_id} to ${ballotText(event.superseded_by)}`;
    },
    async visualize(event, visualizer) {
      flashNodeWithMotion(visualizer, event.leader, this.color, {
        motion: 'failure',
        resetDelay: 320,
      });
      if (typeof visualizer.setNodeState === 'function') {
        visualizer.setNodeState(event.leader, 'preempt');
      }
      scheduleNodeReset(visualizer, event.leader, 320);
    }
  },

  VerticalReplicaSetActivated: {
    color: '#14b8a6',
    name: 'ReplicaSetActive',
    format(event, labels) {
      return `[REPLICAS] active for ${event.configuration_id}: ${listText(labels, event.replicas)}`;
    },
    async visualize(event, visualizer) {
      if (typeof visualizer.setNodeState !== 'function') return;
      for (const replicaId of event.replicas || []) {
        visualizer.setNodeState(replicaId, 'active');
        scheduleNodeReset(visualizer, replicaId, 300);
      }
    }
  },

  VerticalReplicaSetDecommissioned: {
    color: '#b45309',
    name: 'ReplicaSetRedirect',
    format(event, labels) {
      return `[REPLICAS] redirecting old set ${event.configuration_id}: ${listText(labels, event.replicas)}`;
    },
    async visualize(event, visualizer) {
      if (typeof visualizer.setNodeState !== 'function') return;
      for (const replicaId of event.replicas || []) {
        visualizer.setNodeState(replicaId, 'redirect');
      }
    }
  },

  VerticalReplicaRedirected: {
    color: '#f97316',
    name: 'ReplicaRedirect',
    format(event, labels) {
      return `[REDIRECT] ${nodeText(labels, event.from_replica)} -> active ${event.active_configuration_id}, leader=${nodeText(labels, event.leader)}, replicas=${listText(labels, event.replicas)}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      flashNodeWithMotion(visualizer, event.from_replica, this.color, {
        motion: 'failure',
        resetDelay: 320,
      });
      const duration = getBeamDuration(state.snapshot(), 180);
      if (
        typeof visualizer.drawBeam === 'function' &&
        typeof canCommunicate === 'function' &&
        canCommunicate(event.from_replica, event.leader)
      ) {
        await visualizer.drawBeam(
          event.from_replica,
          event.leader,
          this.color,
          duration,
          'dashed',
          { motion: 'failure', curveOffset: 28 }
        );
      }
      if (typeof visualizer.setNodeState === 'function') {
        visualizer.setNodeState(event.from_replica, 'redirect');
      }
      scheduleNodeReset(visualizer, event.from_replica, duration + 80);
    }
  },

  VerticalReplicaApplied: {
    color: '#38bdf8',
    name: 'ReplicaApplied',
    format(event, labels) {
      return `[APPLY] ${nodeText(labels, event.id)} applied slot ${event.slot}`;
    },
    async visualize(event, visualizer) {
      flashNodeWithMotion(visualizer, event.id, this.color, {
        motion: 'success',
        resetDelay: 240,
      });
      if (typeof visualizer.setNodeState === 'function') {
        visualizer.setNodeState(event.id, 'apply');
      }
      scheduleNodeReset(visualizer, event.id, 280);
    }
  }
};
