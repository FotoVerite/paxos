import {
  listText,
  nodeText,
  scheduleNodeReset
} from '/js/paxos-demo/visualizers/shared.js';

export const SYSTEM_EVENT_VISUALIZERS = {
  PartitionCreated: {
    color: '#f87171',
    name: 'Partition',

    format(event, labels) {
      return `Network partitioned: A=${listText(labels, event.partition_a)}, B=${listText(labels, event.partition_b)}`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      for (const nodeId of event.partition_b) {
        visualizer.setNodePartitioned(nodeId, true);
      }
    }
  },

  PartitionHealed: {
    color: '#34d399',
    name: 'Healed',

    format(event) {
      return 'Network healed - all nodes connected';
    },

    async visualize(event, visualizer, state, canCommunicate) {
      const allNodesInPartitions = [...event.partition_a, ...event.partition_b];
      for (const nodeId of allNodesInPartitions) {
        visualizer.setNodePartitioned(nodeId, false);
      }
    }
  },

  LeaderElected: {
    color: '#fbbf24',
    name: 'Leader',

    format(event, labels) {
      return `[Leader] ${nodeText(labels, event.id)} elected as leader`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.setLeader(event.id);
    }
  },

  LeaderSteppedDown: {
    color: '#f97316',
    name: 'StepDown',

    format(event, labels) {
      return `[Leader] ${nodeText(labels, event.id)} stepped down`;
    },

    async visualize(event, visualizer) {
      if (typeof visualizer.clearLeader === 'function') {
        visualizer.clearLeader(event.id);
      }
      if (typeof visualizer.setNodeState === 'function') {
        visualizer.setNodeState(event.id, 'passive');
      }
      scheduleNodeReset(visualizer, event.id, 250);
    }
  },

  NodeCrashed: {
    color: '#ef4444',
    name: 'Crash',
    format(event, labels) {
      return `[CRASH] ${nodeText(labels, event.id)} crashed`;
    },
    async visualize(event, visualizer) {
      if (typeof visualizer.clearLeader === 'function') {
        visualizer.clearLeader(event.id);
      }
      if (typeof visualizer.setNodeState === 'function') {
        visualizer.setNodeState(event.id, 'crash');
      }
      if (typeof visualizer.setNodeCrashed === 'function') {
        visualizer.setNodeCrashed(event.id, true);
      }
    }
  }
};
