/**
 * Event Visualizers Registry
 * Declarative mapping of Paxos events to visualization behavior
 * Replaces imperative visualizeProposal(), visualizePromise(), etc. functions
 */

/**
 * Helper: format decree from event value
 */
function formatDecree(event) {
  if (event.value === 'NOOP') return 'NOOP';
  if (event.value && typeof event.value === 'object' && event.value.EnactDecree) {
    return event.value.EnactDecree.law;
  }
  return `Decree #${event.decree_num}`;
}

/**
 * Core event visualizers
 * Each visualizer defines:
 * - color: hex color for this event type
 * - name: display name (e.g., "NextBallot")
 * - format(event): format event for log display
 * - visualize(event, visualizer, state, canCommunicate): async visualization logic
 */
export const EVENT_VISUALIZERS = {
  Proposal: {
    color: '#60a5fa', // Blue
    name: 'NextBallot',

    format(event) {
      return `[NextBallot] Node ${event.id}: "${formatDecree(event)}"`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.setNodeState(event.id, 'propose');
      visualizer.activateNode(event.id, this.color);

      const snapshot = state.snapshot();
      const speed = snapshot.simulation.speed;
      const duration = Math.max(200, (500 / speed) * 0.67);

      // Draw beams to all reachable nodes
      const beams = [];
      for (let i = 0; i < snapshot.cluster.total_nodes; i++) {
        if (i !== event.id && canCommunicate(event.id, i)) {
          beams.push(visualizer.drawBeam(event.id, i, this.color, duration, 'solid'));
        }
      }
      await Promise.all(beams);
    }
  },

  Promise: {
    color: '#ec4899', // Pink
    name: 'LastVote',

    format(event) {
      return `[LastVote] Node ${event.id} → Node ${event.from}: Ballot ${event.ballot}`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.setNodeState(event.id, 'promise');
      visualizer.activateNode(event.id, this.color);

      if (event.from !== undefined && event.from !== event.id) {
        const snapshot = state.snapshot();
        const speed = snapshot.simulation.speed;
        const duration = Math.max(200, (500 / speed) * 0.67);
        await visualizer.drawBeam(event.id, event.from, this.color, duration, 'dashed');
      }
    }
  },

  Accept: {
    color: '#f87171', // Red
    name: 'BeginBallot',

    format(event) {
      return `[BeginBallot] Node ${event.id}: Ballot ${event.ballot}, Decree #${event.decree_num}`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.setNodeState(event.id, 'accept');
      visualizer.activateNode(event.id, this.color);

      const snapshot = state.snapshot();
      const speed = snapshot.simulation.speed;
      const duration = Math.max(200, (500 / speed) * 0.67);

      // Draw beams to quorum nodes
      const beams = [];
      if (event.quorum && Array.isArray(event.quorum)) {
        for (const nodeId of event.quorum) {
          if (nodeId !== event.id && canCommunicate(event.id, nodeId)) {
            beams.push(visualizer.drawBeam(event.id, nodeId, this.color, duration, 'solid'));
          }
        }
      }
      await Promise.all(beams);
    }
  },

  Accepted: {
    color: '#10b981', // Green
    name: 'Voted',

    format(event) {
      return `[Voted] Node ${event.id} → Node ${event.from}: Ballot ${event.ballot}`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.setNodeState(event.id, 'voted');
      visualizer.activateNode(event.id, this.color);

      if (event.from !== undefined && event.from !== event.id) {
        const snapshot = state.snapshot();
        const speed = snapshot.simulation.speed;
        const duration = Math.max(200, (500 / speed) * 0.67);
        await visualizer.drawBeam(event.id, event.from, this.color, duration, 'dashed');
      }
    }
  },

  Learn: {
    color: '#34d399', // Emerald
    name: 'Learn',

    format(event) {
      const decree = formatDecree(event);
      return `[Learn] Node ${event.id}: "${decree}"`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.setNodeState(event.id, 'learn');
      visualizer.activateNode(event.id, this.color);
    }
  },

  LearnedValue: {
     color: '#34d399', // Emerald
     name: 'LearnedValue',

     format(event) {
       const decree = formatDecree(event);
       return `[LearnedValue] Node ${event.id}: "${decree}"`;
     },

     async visualize(event, visualizer, state, canCommunicate) {
       visualizer.setNodeState(event.id, 'learn');
       visualizer.activateNode(event.id, this.color);
     }
   },

  InitialDecree: {
    color: '#8b5cf6', // Purple
    name: 'InitialDecree',

    format(event) {
      const decree = formatDecree(event);
      return `[InitialDecree] Node ${event.id}: [${event.decree_num}] "${decree}"`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      // No visualization for initial decrees - they're just state
    }
  },

  Success: {
    color: '#6366f1', // Indigo
    name: 'Success',

    format(event) {
      const proposerInfo = event.ballot_proposer !== undefined ? ` (proposed by node ${event.ballot_proposer})` : '';
      return `[Success] Node ${event.id}: Decree #${event.decree_num} chosen${proposerInfo}`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.setNodeState(event.id, 'success');
      visualizer.activateNode(event.id, this.color);
    }
  },

  // Partition events
  PartitionCreated: {
    color: '#f87171', // Red
    name: 'Partition',

    format(event) {
      return `Network partitioned: A=${JSON.stringify(event.partition_a)}, B=${JSON.stringify(event.partition_b)}`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      // Mark partitioned nodes
      for (const nodeId of event.partition_b) {
        visualizer.setNodePartitioned(nodeId, true);
      }
    }
  },

  PartitionHealed: {
    color: '#34d399', // Emerald
    name: 'Healed',

    format(event) {
      return 'Network healed - all nodes connected';
    },

    async visualize(event, visualizer, state, canCommunicate) {
      // Unmark all partitioned nodes
      const allNodesInPartitions = [...event.partition_a, ...event.partition_b];
      for (const nodeId of allNodesInPartitions) {
        visualizer.setNodePartitioned(nodeId, false);
      }
    }
  },
};

/**
 * Get visualizer for an event type
 * @param {string} eventType - Event type (e.g., 'Proposal', 'Promise')
 * @returns {Object|null} Visualizer object or null if not found
 */
export function getEventVisualizer(eventType) {
  return EVENT_VISUALIZERS[eventType] || null;
}

/**
 * Get all event types with visualizers
 * @returns {Array<string>} Event type names
 */
export function getSupportedEventTypes() {
  return Object.keys(EVENT_VISUALIZERS);
}
