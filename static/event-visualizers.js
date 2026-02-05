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

function scheduleNodeReset(visualizer, nodeId, delayMs) {
  if (!visualizer) return;
  if (typeof visualizer.scheduleNodeReset === 'function') {
    visualizer.scheduleNodeReset(nodeId, delayMs);
    return;
  }
  setTimeout(() => {
    if (typeof visualizer.resetNodeToRoleColor === 'function') {
      visualizer.resetNodeToRoleColor(nodeId);
    }
  }, delayMs);
}

function getBeamDuration(snapshot, base = 500) {
  const speed = snapshot?.simulation?.speed || 1;
  return Math.max(200, (base / speed) * 0.67);
}

function getReachableTargets(fromId, totalNodes, canCommunicate) {
  if (!Number.isFinite(totalNodes)) return [];
  const targets = [];
  for (let i = 0; i < totalNodes; i++) {
    if (i !== fromId && canCommunicate(fromId, i)) {
      targets.push(i);
    }
  }
  return targets;
}

async function drawBeamsTo(visualizer, fromId, toIds, color, duration, pattern) {
  if (!toIds || toIds.length === 0) return;
  if (typeof visualizer.drawBeamsTo === 'function') {
    await visualizer.drawBeamsTo(fromId, toIds, color, duration, pattern);
    return;
  }
  const promises = toIds.map((toId) =>
    visualizer.drawBeam(fromId, toId, color, duration, pattern)
  );
  await Promise.all(promises);
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
      const duration = getBeamDuration(snapshot, 500);

      const targets = getReachableTargets(
        event.id,
        snapshot.cluster?.total_nodes,
        canCommunicate
      );
      await drawBeamsTo(visualizer, event.id, targets, this.color, duration, 'solid');

      scheduleNodeReset(visualizer, event.id, duration + 50);
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
        const duration = getBeamDuration(snapshot, 500);
        await visualizer.drawBeam(event.id, event.from, this.color, duration, 'dashed');
        scheduleNodeReset(visualizer, event.id, duration + 50);
      } else {
        scheduleNodeReset(visualizer, event.id, 200);
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
      const duration = getBeamDuration(snapshot, 500);

      const quorum = Array.isArray(event.quorum) ? event.quorum : [];
      const targets = quorum.filter(
        (nodeId) => nodeId !== event.id && canCommunicate(event.id, nodeId)
      );
      await drawBeamsTo(visualizer, event.id, targets, this.color, duration, 'solid');

      scheduleNodeReset(visualizer, event.id, duration + 50);
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

     const snapshot = state.snapshot();
     const duration = getBeamDuration(snapshot, 500);

     if (event.from !== undefined && event.from !== event.id) {
       await visualizer.drawBeam(event.id, event.from, this.color, duration, 'dashed');
     }

     scheduleNodeReset(visualizer, event.id, duration + 50);
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

     scheduleNodeReset(visualizer, event.id, 350);
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

      scheduleNodeReset(visualizer, event.id, 350);
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
      // Add decree to state
      state.addDecree(event.id, {
        decree_num: event.decree_num,
        decree: formatDecree(event),
        timestamp: Date.now()
      });
    }
  },

  BatchInitialDecrees: {
    color: '#8b5cf6', // Purple
    name: 'BatchInitialDecrees',

    format(event) {
      return `[Batch Init] Node ${event.id}: ${event.decrees.length} decrees loaded`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      // Add all decrees to state
      for (const [decree_num, value] of event.decrees) {
        state.addDecree(event.id, {
          decree_num,
          decree: typeof value === 'string' ? value : JSON.stringify(value),
          timestamp: Date.now()
        });
      }
    }
  },

  LedgerDump: {
    color: '#8b5cf6', // Purple
    name: 'LedgerDump',

    format(event) {
      return `[Ledger Dump] Node ${event.id}: ${event.decrees.length} total decrees`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      // Add all decrees to state
      for (const [decree_num, value] of event.decrees) {
        state.addDecree(event.id, {
          decree_num,
          decree: typeof value === 'string' ? value : JSON.stringify(value),
          timestamp: Date.now()
        });
      }
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
     
     // Draw beams from proposer to all other nodes (broadcast)
     const snapshot = state.snapshot();
     if (snapshot.cluster) {
       const targets = getReachableTargets(
         event.id,
         snapshot.cluster.total_nodes,
         canCommunicate
       );
       await drawBeamsTo(visualizer, event.id, targets, this.color, 350, 'solid');
       scheduleNodeReset(visualizer, event.id, 400);
     }
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

  LeaderElected: {
    color: '#fbbf24', // Amber
    name: 'Leader',

    format(event) {
      return `[Leader] Node ${event.id} elected as leader`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.setLeader(event.id);
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
