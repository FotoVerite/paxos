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

function labelOf(labels, nodeId) {
  if (labels && typeof labels.label === 'function') {
    return labels.label(nodeId);
  }
  return String(nodeId);
}

function nodeText(labels, nodeId) {
  if (labels && typeof labels.node === 'function') {
    return labels.node(nodeId);
  }
  return `Node ${nodeId}`;
}

function listText(labels, nodeIds) {
  if (!Array.isArray(nodeIds)) return '[]';
  return `[${nodeIds.map((nodeId) => labelOf(labels, nodeId)).join(', ')}]`;
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

    format(event, labels) {
      return `[NextBallot] ${nodeText(labels, event.id)}: "${formatDecree(event)}"`;
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

    format(event, labels) {
      return `[LastVote] ${nodeText(labels, event.id)} → ${nodeText(labels, event.from)}: Ballot ${event.ballot}`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.setNodeState(event.id, 'promise');
      visualizer.activateNode(event.id, this.color);

      if (event.from !== undefined && event.from !== event.id) {
        const snapshot = state.snapshot();
        const duration = getBeamDuration(snapshot, 500);
        if (canCommunicate(event.id, event.from)) {
          await visualizer.drawBeam(event.id, event.from, this.color, duration, 'dashed');
        }
        scheduleNodeReset(visualizer, event.id, duration + 50);
      } else {
        scheduleNodeReset(visualizer, event.id, 200);
      }
    }
  },

  Accept: {
    color: '#f87171', // Red
    name: 'BeginBallot',

    format(event, labels) {
      return `[BeginBallot] ${nodeText(labels, event.id)}: Ballot ${event.ballot}, Decree #${event.decree_num}`;
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

   format(event, labels) {
     return `[Voted] ${nodeText(labels, event.id)} → ${nodeText(labels, event.from)}: Ballot ${event.ballot}`;
   },

   async visualize(event, visualizer, state, canCommunicate) {
     visualizer.setNodeState(event.id, 'voted');
     visualizer.activateNode(event.id, this.color);

     const snapshot = state.snapshot();
     const duration = getBeamDuration(snapshot, 500);

     if (event.from !== undefined && event.from !== event.id) {
       if (canCommunicate(event.id, event.from)) {
         await visualizer.drawBeam(event.id, event.from, this.color, duration, 'dotted');
       }
     }

     scheduleNodeReset(visualizer, event.id, duration + 50);
   }
  },

  Learn: {
   color: '#34d399', // Emerald
   name: 'Learn',

   format(event, labels) {
     const decree = formatDecree(event);
     return `[Learn] ${nodeText(labels, event.id)}: "${decree}"`;
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

    format(event, labels) {
      const decree = formatDecree(event);
      return `[LearnedValue] ${nodeText(labels, event.id)}: "${decree}"`;
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

    format(event, labels) {
      const decree = formatDecree(event);
      return `[InitialDecree] ${nodeText(labels, event.id)}: [${event.decree_num}] "${decree}"`;
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

    format(event, labels) {
      return `[Batch Init] ${nodeText(labels, event.id)}: ${event.decrees.length} decrees loaded`;
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

    format(event, labels) {
      return `[Ledger Dump] ${nodeText(labels, event.id)}: ${event.decrees.length} total decrees`;
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

   format(event, labels) {
     const proposerInfo =
       event.ballot_proposer !== undefined
         ? ` (proposed by ${nodeText(labels, event.ballot_proposer)})`
         : '';
     return `[Success] ${nodeText(labels, event.id)}: Decree #${event.decree_num} chosen${proposerInfo}`;
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

    format(event, labels) {
      return `Network partitioned: A=${listText(labels, event.partition_a)}, B=${listText(labels, event.partition_b)}`;
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

    format(event, labels) {
      return `[Leader] ${nodeText(labels, event.id)} elected as leader`;
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
