import {
  flashNodeWithMotion,
  getBeamDuration,
  listText,
  nodeText,
  scheduleNodeReset
} from '/js/paxos-demo/visualizers/shared.js';

function strategyLabel(strategy) {
  return typeof strategy === 'string' ? strategy : 'UnknownStrategy';
}

function commandLabel(cmd) {
  if (!cmd) return 'none';
  if (typeof cmd === 'string') return cmd;
  if (cmd.ClientRequest && typeof cmd.ClientRequest === 'object') {
    return commandLabel(cmd.ClientRequest.cmd);
  }
  if (typeof cmd !== 'object') return String(cmd);
  const [kind] = Object.keys(cmd);
  return kind || 'unknown';
}

function isNoopCommand(cmd) {
  return commandLabel(cmd) === 'NOOP';
}

function hasStopBoundary(event) {
  return event.stop_slot !== null && event.stop_slot !== undefined;
}

function isMeaningfulProposalGateEvent(event) {
  const original = commandLabel(event.original_cmd);
  const effective = commandLabel(event.effective_cmd);
  if (hasStopBoundary(event)) return true;
  if (event.outcome !== 'admitted') return true;
  if (event.barrier_active) return true;
  return original !== effective;
}

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
      flashNodeWithMotion(visualizer, event.id, this.color, {
        motion: 'failure',
        resetDelay: 320,
      });
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
      if (typeof visualizer.pulseNode === 'function') {
        visualizer.pulseNode(event.id, this.color, {
          resetDelay: 420,
          glowRadius: 12,
          glowOpacity: 0.82,
        });
      }
      if (typeof visualizer.setNodeCrashed === 'function') {
        visualizer.setNodeCrashed(event.id, true);
      }
    }
  },

  ReconfigurationRequested: {
    color: '#fbbf24',
    name: 'Reconfig',
    format(event, labels) {
      return `[RECONFIG] requested ${strategyLabel(event.strategy)} (+${listText(labels, event.add_nodes)}, -${listText(labels, event.remove_nodes)})`;
    },
    async visualize(event, visualizer) {}
  },

  ReconfigurationStopStarted: {
    color: '#f97316',
    name: 'StopStart',
    format(event, labels) {
      return `[RECONFIG] stop started (${strategyLabel(event.strategy)}): ${listText(labels, event.target_nodes)}`;
    },
    async visualize(event, visualizer) {
      if (typeof visualizer.setNodeState !== 'function') return;
      for (const nodeId of event.target_nodes || []) {
        visualizer.setNodeState(nodeId, 'stop');
      }
    }
  },

  ReconfigurationStopCommandSent: {
    color: '#f59e0b',
    name: 'StopSend',
    format(event, labels) {
      return `[RECONFIG] stop command sent to ${nodeText(labels, event.to)}`;
    },
    async visualize(event, visualizer, state, canCommunicate) {
      const snapshot = typeof state?.snapshot === 'function' ? state.snapshot() : null;
      const leader = snapshot?.leaderId;
      const duration = getBeamDuration(snapshot, 220);
      if (
        Number.isFinite(leader) &&
        leader !== event.to &&
        typeof canCommunicate === 'function' &&
        canCommunicate(leader, event.to) &&
        typeof visualizer.drawBeam === 'function'
      ) {
        await visualizer.drawBeam(leader, event.to, this.color, duration, 'dashed', {
          motion: 'proposal',
        });
      }
      if (typeof visualizer.setNodeState === 'function') {
        visualizer.setNodeState(event.to, 'stop-send');
      }
      flashNodeWithMotion(visualizer, event.to, this.color, {
        motion: 'proposal',
        resetDelay: duration + 40,
      });
      scheduleNodeReset(visualizer, event.to, duration + 40);
    }
  },

  ReconfigurationStopCompleted: {
    color: '#f59e0b',
    name: 'Stopped',
    format(event, labels) {
      return `[RECONFIG] stop completed (${strategyLabel(event.strategy)}): ${listText(labels, event.stopped_nodes)}`;
    },
    async visualize(event, visualizer) {
      if (typeof visualizer.setNodeState !== 'function') return;
      for (const nodeId of event.stopped_nodes || []) {
        visualizer.setNodeState(nodeId, 'stopped');
      }
    }
  },

  ReconfigurationStopDecided: {
    color: '#f59e0b',
    name: 'StopDecided',
    format(event, labels) {
      return `[RECONFIG] stop decided on ${nodeText(labels, event.id)} @slot=${event.slot} (${strategyLabel(event.strategy)}, delay=${event.delayed_slots ?? 0})`;
    },
    async visualize(event, visualizer) {
      if (typeof visualizer.setNodeState === 'function') {
        visualizer.setNodeState(event.id, 'stop-decided');
      }
      scheduleNodeReset(visualizer, event.id, 260);
    }
  },

  ReconfigurationStopApplied: {
    color: '#f97316',
    name: 'StopApplied',
    format(event, labels) {
      return `[RECONFIG] stop applied on ${nodeText(labels, event.id)} @slot=${event.slot} (${strategyLabel(event.strategy)})`;
    },
    async visualize(event, visualizer) {
      if (typeof visualizer.setNodeState === 'function') {
        visualizer.setNodeState(event.id, 'stop-applied');
      }
      scheduleNodeReset(visualizer, event.id, 260);
    }
  },

  ReconfigurationProposalObserved: {
    color: '#38bdf8',
    name: 'Proposal',
    format(event, labels) {
      if (!isMeaningfulProposalGateEvent(event)) {
        return '';
      }
      const slot = event.slot === null || event.slot === undefined ? '-' : event.slot;
      const stopSlot = event.stop_slot === null || event.stop_slot === undefined ? '-' : event.stop_slot;
      const barrier = event.barrier_active ? 'on' : 'off';
      const effective = commandLabel(event.effective_cmd);
      const original = commandLabel(event.original_cmd);
      const strategy = strategyLabel(event.strategy);
      return `[RECONFIG] proposal ${event.outcome} on ${nodeText(labels, event.id)} strategy=${strategy} slot=${slot} stop_slot=${stopSlot} barrier=${barrier} cmd=${original}->${effective}`;
    },
    async visualize(event, visualizer) {
      if (!isMeaningfulProposalGateEvent(event)) {
        return;
      }
      if (typeof visualizer.setNodeState !== 'function') return;
      if (event.outcome === 'stopped') {
        visualizer.setNodeState(event.id, event.strategy === 'BrickWall' ? 'brick' : 'stopped');
      } else if (event.outcome === 'queued') {
        visualizer.setNodeState(event.id, 'queued');
      } else if (event.outcome === 'admitted') {
        if (
          event.strategy === 'Padding' &&
          !isNoopCommand(event.original_cmd) &&
          isNoopCommand(event.effective_cmd)
        ) {
          visualizer.setNodeState(event.id, 'noop-pad');
        } else if (event.strategy === 'DelayedStopSign' && !event.barrier_active) {
          visualizer.setNodeState(event.id, 'grace');
        } else {
          visualizer.setNodeState(event.id, 'admit');
        }
      } else {
        visualizer.setNodeState(event.id, 'cached');
      }
      scheduleNodeReset(visualizer, event.id, 220);
    }
  },

  ReconfigurationCheckpointSelected: {
    color: '#38bdf8',
    name: 'Checkpoint',
    format(event, labels) {
      const source =
        event.source_node !== null && event.source_node !== undefined
          ? nodeText(labels, event.source_node)
          : 'unknown';
      return `[RECONFIG] checkpoint selected (${strategyLabel(event.strategy)}): source=${source}, slot=${event.last_applied_slot ?? 'none'}`;
    },
    async visualize(event, visualizer) {
      if (event.source_node === null || event.source_node === undefined) return;
      if (typeof visualizer.setNodeState === 'function') {
        visualizer.setNodeState(event.source_node, 'checkpoint');
      }
      scheduleNodeReset(visualizer, event.source_node, 300);
    }
  },

  ReconfigurationApplied: {
    color: '#34d399',
    name: 'Applied',
    format(event) {
      return `[RECONFIG] applied ${strategyLabel(event.strategy)}: nodes ${event.previous_node_count} -> ${event.next_node_count}`;
    },
    async visualize(event, visualizer) {}
  },

  ReconfigurationReady: {
    color: '#10b981',
    name: 'Ready',
    format(event, labels) {
      const leader =
        event.leader !== null && event.leader !== undefined
          ? nodeText(labels, event.leader)
          : 'none';
      return `[RECONFIG] ready (${strategyLabel(event.strategy)}): leader=${leader}, active=${listText(labels, event.active_nodes)}`;
    },
    async visualize(event, visualizer) {
      if (event.leader !== null && event.leader !== undefined && typeof visualizer.setLeader === 'function') {
        visualizer.setLeader(event.leader);
      }
      if (typeof visualizer.setNodeState === 'function') {
        for (const nodeId of event.active_nodes || []) {
          visualizer.setNodeState(nodeId, 'active');
        }
      }
    }
  },

  ReconfigurationNodeRetired: {
    color: '#ef4444',
    name: 'Retired',
    format(event, labels) {
      return `[RECONFIG] node retired ${nodeText(labels, event.id)}`;
    },
    async visualize(event, visualizer) {
      if (typeof visualizer.activateNode === 'function') {
        visualizer.activateNode(event.id, this.color);
      }
      if (typeof visualizer.clearLeader === 'function') {
        visualizer.clearLeader(event.id);
      }
      if (typeof visualizer.setNodeState === 'function') {
        visualizer.setNodeState(event.id, 'retired');
      }
      if (typeof visualizer.setNodeCrashed === 'function') {
        visualizer.setNodeCrashed(event.id, true);
      }
    }
  },

  ReconfigurationNodeRebooted: {
    color: '#22c55e',
    name: 'Rebooted',
    format(event, labels) {
      return `[RECONFIG] node rebooted ${nodeText(labels, event.id)}`;
    },
    async visualize(event, visualizer) {
      if (typeof visualizer.activateNode === 'function') {
        visualizer.activateNode(event.id, this.color);
      }
      if (typeof visualizer.setNodeCrashed === 'function') {
        visualizer.setNodeCrashed(event.id, false);
      }
      if (typeof visualizer.setNodeState === 'function') {
        visualizer.setNodeState(event.id, 'reboot');
      }
      scheduleNodeReset(visualizer, event.id, 350);
    }
  },

  ReconfigurationFailed: {
    color: '#ef4444',
    name: 'ReconfigFail',
    format(event) {
      return `[RECONFIG] failed (${strategyLabel(event.strategy)} @ ${event.phase}): ${event.reason}`;
    },
    async visualize(event, visualizer) {}
  }
};
