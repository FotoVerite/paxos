/**
 * Snapshot Utilities
 * Shared helpers for cloning and restoring playback state.
 */

export function cloneStateSnapshot(snapshot) {
  const nodes = new Map();
  snapshot.nodes.forEach((node, id) => {
    nodes.set(id, {
      state: node.state,
      partitioned: node.partitioned,
      decrees: Array.isArray(node.decrees)
        ? node.decrees.map((decree) => ({ ...decree }))
        : [],
      role: node.role
        ? {
            roles: Array.isArray(node.role.roles) ? [...node.role.roles] : [],
            learningStrategy: node.role.learningStrategy,
          }
        : null,
      color: node.color,
    });
  });

  return {
    cluster: snapshot.cluster ? { ...snapshot.cluster } : null,
    nodes,
    simulation: { ...snapshot.simulation, running: false },
    eventCounts: { ...snapshot.eventCounts },
  };
}

export function syncVisualizerToState(snapshot, visualizer) {
  if (!snapshot || !snapshot.nodes || !visualizer) return;

  if (typeof visualizer.clearScheduledResets === 'function') {
    visualizer.clearScheduledResets();
  }
  if (typeof visualizer.clearBeams === 'function') {
    visualizer.clearBeams();
  }

  snapshot.nodes.forEach((node, nodeId) => {
    if (typeof visualizer.clearLeader === 'function') {
      visualizer.clearLeader(nodeId);
    }
    if (typeof visualizer.setNodeState === 'function') {
      visualizer.setNodeState(nodeId, node.state || '--');
    }
    if (typeof visualizer.setNodePartitioned === 'function') {
      visualizer.setNodePartitioned(nodeId, Boolean(node.partitioned));
    } else if (typeof visualizer.resetNodeToRoleColor === 'function') {
      visualizer.resetNodeToRoleColor(nodeId);
    }
  });

  if (snapshot.leaderId !== null && snapshot.leaderId !== undefined) {
    if (typeof visualizer.setLeader === 'function') {
      visualizer.setLeader(snapshot.leaderId);
    }
  }
}

export function restoreSnapshotState(snapshot, state, visualizer) {
  if (!snapshot) return;
  state.restoreSnapshot(snapshot);
  syncVisualizerToState(snapshot, visualizer);
}
