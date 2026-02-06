/**
 * Demo State Management
 * Centralized state container for the Paxos live demo
 * Replaces scattered global variables with a single source of truth
 */

export class DemoState {
  constructor() {
    this.cluster = null;
    this.nodes = new Map(); // id -> { state, partitioned, decrees[], role, color }
    this.simulation = {
      running: false,
      speed: 1.0,
      selectedNode: null,
    };
    this.eventCounts = {};
    this.leaderId = null;
    this.listeners = new Set();
  }

  /**
   * Initialize cluster state
   * @param {Object} clusterInfo - { total_nodes, quorum_size }
   */
  initialize(clusterInfo) {
    this.cluster = clusterInfo;
    this.eventCounts = {};

    // Preserve existing nodes that may have been created by NodeCapabilities events
    const existingNodes = new Map(this.nodes);
    this.nodes.clear();

    for (let i = 0; i < clusterInfo.total_nodes; i++) {
      const existing = existingNodes.get(i);
      if (existing) {
        // Preserve role and other data from existing node
        this.nodes.set(i, existing);
      } else {
        // Create new node
        this.nodes.set(i, {
          state: '--',
          partitioned: false,
          decrees: [],
          role: null,
          color: '#3b82f6',
        });
      }
    }
    this.notify();
  }

  /**
   * Set node capabilities from backend event
   * @param {number} nodeId - Node ID
   * @param {Array<string>} roles - ['Proposer', 'Acceptor', 'Learner']
   * @param {Object} learningStrategy - { type: 'Direct'|'ProposerManaged'|'DistinguishedLearners', ... }
   */
  setNodeCapabilities(nodeId, roles, learningStrategy) {
    let node = this.nodes.get(nodeId);
    
    // Create node if it doesn't exist yet (NodeCapabilities may arrive before ClusterInitialized)
    if (!node) {
      node = {
        state: '--',
        partitioned: false,
        decrees: [],
        role: null,
        color: '#3b82f6',
      };
      this.nodes.set(nodeId, node);
    }
    
    node.role = { roles, learningStrategy };
    this.notify();
  }

  /**
   * Set node visual state text
   * @param {number} nodeId - Node ID
   * @param {string} state - State text to display
   */
  setNodeState(nodeId, state) {
    const node = this.nodes.get(nodeId);
    if (node) {
      node.state = state;
      this.notify();
    }
  }

  /**
   * Set node color
   * @param {number} nodeId - Node ID
   * @param {string} color - Hex color
   */
  setNodeColor(nodeId, color) {
    const node = this.nodes.get(nodeId);
    if (node) {
      node.color = color;
      this.notify();
    }
  }

  /**
   * Mark node as partitioned or healed
   * @param {number} nodeId - Node ID
   * @param {boolean} partitioned - True if partitioned
   */
  setNodePartitioned(nodeId, partitioned) {
    const node = this.nodes.get(nodeId);
    if (node) {
      node.partitioned = partitioned;
      this.notify();
    }
  }

  /**
   * Set the current leader
   * @param {number|null} nodeId - Node ID or null to clear
   */
  setLeader(nodeId) {
    this.leaderId = nodeId === null || nodeId === undefined ? null : nodeId;
    this.notify();
  }

  /**
   * Add decree learned by a node
   * @param {number} nodeId - Node ID
   * @param {Object} decree - { decree_num, decree, timestamp }
   */
  addDecree(nodeId, decree) {
    const node = this.nodes.get(nodeId);
    if (node) {
      node.decrees.unshift(decree); // Add to front for newest first
      this.notify();
    }
  }

  /**
   * Get all decrees for a node
   * @param {number} nodeId - Node ID
   * @returns {Array} Decrees
   */
  getDecrees(nodeId) {
    const node = this.nodes.get(nodeId);
    return node ? node.decrees : [];
  }

  /**
   * Get node info
   * @param {number} nodeId - Node ID
   * @returns {Object} Node info
   */
  getNode(nodeId) {
    return this.nodes.get(nodeId);
  }

  /**
   * Set simulation running state
   * @param {boolean} running - True if running
   */
  setRunning(running) {
    this.simulation.running = running;
    this.notify();
  }

  /**
   * Set simulation speed
   * @param {number} speed - Speed multiplier
   */
  setSpeed(speed) {
    this.simulation.speed = speed;
    this.notify();
  }

  /**
   * Select/deselect a node
   * @param {number} nodeId - Node ID (or null to deselect)
   */
  selectNode(nodeId) {
    this.simulation.selectedNode = this.simulation.selectedNode === nodeId ? null : nodeId;
    this.notify();
  }

  /**
   * Increment event counter
   * @param {string} eventType - Event type
   */
  incrementEventCount(eventType) {
    this.eventCounts[eventType] = (this.eventCounts[eventType] || 0) + 1;
    this.notify();
  }

  /**
   * Reset all event counts
   */
  resetEventCounts() {
    this.eventCounts = {};
    this.notify();
  }

  /**
   * Reset simulation state (for restart)
   */
  resetSimulation() {
    if (this.cluster) {
      for (let i = 0; i < this.cluster.total_nodes; i++) {
        const node = this.nodes.get(i);
        if (node) {
          node.state = '--';
          node.partitioned = false;
          node.decrees = [];
          node.color = '#3b82f6';
        }
      }
    }
    this.simulation.running = false;
    this.simulation.selectedNode = null;
    this.leaderId = null;
    this.eventCounts = {};
    this.notify();
  }

  /**
   * Subscribe to state changes
   * @param {Function} callback - Called with snapshot whenever state changes
   * @returns {Function} Unsubscribe function
   */
  subscribe(callback) {
    this.listeners.add(callback);
    return () => this.listeners.delete(callback);
  }

  /**
   * Notify all subscribers
   */
  notify() {
    const snapshot = this.snapshot();
    this.listeners.forEach(cb => {
      try {
        cb(snapshot);
      } catch (err) {
        console.error('Error in state subscriber:', err);
      }
    });
  }

  /**
   * Get immutable snapshot of current state
   * @returns {Object} State snapshot
   */
  snapshot() {
    return {
      cluster: this.cluster,
      nodes: new Map(this.nodes),
      simulation: { ...this.simulation },
      eventCounts: { ...this.eventCounts },
      leaderId: this.leaderId,
    };
  }

  /**
   * Restore state from a snapshot
   * @param {Object} snapshot - Snapshot from snapshot()
   */
  restoreSnapshot(snapshot) {
    if (!snapshot) return;
    this.cluster = snapshot.cluster ? { ...snapshot.cluster } : null;
    this.nodes = new Map();
    snapshot.nodes.forEach((node, id) => {
      this.nodes.set(id, {
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
    this.simulation = { ...snapshot.simulation };
    this.eventCounts = { ...snapshot.eventCounts };
    this.leaderId = snapshot.leaderId ?? null;
    this.notify();
  }

  /**
   * Clear all state
   */
  clear() {
    this.cluster = null;
    this.nodes.clear();
    this.simulation = {
      running: false,
      speed: 1.0,
      selectedNode: null,
    };
    this.eventCounts = {};
    this.leaderId = null;
    this.notify();
  }
}

// Export singleton instance
export const state = new DemoState();
