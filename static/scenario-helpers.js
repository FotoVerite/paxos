/**
 * Scenario Phase Helpers
 * Builder pattern for constructing scenario phases
 * Reduces boilerplate in scenario definitions
 */

export class ScenarioPhase {
  constructor(visualizer, utils) {
    this.v = visualizer;
    this.u = utils;
    this.actions = [];
  }

  /**
   * Run all actions in sequence
   * @param {Array<Function>} actions - Array of async action functions
   * @returns {Promise}
   */
  async run(actions) {
    for (const action of actions) {
      await action();
    }
  }

  /**
   * Reset nodes to default state
   * @param {Array<number>} ids - Node IDs
   * @param {string} color - Default color (default '#3b82f6')
   * @returns {Function} Action function
   */
  resetNodes(ids, color = '#3b82f6') {
    return async () => {
      ids.forEach(id => {
        this.v.setNodeState(id, '--');
        this.v.setNodeColor(id, color);
      });
      this.v.clearBeams();
    };
  }

  /**
   * Set node state text
   * @param {number} id - Node ID
   * @param {string} state - State text
   * @returns {Function} Action function
   */
  nodeState(id, state) {
    return async () => {
      this.v.setNodeState(id, state);
    };
  }

  /**
   * Activate node with color
   * @param {number} id - Node ID
   * @param {string} color - Hex color
   * @returns {Function} Action function
   */
  activate(id, color) {
    return async () => {
      this.v.activateNode(id, color);
    };
  }

  /**
   * Set both state and color for a node
   * @param {number} id - Node ID
   * @param {string} state - State text
   * @param {string} color - Hex color
   * @returns {Function} Action function
   */
  stateAndColor(id, state, color) {
    return async () => {
      this.v.setNodeState(id, state);
      this.v.activateNode(id, color);
    };
  }

  /**
   * Draw beams from one node to many
   * @param {number} from - Source node ID
   * @param {Array<number>} to - Target node IDs
   * @param {string} color - Hex color
   * @param {number} duration - Animation duration (ms)
   * @param {number} stagger - Stagger delay between beams (ms)
   * @returns {Function} Action function
   */
  beamsTo(from, to, color, duration = 500, stagger = 80) {
    return async () => {
      await this.v.drawBeamsTo(from, to, color, duration, 'solid', stagger);
    };
  }

  /**
   * Draw beams from many nodes to one
   * @param {Array<number>} from - Source node IDs
   * @param {number} to - Target node ID
   * @param {string} color - Hex color
   * @param {number} duration - Animation duration (ms)
   * @param {number} stagger - Stagger delay between beams (ms)
   * @returns {Function} Action function
   */
  beamsFrom(from, to, color, duration = 500, stagger = 150) {
    return async () => {
      await this.v.drawBeamsFrom(from, to, color, duration, 'dashed', stagger);
    };
  }

  /**
   * Add event to log
   * @param {string} msg - Event message
   * @param {string} color - Hex color
   * @returns {Function} Action function
   */
  log(msg, color) {
    return async () => {
      this.u.addEvent(msg, color);
    };
  }

  /**
   * Wait for duration
   * @param {number} ms - Milliseconds to wait
   * @returns {Function} Action function
   */
  wait(ms = 300) {
    return async () => {
      await this.u.sleep(ms);
    };
  }

  /**
   * Increment event counter and update UI
   * @param {string} counter - Counter name
   * @returns {Function} Action function
   */
  incr(counter) {
    return async () => {
      this.u.eventCounts[counter]++;
      this.u.updateCounts();
    };
  }

  /**
   * Clear all beams
   * @returns {Function} Action function
   */
  clearBeams() {
    return async () => {
      this.v.clearBeams();
    };
  }

  /**
   * Set multiple nodes to the same state
   * @param {Array<number>} ids - Node IDs
   * @param {string} state - State text
   * @param {string} color - Hex color
   * @returns {Function} Action function
   */
  multiState(ids, state, color) {
    return async () => {
      ids.forEach(id => {
        this.v.setNodeState(id, state);
        this.v.activateNode(id, color);
      });
    };
  }

  /**
   * Activate multiple nodes
   * @param {Array<number>} ids - Node IDs
   * @param {string} color - Hex color
   * @returns {Function} Action function
   */
  multiActivate(ids, color) {
    return async () => {
      ids.forEach(id => {
        this.v.activateNode(id, color);
      });
    };
  }
}
