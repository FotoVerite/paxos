/**
 * Node Capabilities Plugin
 * Keeps role capabilities in sync for state + visualizer.
 */

export function createNodeCapabilitiesPlugin() {
  return {
    onEvent({ eventType, eventData }, ctx) {
      if (eventType !== 'NodeCapabilities' || !eventData) return;
      ctx.state.setNodeCapabilities(
        eventData.id,
        eventData.roles,
        eventData.learning_strategy
      );
      if (ctx.visualizer && typeof ctx.visualizer.setNodeCapabilities === 'function') {
        ctx.visualizer.setNodeCapabilities(
          eventData.id,
          eventData.roles,
          eventData.learning_strategy
        );
        if (typeof ctx.visualizer.resetNodeToRoleColor === 'function') {
          ctx.visualizer.resetNodeToRoleColor(eventData.id);
        }
      }
    },

    onCluster(_, ctx) {
      const snapshot = ctx.state.snapshot();
      snapshot.nodes.forEach((node, nodeId) => {
        if (!node.role) return;
        if (ctx.visualizer && typeof ctx.visualizer.setNodeCapabilities === 'function') {
          ctx.visualizer.setNodeCapabilities(
            nodeId,
            node.role.roles,
            node.role.learningStrategy
          );
        }
        if (ctx.visualizer && typeof ctx.visualizer.resetNodeToRoleColor === 'function') {
          ctx.visualizer.resetNodeToRoleColor(nodeId);
        }
      });
    },
  };
}
