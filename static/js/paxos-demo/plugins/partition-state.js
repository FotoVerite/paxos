/**
 * Partition State Plugin
 * Keeps demo state and visualizer in sync for partitions
 */

export function createPartitionStatePlugin() {
  return {
    onEvent({ eventType, eventData }, ctx) {
      if (!eventData) return;

      if (eventType === 'PartitionCreated') {
        const partitioned = eventData.partition_b || [];
        for (const nodeId of partitioned) {
          ctx.state.setNodePartitioned(nodeId, true);
          ctx.visualizer.setNodePartitioned(nodeId, true);
        }
      }

      if (eventType === 'PartitionHealed') {
        const allNodes = [
          ...(eventData.partition_a || []),
          ...(eventData.partition_b || []),
        ];
        for (const nodeId of allNodes) {
          ctx.state.setNodePartitioned(nodeId, false);
          ctx.visualizer.setNodePartitioned(nodeId, false);
        }
      }
    },
  };
}
