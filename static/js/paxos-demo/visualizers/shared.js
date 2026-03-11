/**
 * Shared helper functions for event visualizers.
 */

export function formatDecree(event) {
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

export function nodeText(labels, nodeId) {
  if (labels && typeof labels.node === 'function') {
    return labels.node(nodeId);
  }
  return `Node ${nodeId}`;
}

export function listText(labels, nodeIds) {
  if (!Array.isArray(nodeIds)) return '[]';
  return `[${nodeIds.map((nodeId) => labelOf(labels, nodeId)).join(', ')}]`;
}

export function scheduleNodeReset(visualizer, nodeId, delayMs) {
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

export function getBeamDuration(snapshot, base = 500) {
  const speed = snapshot?.simulation?.speed || 1;
  return Math.max(200, (base / speed) * 0.67);
}

export function getReachableTargets(fromId, totalNodes, canCommunicate) {
  if (!Number.isFinite(totalNodes)) return [];
  const targets = [];
  for (let i = 0; i < totalNodes; i++) {
    if (i !== fromId && canCommunicate(fromId, i)) {
      targets.push(i);
    }
  }
  return targets;
}

export function getLeaderTargets(snapshot, fromId, canCommunicate) {
  const nodes = snapshot?.nodes;
  if (!(nodes instanceof Map)) return [];
  const targets = [];
  nodes.forEach((node, nodeId) => {
    const isLeader = Array.isArray(node?.role?.roles) && node.role.roles.includes('Leader');
    if (!isLeader || nodeId === fromId) return;
    if (canCommunicate(fromId, nodeId)) {
      targets.push(nodeId);
    }
  });
  return targets;
}

export async function drawBeamsTo(visualizer, fromId, toIds, color, duration, pattern) {
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
