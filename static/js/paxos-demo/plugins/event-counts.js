/**
 * Event Counts Plugin
 * Increments counters and updates summary UI
 */

const DEFAULT_COUNT_MAP = {
  Proposal: 'nextBallotCount',
  Promise: 'lastVoteCount',
  Accept: 'beginBallotCount',
  Accepted: 'votedCount',
  LearnedValue: 'learnedCount',
  Success: 'successCount',
};

function renderCounts(state, countMap) {
  const snapshot = state.snapshot();
  for (const [eventType, elementId] of Object.entries(countMap)) {
    const el = document.getElementById(elementId);
    if (!el) continue;
    el.textContent = snapshot.eventCounts[eventType] || 0;
  }
}

export function createEventCountsPlugin({ countMap = DEFAULT_COUNT_MAP } = {}) {
  return {
    onEvent({ eventType }, ctx) {
      if (!countMap[eventType]) return;
      ctx.state.incrementEventCount(eventType);
      renderCounts(ctx.state, countMap);
    },

    onCluster(_, ctx) {
      renderCounts(ctx.state, countMap);
    },

    onReset(ctx) {
      renderCounts(ctx.state, countMap);
    },

    onRestore(_, ctx) {
      renderCounts(ctx.state, countMap);
    },
  };
}
