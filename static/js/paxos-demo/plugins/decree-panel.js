/**
 * Decree Panel Plugin
 * Renders per-node decree counts and the decree detail panel
 */

const DECREE_EVENTS = new Set([
  'LearnedValue',
  'InitialDecree',
  'BatchInitialDecrees',
  'LedgerDump',
]);

function formatDecree(event) {
  if (event.value === 'NOOP') return 'NOOP';
  if (event.value && typeof event.value === 'object' && event.value.EnactDecree) {
    return event.value.EnactDecree.law;
  }
  return `Decree #${event.decree_num}`;
}

function renderStats(state, statsContainer) {
  if (!statsContainer) return;
  const snapshot = state.snapshot();
  if (!snapshot.cluster) return;

  const selectedNodeId = snapshot.simulation.selectedNode;
  let html = "<div class='proposal-stats'>";

  for (let nodeId = 0; nodeId < snapshot.cluster.total_nodes; nodeId++) {
    const node = snapshot.nodes.get(nodeId);
    const decreeCount = node?.decrees.length || 0;
    const isSelected = selectedNodeId === nodeId;
    const selectClass = isSelected ? 'selected' : '';
    html += `<div class='proposal-stat-item ${selectClass}' data-node-id='${nodeId}'>`;
    html += `<span class='node-id'>N${nodeId}:</span> ${decreeCount}`;
    html += `</div>`;
  }

  html += '</div>';
  statsContainer.innerHTML = html;
}

function renderDecreePanel(state, decreePanel) {
  if (!decreePanel) return;
  const snapshot = state.snapshot();
  const selectedNodeId = snapshot.simulation.selectedNode;

  if (selectedNodeId === null || selectedNodeId === undefined) {
    decreePanel.innerHTML = "<p class='decree-hint'>Click a node to view its learned decrees</p>";
    return;
  }

  const node = snapshot.nodes.get(selectedNodeId);
  if (!node || node.decrees.length === 0) {
    decreePanel.innerHTML = `<p class='decree-hint'>Node ${selectedNodeId} has not learned any decrees yet</p>`;
    return;
  }

  const sortedDecrees = [...node.decrees].sort((a, b) => a.decree_num - b.decree_num);
  let html = `<div class='decree-content'>`;
  html += `<div class='decree-node-label'>Node ${selectedNodeId} (${node.decrees.length} decrees)</div>`;
  html += `<div class='decree-list'>`;

  for (const decree of sortedDecrees) {
    html += `<div class='decree-item'><div class='decree-text'>[${decree.decree_num}] "${decree.decree}"</div></div>`;
  }

  html += '</div></div>';
  decreePanel.innerHTML = html;
}

function renderAll(state, statsContainer, decreePanel) {
  renderStats(state, statsContainer);
  renderDecreePanel(state, decreePanel);
}

export function createDecreePanelPlugin({ statsContainer, decreePanel } = {}) {
  function handleStatsClick(event, ctx) {
    const target = event.target.closest('[data-node-id]');
    if (!target || !statsContainer.contains(target)) return;

    const nodeId = Number(target.dataset.nodeId);
    if (Number.isNaN(nodeId)) return;

    ctx.state.selectNode(nodeId);
    renderAll(ctx.state, statsContainer, decreePanel);
  }

  return {
    init(ctx) {
      if (statsContainer) {
        statsContainer.addEventListener('click', (event) => handleStatsClick(event, ctx));
      }
    },

    onCluster(_, ctx) {
      renderAll(ctx.state, statsContainer, decreePanel);
    },

    onEvent({ eventType, eventData }, ctx) {
      if (eventType === 'LearnedValue' && eventData?.decree_num !== undefined) {
        ctx.state.addDecree(eventData.id, {
          decree_num: eventData.decree_num,
          decree: formatDecree(eventData),
          timestamp: Date.now(),
        });
      }

      if (DECREE_EVENTS.has(eventType)) {
        renderAll(ctx.state, statsContainer, decreePanel);
      }
    },

    onReset(ctx) {
      ctx.state.selectNode(null);
      renderAll(ctx.state, statsContainer, decreePanel);
    },

    onRestore(_, ctx) {
      renderAll(ctx.state, statsContainer, decreePanel);
    },
  };
}
