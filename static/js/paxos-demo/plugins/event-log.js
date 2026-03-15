/**
 * Event Log Plugin
 * Renders event messages with color accents
 */

import { getEventVisualizer } from '/event-visualizers.js';

const FILTER_PRESETS = [
  { key: 'all', label: 'All', types: null },
  { key: 'nextballot', label: 'NextBallot', types: ['Proposal'] },
  { key: 'lastvote', label: 'LastVote', types: ['Promise'] },
  { key: 'beginballot', label: 'BeginBallot', types: ['Accept'] },
  { key: 'voted', label: 'Voted', types: ['Accepted'] },
  { key: 'learn', label: 'Learn', types: ['Learn', 'LearnedValue'] },
  { key: 'success', label: 'Success', types: ['Success'] },
  { key: 'partition', label: 'Partition', types: ['PartitionCreated', 'PartitionHealed'] },
  { key: 'leader', label: 'Leader', types: ['LeaderElected'] },
  {
    key: 'reconfig',
    label: 'Reconfig',
    types: [
      'ReconfigurationRequested',
      'ReconfigurationStopStarted',
      'ReconfigurationStopCommandSent',
      'ReconfigurationStopCompleted',
      'ReconfigurationStopDecided',
      'ReconfigurationStopApplied',
      'ReconfigurationProposalObserved',
      'ReconfigurationCheckpointSelected',
      'ReconfigurationNodeRetired',
      'ReconfigurationNodeRebooted',
      'ReconfigurationApplied',
      'ReconfigurationReady',
      'ReconfigurationFailed',
    ],
  },
  { key: 'initial', label: 'Initial', types: ['InitialDecree', 'BatchInitialDecrees', 'LedgerDump'] },
];

export function createEventLogPlugin({
  eventLog,
  limit = 30,
  presets = FILTER_PRESETS,
  defaultFilter = 'all',
  excludeTypes = [],
  filterBar,
  filterChips,
  filterToggle,
  filterClose,
  filterStatus,
  copyAllButton,
} = {}) {
  const activePresets = Array.isArray(presets) && presets.length > 0 ? presets : FILTER_PRESETS;
  const excludedTypeSet = new Set(excludeTypes || []);
  const items = [];
  let cursor = -1;
  let baseTimestamp = null;
  let replayCheckpointSlot = null;
  let activeFilter = defaultFilter || 'all';
  const typeCounts = {};
  const filterButtons = new Map();
  let scrollRaf = null;

  function formatEventText(eventType, eventData, ctx) {
    const viz = getEventVisualizer(eventType);
    if (!viz || typeof viz.format !== 'function') return '';
    return viz.format(eventData, ctx?.nodeLabels);
  }

  function detectReplay(eventType, eventData) {
    if (eventType !== 'PmmcP2B') return false;
    if (!Number.isInteger(replayCheckpointSlot)) return false;
    const slot = eventData?.pvalue?.slot;
    return Number.isInteger(slot) && slot <= replayCheckpointSlot;
  }

  function formatRelativeTime(createdAtMicros) {
    if (!Number.isFinite(createdAtMicros) || !Number.isFinite(baseTimestamp)) {
      return '';
    }
    const deltaMicros = Math.max(0, createdAtMicros - baseTimestamp);
    const deltaMs = deltaMicros / 1000;
    if (deltaMs < 1000) {
      return `+${deltaMs.toFixed(1)}ms`;
    }
    const deltaSec = deltaMs / 1000;
    if (deltaSec < 60) {
      return `+${deltaSec.toFixed(2)}s`;
    }
    const minutes = Math.floor(deltaSec / 60);
    const seconds = deltaSec - minutes * 60;
    return `+${minutes}m ${seconds.toFixed(1)}s`;
  }

  function matchesFilter(item) {
    if (activeFilter === 'all') return true;
    const preset = activePresets.find((entry) => entry.key === activeFilter);
    if (!preset || !preset.types) return true;
    return preset.types.includes(item.eventType);
  }

  function updateFilterStatus() {
    if (!filterStatus) return;
    if (activeFilter === 'all') {
      filterStatus.textContent = 'All events';
      return;
    }
    const preset = activePresets.find((entry) => entry.key === activeFilter);
    filterStatus.textContent = preset ? `${preset.label} only` : 'Filtered';
  }

  function updateFilterButtons() {
    for (const [key, button] of filterButtons.entries()) {
      button.classList.toggle('active', key === activeFilter);
      const preset = activePresets.find((entry) => entry.key === key);
      if (!preset) continue;
      const count = preset.types
        ? preset.types.reduce((sum, type) => sum + (typeCounts[type] || 0), 0)
        : Object.values(typeCounts).reduce((sum, value) => sum + value, 0);
      const countEl = button.querySelector('[data-count]');
      if (countEl) {
        countEl.textContent = count;
      }
    }
    updateFilterStatus();
  }

  function setActiveFilter(key) {
    activeFilter = key || 'all';
    updateVisibility();
    scrollToActive();
    updateFilterButtons();
  }

  function updateVisibility() {
    for (const item of items) {
      const shouldShow = matchesFilter(item);
      item.node.classList.toggle('filtered', !shouldShow);
    }
  }

  function updateFutureClasses() {
    for (const item of items) {
      const isFuture = item.frameIndex > cursor;
      item.node.classList.toggle('future', isFuture);
    }
  }

  function scrollToActive() {
    if (!eventLog) return;
    if (scrollRaf !== null) {
      cancelAnimationFrame(scrollRaf);
      scrollRaf = null;
    }
    let activeItem = items.find(
      (item) => item.frameIndex === cursor && matchesFilter(item)
    );
    if (!activeItem) {
      activeItem = items.find(
        (item) => item.frameIndex <= cursor && matchesFilter(item)
      );
    }
    if (!activeItem) return;
    // Defer until after DOM/layout settles; prepend insertions can otherwise
    // anchor the viewport and override immediate scrollTop writes.
    scrollRaf = requestAnimationFrame(() => {
      eventLog.scrollTop = activeItem.node.offsetTop;
      scrollRaf = null;
    });
  }

  async function copyAllLogs() {
    const lines = [...items]
      .reverse()
      .map((item) => {
        const time = item.node.querySelector('.event-time')?.textContent || '';
        const text = item.textEl?.textContent || '';
        return time ? `${time} ${text}` : text;
      })
      .filter(Boolean);
    const payload = lines.join('\n');
    if (!payload) return false;

    try {
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(payload);
      } else {
        const ta = document.createElement('textarea');
        ta.value = payload;
        ta.setAttribute('readonly', 'true');
        ta.style.position = 'absolute';
        ta.style.left = '-9999px';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
      }
      return true;
    } catch (err) {
      console.error('[event-log] failed to copy logs:', err);
      return false;
    }
  }

  return {
    init() {
      if (eventLog) {
        // Disable browser scroll anchoring so prepending entries does not
        // fight plugin-controlled positioning.
        eventLog.style.overflowAnchor = 'none';
      }
      if (!filterChips) return;
      filterChips.innerHTML = '';
      filterButtons.clear();

      for (const preset of activePresets) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'event-filter-chip';
        button.dataset.filter = preset.key;
        button.innerHTML = `<span class="label">${preset.label}</span><span class="count" data-count>0</span>`;
        button.addEventListener('click', () => setActiveFilter(preset.key));
        filterChips.appendChild(button);
        filterButtons.set(preset.key, button);
      }

      if (filterToggle && filterBar) {
        filterToggle.addEventListener('click', () => {
          filterBar.classList.toggle('hidden');
        });
      }

      if (filterClose && filterBar) {
        filterClose.addEventListener('click', () => {
          filterBar.classList.add('hidden');
        });
      }

      if (copyAllButton) {
        copyAllButton.addEventListener('click', async () => {
          const originalText = copyAllButton.textContent;
          const copied = await copyAllLogs();
          copyAllButton.textContent = copied ? 'Copied' : 'Copy failed';
          setTimeout(() => {
            copyAllButton.textContent = originalText;
          }, 1200);
        });
      }

      setActiveFilter(activeFilter);
    },

    onBatchCaptured(batch, batchIndex, ctx) {
      if (!eventLog) return;
      for (const entry of batch) {
        if (excludedTypeSet.has(entry.eventType)) continue;
        if (entry.eventType === 'ReconfigurationRequested') {
          replayCheckpointSlot = null;
        } else if (entry.eventType === 'ReconfigurationCheckpointSelected') {
          const slot = entry.eventData?.last_applied_slot;
          replayCheckpointSlot = Number.isInteger(slot) ? slot : null;
        }

        const viz = getEventVisualizer(entry.eventType);
        if (!viz || !viz.format) continue;
        const isReplay = detectReplay(entry.eventType, entry.eventData);
        const baseText = formatEventText(entry.eventType, entry.eventData, ctx);
        const text = isReplay ? `${baseText} [replay]` : baseText;
        if (!text || !text.trim()) continue;

        if (baseTimestamp === null && Number.isFinite(entry.created_at)) {
          baseTimestamp = entry.created_at;
        }

        typeCounts[entry.eventType] = (typeCounts[entry.eventType] || 0) + 1;

        const node = document.createElement('div');
        node.className = 'event-item';
        node.style.borderLeftColor = viz.color;

        const timeEl = document.createElement('span');
        timeEl.className = 'event-time';
        timeEl.textContent = formatRelativeTime(entry.created_at);

        const textEl = document.createElement('span');
        textEl.className = 'event-text';
        textEl.textContent = text;

        node.appendChild(timeEl);
        node.appendChild(textEl);
        eventLog.insertBefore(node, eventLog.firstChild);

        items.unshift({
          node,
          textEl,
          isReplay,
          frameIndex: batchIndex,
          eventType: entry.eventType,
          eventData: entry.eventData,
        });

        if (items.length > limit) {
          const removed = items.pop();
          if (removed?.node?.parentNode) {
            removed.node.parentNode.removeChild(removed.node);
          }
        }
      }
      updateFutureClasses();
      updateFilterButtons();
      updateVisibility();
    },

    onNodeLabelsChanged(_, ctx) {
      for (const item of items) {
        const baseText = formatEventText(item.eventType, item.eventData, ctx);
        item.textEl.textContent = item.isReplay ? `${baseText} [replay]` : baseText;
      }
      updateVisibility();
      scrollToActive();
    },

    onRestore({ targetIndex }) {
      if (!eventLog) return;
      cursor = targetIndex;
      updateFutureClasses();
      scrollToActive();
    },

    onPlaybackUpdate(playbackState) {
      if (!eventLog) return;
      cursor = playbackState.cursor ?? cursor;
      updateFutureClasses();
      if (playbackState?.playerMode === 'follow') {
        if (scrollRaf !== null) {
          cancelAnimationFrame(scrollRaf);
          scrollRaf = null;
        }
        eventLog.scrollTop = 0;
        return;
      }
      scrollToActive();
    },

    onReset() {
      if (!eventLog) return;
      items.length = 0;
      cursor = -1;
      baseTimestamp = null;
      replayCheckpointSlot = null;
      for (const key of Object.keys(typeCounts)) {
        delete typeCounts[key];
      }
      eventLog.innerHTML = '';
      updateFilterButtons();
    },
  };
}
