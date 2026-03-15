function commandLabel(cmd) {
  if (!cmd) return 'none';
  if (typeof cmd === 'string') return cmd;
  if (cmd.ClientRequest && typeof cmd.ClientRequest === 'object') {
    return commandLabel(cmd.ClientRequest.cmd);
  }
  if (typeof cmd !== 'object') return String(cmd);
  const [kind] = Object.keys(cmd);
  return kind || 'cmd';
}

function commandFingerprint(cmd) {
  if (cmd === null || cmd === undefined) return 'none';
  if (typeof cmd === 'string') return `s:${cmd}`;
  if (typeof cmd === 'number' || typeof cmd === 'boolean') return `${typeof cmd}:${cmd}`;
  try {
    return `j:${JSON.stringify(cmd)}`;
  } catch {
    return `f:${commandLabel(cmd)}`;
  }
}

function isNoopCommand(cmd) {
  return commandLabel(cmd) === 'NOOP';
}

function inferEpochFromEvent(eventType, eventData) {
  if (!eventData || typeof eventData !== 'object') return null;
  if (
    eventType === 'PmmcP2B' ||
    eventType === 'PmmcP1B' ||
    eventType === 'PmmcAdopted' ||
    eventType === 'PmmcPreempted' ||
    eventType === 'PmmcHeartbeat'
  ) {
    const epoch =
      eventData?.ballot?.epoch ??
      eventData?.pvalue?.ballot?.epoch;
    return Number.isInteger(epoch) ? epoch : null;
  }
  return null;
}

function findEpochForExistingSlot(slots, currentEpoch, slot) {
  for (let candidate = currentEpoch; candidate >= 1; candidate -= 1) {
    if (slots.has(`${candidate}:${slot}`)) {
      return candidate;
    }
  }
  return currentEpoch;
}

function findEpochForSlotAndCommand(slots, currentEpoch, slot, cmd) {
  if (!Number.isInteger(slot)) return currentEpoch;
  const cmdFp = commandFingerprint(cmd);
  for (let candidate = currentEpoch; candidate >= 1; candidate -= 1) {
    const record = slots.get(`${candidate}:${slot}`);
    if (record && record.cmdFingerprint === cmdFp) {
      return candidate;
    }
  }
  return findEpochForExistingSlot(slots, currentEpoch, slot);
}

function ensureSlotRecord(slots, epoch, slot) {
  if (!Number.isInteger(slot)) return null;
  const key = `${epoch}:${slot}`;
  if (!slots.has(key)) {
    slots.set(key, {
      key,
      epoch,
      slot,
      cmd: 'cmd',
      cmdFingerprint: 'none',
      proposed: false,
      learned: false,
      stopDecided: false,
      stopApplied: false,
      strategy: null,
      paddedNoop: false,
      p2bSources: new Set(),
      ackSources: new Set(),
      pulseUntil: 0,
      updatedAt: 0,
    });
  }
  return slots.get(key);
}

function touch(record) {
  record.updatedAt = Date.now();
  record.pulseUntil = Date.now() + 700;
}

function epochHue(epoch) {
  const palette = [212, 158, 24, 288, 352];
  return palette[(Math.max(1, epoch) - 1) % palette.length];
}

export function createSlotTapePlugin({ container, maxSlots = 16 } = {}) {
  const slots = new Map();
  let epoch = 1;
  const checkpointBoundaries = new Map();
  const slotEpochHints = new Map();

  function render() {
    if (!container) return;
    if (slots.size === 0) {
      container.innerHTML = '<div class="slot-tape-empty">No slot activity yet</div>';
      return;
    }

    const sorted = Array.from(slots.values())
      .sort((a, b) => {
        if (a.epoch !== b.epoch) {
          return b.epoch - a.epoch;
        }
        return b.slot - a.slot;
      })
      .slice(0, maxSlots);

    const rows = sorted
      .map((slot) => {
        const pulse = slot.pulseUntil > Date.now() ? ' pulse' : '';
        const checkpointBoundary = checkpointBoundaries.get(slot.epoch);
        const checkpointed =
          Number.isInteger(checkpointBoundary) && slot.slot <= checkpointBoundary;
        const checkpointBoundarySlot =
          Number.isInteger(checkpointBoundary) && slot.slot === checkpointBoundary;
        const stopBoundarySlot = slot.stopDecided || slot.stopApplied;
        const checkpointBadge = checkpointed
          ? '<span class="slot-chip checkpoint">checkpointed</span>'
          : '';
        const epochBadge = `<span class="slot-chip epoch">e${slot.epoch}</span>`;
        const strategyBadge = slot.strategy
          ? `<span class="slot-chip strategy">${slot.strategy}</span>`
          : '';
        const paddingBadge = slot.paddedNoop
          ? '<span class="slot-chip padding">padding NOOP</span>'
          : '';
        const stopBadge = slot.stopApplied
          ? '<span class="slot-chip stop hard">STOP applied</span>'
          : slot.stopDecided
            ? '<span class="slot-chip stop soft">STOP decided</span>'
            : '';
        const cardClasses = [
          'slot-tape-card',
          pulse.trim(),
          checkpointed ? 'checkpointed' : '',
          checkpointBoundarySlot ? 'checkpoint-boundary' : '',
          stopBoundarySlot ? 'stop-boundary' : '',
          slot.stopApplied ? 'stop-applied' : '',
          slot.stopDecided && !slot.stopApplied ? 'stop-decided' : '',
        ]
          .filter(Boolean)
          .join(' ');
        const cmd = slot.cmd || 'cmd';
        return `
          <div class="${cardClasses}" style="--slot-epoch-h:${epochHue(slot.epoch)};">
            <div class="slot-boundary-lines" aria-hidden="true">
              ${checkpointBoundarySlot ? '<span class="slot-boundary-line checkpoint" title="checkpoint boundary"></span>' : ''}
              ${stopBoundarySlot ? '<span class="slot-boundary-line stop" title="stop boundary"></span>' : ''}
            </div>
            <div class="slot-tape-slot">s${slot.slot}</div>
            <div class="slot-tape-body">
              <div class="slot-tape-head">
                <span class="slot-tape-cmd">${cmd}</span>
                <div class="slot-chip-group">
                  ${epochBadge}
                  ${checkpointBadge}
                  ${strategyBadge}
                  ${paddingBadge}
                  ${stopBadge}
                </div>
              </div>
              <div class="slot-markers" aria-hidden="true">
                <span class="slot-marker checkpoint ${checkpointed ? 'on' : ''}" title="checkpoint boundary"></span>
                <span class="slot-marker stop ${slot.stopDecided ? 'on' : ''}" title="stop decided"></span>
                <span class="slot-marker applied ${slot.stopApplied ? 'on' : ''}" title="stop applied"></span>
              </div>
              <div class="slot-stage-flow">
                <span class="slot-stage ${slot.proposed ? 'on' : 'off'}">propose</span>
                <span class="slot-arrow">→</span>
                <span class="slot-stage ${slot.p2bSources.size > 0 ? 'on' : 'off'}">accept</span>
                <span class="slot-arrow">→</span>
                <span class="slot-stage ${slot.learned ? 'on' : 'off'}">apply</span>
              </div>
              <div class="slot-tape-meta">
                p2b=${slot.p2bSources.size} · ack=${slot.ackSources.size}
              </div>
            </div>
          </div>
        `;
      })
      .join('');

    container.innerHTML = rows;
  }

  return {
    init() {
      render();
    },

    onReset() {
      slots.clear();
      epoch = 1;
      checkpointBoundaries.clear();
      slotEpochHints.clear();
      render();
    },

    onEvent({ eventType, eventData }) {
      if (!eventData) return;
      const eventEpoch = inferEpochFromEvent(eventType, eventData);
      if (Number.isInteger(eventEpoch)) {
        epoch = Math.max(epoch, eventEpoch);
      }

      if (eventType === 'ReconfigurationRequested') {
        checkpointBoundaries.delete(epoch);
      }

      if (eventType === 'ReconfigurationReady') {
        render();
        return;
      }

      if (eventType === 'ReconfigurationCheckpointSelected') {
        const slot = eventData.last_applied_slot;
        if (Number.isInteger(slot)) {
          checkpointBoundaries.set(epoch, slot);
        } else {
          checkpointBoundaries.delete(epoch);
        }
        render();
        return;
      }

      if (eventType === 'PmmcPropose') {
        const record = ensureSlotRecord(slots, epoch, eventData.slot);
        if (!record) return;
        record.proposed = true;
        record.cmd = commandLabel(eventData.cmd);
        record.cmdFingerprint = commandFingerprint(eventData.cmd);
        slotEpochHints.set(eventData.slot, record.epoch);
        touch(record);
        render();
        return;
      }

      if (eventType === 'ReconfigurationProposalObserved') {
        if (eventData.outcome !== 'admitted') return;
        const record = ensureSlotRecord(slots, epoch, eventData.slot);
        if (!record) return;
        const effective = commandLabel(eventData.effective_cmd);
        record.proposed = true;
        record.cmd = effective;
        record.cmdFingerprint = commandFingerprint(eventData.effective_cmd);
        record.strategy = eventData.strategy || record.strategy;
        slotEpochHints.set(eventData.slot, record.epoch);
        if (
          eventData.strategy === 'Padding' &&
          !isNoopCommand(eventData.original_cmd) &&
          isNoopCommand(eventData.effective_cmd)
        ) {
          record.paddedNoop = true;
        }
        touch(record);
        render();
        return;
      }

      if (eventType === 'PmmcP2B') {
        const slot = eventData?.pvalue?.slot;
        const hintedEpoch = slotEpochHints.get(slot);
        const recordEpoch = Number.isInteger(eventEpoch)
          ? eventEpoch
          : Number.isInteger(hintedEpoch)
            ? hintedEpoch
            : findEpochForExistingSlot(slots, epoch, slot);
        const record = ensureSlotRecord(slots, recordEpoch, slot);
        if (!record) return;
        if (eventData.from !== null && eventData.from !== undefined) {
          record.p2bSources.add(eventData.from);
        }
        if (eventData?.pvalue?.cmd !== undefined) {
          record.cmd = commandLabel(eventData.pvalue.cmd);
          record.cmdFingerprint = commandFingerprint(eventData.pvalue.cmd);
        }
        slotEpochHints.set(slot, record.epoch);
        touch(record);
        render();
        return;
      }

      if (eventType === 'PmmcAck') {
        const hintedEpoch = slotEpochHints.get(eventData.slot);
        const recordEpoch = Number.isInteger(hintedEpoch)
          ? hintedEpoch
          : findEpochForExistingSlot(slots, epoch, eventData.slot);
        const record = ensureSlotRecord(slots, recordEpoch, eventData.slot);
        if (!record) return;
        if (eventData.from !== null && eventData.from !== undefined) {
          record.ackSources.add(eventData.from);
        }
        touch(record);
        render();
        return;
      }

      if (eventType === 'LearnedValue') {
        const hintedEpoch = slotEpochHints.get(eventData.decree_num);
        const recordEpoch = Number.isInteger(hintedEpoch)
          ? findEpochForSlotAndCommand(slots, hintedEpoch, eventData.decree_num, eventData.value)
          : findEpochForSlotAndCommand(slots, epoch, eventData.decree_num, eventData.value);
        const record = ensureSlotRecord(slots, recordEpoch, eventData.decree_num);
        if (!record) return;
        record.learned = true;
        record.cmd = commandLabel(eventData.value);
        record.cmdFingerprint = commandFingerprint(eventData.value);
        slotEpochHints.set(eventData.decree_num, record.epoch);
        touch(record);
        render();
        return;
      }

      if (eventType === 'ReconfigurationStopDecided') {
        const hintedEpoch = slotEpochHints.get(eventData.slot);
        const recordEpoch = Number.isInteger(hintedEpoch)
          ? hintedEpoch
          : findEpochForExistingSlot(slots, epoch, eventData.slot);
        const record = ensureSlotRecord(slots, recordEpoch, eventData.slot);
        if (!record) return;
        record.stopDecided = true;
        record.strategy = eventData.strategy || null;
        slotEpochHints.set(eventData.slot, record.epoch);
        touch(record);
        render();
        return;
      }

      if (eventType === 'ReconfigurationStopApplied') {
        const hintedEpoch = slotEpochHints.get(eventData.slot);
        const recordEpoch = Number.isInteger(hintedEpoch)
          ? hintedEpoch
          : findEpochForExistingSlot(slots, epoch, eventData.slot);
        const record = ensureSlotRecord(slots, recordEpoch, eventData.slot);
        if (!record) return;
        record.stopApplied = true;
        record.stopDecided = true;
        record.strategy = eventData.strategy || record.strategy;
        slotEpochHints.set(eventData.slot, record.epoch);
        touch(record);
        render();
      }
    },
  };
}
