import { nodeText } from '/js/paxos-demo/visualizers/shared.js';

const STRATEGY_SUMMARY = {
  JointConsensus: 'baseline handoff with an immediate stop barrier.',
  StopSign: 'immediate stop barrier once STOP is chosen.',
  DelayedStopSign: 'stop barrier after the delayed grace window.',
  Padding: 'post-stop client proposals are rewritten to NOOP.',
  BrickWall: 'post-stop decisions are rejected by the wall.',
};

function strategyLabel(strategy) {
  return typeof strategy === 'string' ? strategy : 'UnknownStrategy';
}

function commandLabel(cmd) {
  if (!cmd) return 'none';
  if (typeof cmd === 'string') return cmd;
  if (cmd.ClientRequest && typeof cmd.ClientRequest === 'object') {
    return commandLabel(cmd.ClientRequest.cmd);
  }
  if (typeof cmd !== 'object') return String(cmd);
  const [kind] = Object.keys(cmd);
  return kind || 'unknown';
}

function phaseTitle(summaryTitle, strategy) {
  const label = strategyLabel(strategy);
  return `${summaryTitle} (${label})`;
}

function hasStopBoundary(event) {
  return event.stop_slot !== null && event.stop_slot !== undefined;
}

function isMeaningfulProposalGateEvent(event) {
  const original = commandLabel(event.original_cmd);
  const effective = commandLabel(event.effective_cmd);
  if (hasStopBoundary(event)) return true;
  if (event.outcome !== 'admitted') return true;
  if (event.barrier_active) return true;
  return original !== effective;
}

export function createReconfigPhasePlugin({ titleEl, descriptionEl } = {}) {
  const stopTargets = new Set();
  const retiredNodes = new Set();
  const rebootedNodes = new Set();
  let reconfigurationInFlight = false;
  let requestedStrategy = null;

  function displayStrategy(eventStrategy) {
    return requestedStrategy || strategyLabel(eventStrategy);
  }

  function update(title, description, color) {
    if (!titleEl || !descriptionEl) return;
    titleEl.textContent = title;
    descriptionEl.textContent = description;
    titleEl.style.color = color;
  }

  return {
    onReset() {
      stopTargets.clear();
      retiredNodes.clear();
      rebootedNodes.clear();
      reconfigurationInFlight = false;
      requestedStrategy = null;
      update(
        'Reconfiguration Idle',
        'Run a reconfiguration scenario to visualize stop boundaries and reboot behavior.',
        '#60a5fa'
      );
    },

    onEvent({ eventType, eventData }, ctx) {
      if (!titleEl || !descriptionEl || !eventData) return;

      switch (eventType) {
        case 'ReconfigurationRequested': {
          reconfigurationInFlight = true;
          const strategy = strategyLabel(eventData.strategy);
          requestedStrategy = strategy;
          const summary = STRATEGY_SUMMARY[strategy] || 'reconfiguration in progress.';
          update(
            phaseTitle('Reconfiguration Requested', strategy),
            `Add=${eventData.add_nodes?.length ?? 0}, remove=${eventData.remove_nodes?.length ?? 0}; ${summary}`,
            '#fbbf24'
          );
          break;
        }

        case 'ReconfigurationStopStarted': {
          const strategy = displayStrategy(eventData.strategy);
          update(
            phaseTitle('Stopping Current Runtime', strategy),
            `Sending stop to ${eventData.target_nodes?.length ?? 0} nodes.`,
            '#f97316'
          );
          break;
        }

        case 'ReconfigurationStopCommandSent': {
          if (eventData.to !== null && eventData.to !== undefined) {
            stopTargets.add(eventData.to);
          }
          update(
            'Stop Commands In Flight',
            `Stop sent to ${stopTargets.size} node(s); waiting for chosen/applied stop boundaries.`,
            '#f59e0b'
          );
          break;
        }

        case 'ReconfigurationStopDecided': {
          const strategy = displayStrategy(eventData.strategy);
          const delayed = Number(eventData.delayed_slots || 0);
          const delayLabel = delayed > 0 ? ` (+${delayed} delayed slots)` : '';
          update(
            phaseTitle('Stop Boundary Decided', strategy),
            `${ctx.nodeLabels.node(eventData.id)} decided STOP at slot ${eventData.slot}${delayLabel}.`,
            '#f59e0b'
          );
          break;
        }

        case 'ReconfigurationStopApplied': {
          const strategy = displayStrategy(eventData.strategy);
          const delayed = Number(eventData.delayed_slots || 0);
          const delayLabel = delayed > 0 ? ` (delay window ${delayed})` : '';
          update(
            phaseTitle('Stop Boundary Applied', strategy),
            `${ctx.nodeLabels.node(eventData.id)} applied STOP at slot ${eventData.slot}${delayLabel}.`,
            '#ea580c'
          );
          break;
        }

        case 'ReconfigurationProposalObserved': {
          if (!reconfigurationInFlight || !isMeaningfulProposalGateEvent(eventData)) {
            break;
          }
          const strategy = displayStrategy(eventData.strategy);
          const original = commandLabel(eventData.original_cmd);
          const effective = commandLabel(eventData.effective_cmd);
          const stopSlot =
            eventData.stop_slot === null || eventData.stop_slot === undefined
              ? '-'
              : eventData.stop_slot;
          const slot =
            eventData.slot === null || eventData.slot === undefined ? '-' : eventData.slot;

          let description = `slot=${slot}, stop_slot=${stopSlot}, outcome=${eventData.outcome}, cmd=${original}->${effective}.`;
          let color = '#38bdf8';

          if (
            eventData.outcome === 'admitted' &&
            strategy === 'Padding' &&
            original !== 'NOOP' &&
            effective === 'NOOP'
          ) {
            description = `Padding rewrote client proposal ${original} to NOOP at slot ${slot}.`;
            color = '#06b6d4';
          } else if (eventData.outcome === 'stopped' && strategy === 'BrickWall') {
            description = `BrickWall rejected proposal after stop boundary (slot=${slot}, stop_slot=${stopSlot}).`;
            color = '#ef4444';
          } else if (
            eventData.outcome === 'admitted' &&
            strategy === 'DelayedStopSign' &&
            !eventData.barrier_active
          ) {
            description = `DelayedStopSign grace window accepted proposal at slot ${slot} before barrier activation.`;
            color = '#a78bfa';
          }

          update(phaseTitle('Proposal Gate', strategy), description, color);
          break;
        }

        case 'ReconfigurationStopCompleted': {
          const strategy = displayStrategy(eventData.strategy);
          update(
            phaseTitle('Current Runtime Halted', strategy),
            `${eventData.stopped_nodes?.length ?? 0} node(s) reported stopped.`,
            '#f59e0b'
          );
          break;
        }

        case 'ReconfigurationCheckpointSelected': {
          const strategy = displayStrategy(eventData.strategy);
          const source =
            eventData.source_node === null || eventData.source_node === undefined
              ? 'unknown'
              : nodeText(ctx.nodeLabels, eventData.source_node);
          const slot =
            eventData.last_applied_slot === null || eventData.last_applied_slot === undefined
              ? 'none'
              : eventData.last_applied_slot;
          update(
            phaseTitle('Checkpoint Selected', strategy),
            `source=${source}, last_applied_slot=${slot}.`,
            '#38bdf8'
          );
          break;
        }

        case 'ReconfigurationApplied': {
          const strategy = displayStrategy(eventData.strategy);
          update(
            phaseTitle('Booting New Runtime', strategy),
            `Node count ${eventData.previous_node_count} -> ${eventData.next_node_count}.`,
            '#22c55e'
          );
          break;
        }

        case 'ReconfigurationNodeRetired': {
          retiredNodes.add(eventData.id);
          update(
            'Retiring Previous Runtime',
            `Retired nodes: ${retiredNodes.size}.`,
            '#ef4444'
          );
          break;
        }

        case 'ReconfigurationNodeRebooted': {
          rebootedNodes.add(eventData.id);
          update(
            'Starting New Runtime',
            `Rebooted nodes: ${rebootedNodes.size}.`,
            '#22c55e'
          );
          break;
        }

        case 'ReconfigurationReady': {
          reconfigurationInFlight = false;
          const strategy = displayStrategy(eventData.strategy);
          const leader =
            eventData.leader === null || eventData.leader === undefined
              ? 'none'
              : nodeText(ctx.nodeLabels, eventData.leader);
          update(
            phaseTitle('Reconfiguration Complete', strategy),
            `leader=${leader}, active_nodes=${eventData.active_nodes?.length ?? 0}.`,
            '#10b981'
          );
          requestedStrategy = null;
          break;
        }

        case 'ReconfigurationFailed': {
          reconfigurationInFlight = false;
          const strategy = displayStrategy(eventData.strategy);
          update(
            phaseTitle('Reconfiguration Failed', strategy),
            `${eventData.phase}: ${eventData.reason}`,
            '#ef4444'
          );
          requestedStrategy = null;
          break;
        }

        default:
          break;
      }
    },
  };
}
