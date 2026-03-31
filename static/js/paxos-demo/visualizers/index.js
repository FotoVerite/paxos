import { CLASSIC_EVENT_VISUALIZERS } from '/js/paxos-demo/visualizers/classic.js';
import { PMMC_EVENT_VISUALIZERS } from '/js/paxos-demo/visualizers/pmmc.js';
import { SYSTEM_EVENT_VISUALIZERS } from '/js/paxos-demo/visualizers/system.js';
import { VERTICAL_EVENT_VISUALIZERS } from '/js/paxos-demo/visualizers/vertical.js';

const REGISTRIES = {
  classic: CLASSIC_EVENT_VISUALIZERS,
  system: SYSTEM_EVENT_VISUALIZERS,
  pmmc: PMMC_EVENT_VISUALIZERS,
  vertical: VERTICAL_EVENT_VISUALIZERS
};

const PROTOCOL_ORDER = ['classic', 'system', 'pmmc', 'vertical'];

function mergeRegistries() {
  const merged = {};
  for (const protocol of PROTOCOL_ORDER) {
    const registry = REGISTRIES[protocol];
    for (const [eventType, visualizer] of Object.entries(registry)) {
      if (Object.prototype.hasOwnProperty.call(merged, eventType)) {
        throw new Error(
          `Duplicate event visualizer key '${eventType}' across protocol registries`
        );
      }
      merged[eventType] = visualizer;
    }
  }
  return Object.freeze(merged);
}

export const EVENT_VISUALIZERS = mergeRegistries();

export function getSupportedProtocols() {
  return [...PROTOCOL_ORDER];
}

export function getEventVisualizerRegistry(protocol) {
  if (!protocol) {
    return EVENT_VISUALIZERS;
  }
  return REGISTRIES[protocol] || null;
}

export function resolveVisualizerProtocol(eventType) {
  if (Object.prototype.hasOwnProperty.call(CLASSIC_EVENT_VISUALIZERS, eventType)) {
    return 'classic';
  }
  if (Object.prototype.hasOwnProperty.call(SYSTEM_EVENT_VISUALIZERS, eventType)) {
    return 'system';
  }
  if (Object.prototype.hasOwnProperty.call(PMMC_EVENT_VISUALIZERS, eventType)) {
    return 'pmmc';
  }
  if (Object.prototype.hasOwnProperty.call(VERTICAL_EVENT_VISUALIZERS, eventType)) {
    return 'vertical';
  }
  return null;
}

export function getEventVisualizer(eventType) {
  const protocol = resolveVisualizerProtocol(eventType);
  if (!protocol) return null;
  return REGISTRIES[protocol][eventType] || null;
}

export function getSupportedEventTypes(protocol) {
  if (protocol) {
    const registry = getEventVisualizerRegistry(protocol);
    return registry ? Object.keys(registry) : [];
  }
  return Object.keys(EVENT_VISUALIZERS);
}
