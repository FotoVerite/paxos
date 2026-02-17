/**
 * Node ID Mapper
 * Maps UUID node IDs to stable numeric indexes and user-friendly labels.
 */

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const GREEK_LABELS = [
  'α',
  'β',
  'γ',
  'δ',
  'ε',
  'ζ',
  'η',
  'θ',
  'ι',
  'κ',
  'λ',
  'μ',
  'ν',
  'ξ',
  'ο',
  'π',
  'ρ',
  'σ',
  'τ',
  'υ',
  'φ',
  'χ',
  'ψ',
  'ω',
];

const ID_FIELDS = new Set([
  'id',
  'from',
  'to',
  'leader',
  'leader_id',
  'ballot_proposer',
  'node_id',
  'proposer',
]);

const ID_LIST_FIELDS = new Set([
  'quorum',
  'partition_a',
  'partition_b',
  'acceptors',
  'learners',
  'indexes',
]);

function isUuid(value) {
  return typeof value === 'string' && UUID_RE.test(value.trim());
}

function parseInteger(value) {
  if (typeof value === 'number' && Number.isInteger(value)) return value;
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (!/^-?\d+$/.test(trimmed)) return null;
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isInteger(parsed) ? parsed : null;
}

function greekLabel(index) {
  const base = GREEK_LABELS[index % GREEK_LABELS.length];
  const cycle = Math.floor(index / GREEK_LABELS.length);
  return cycle === 0 ? base : `${base}${cycle + 1}`;
}

export function createNodeIdMapper({
  mode = 'number',
  customLabelFormatter = null,
} = {}) {
  let labelMode = mode === 'greek' ? 'greek' : 'number';
  const uuidToIndex = new Map();
  const indexToUuid = [];

  function registerUuid(uuid, preferredIndex = null) {
    if (!isUuid(uuid)) return null;
    if (uuidToIndex.has(uuid)) {
      return uuidToIndex.get(uuid);
    }

    let index =
      Number.isInteger(preferredIndex) && preferredIndex >= 0
        ? preferredIndex
        : indexToUuid.length;
    while (indexToUuid[index] !== undefined) {
      index += 1;
    }

    uuidToIndex.set(uuid, index);
    indexToUuid[index] = uuid;
    return index;
  }

  function ingestClusterInfo(clusterInfo) {
    uuidToIndex.clear();
    indexToUuid.length = 0;
    const nodeUuids = Array.isArray(clusterInfo?.node_uuids)
      ? clusterInfo.node_uuids
      : [];
    nodeUuids.forEach((uuid, index) => registerUuid(uuid, index));
  }

  function toIndex(value) {
    if (value === null || value === undefined) return value;

    const parsed = parseInteger(value);
    if (parsed !== null) return parsed;

    if (isUuid(value)) {
      return registerUuid(value);
    }

    return value;
  }

  function mapNodeIdFields(value, parentKey = null) {
    if (Array.isArray(value)) {
      if (ID_LIST_FIELDS.has(parentKey)) {
        return value.map((item) => toIndex(item));
      }
      return value.map((item) => mapNodeIdFields(item));
    }

    if (value && typeof value === 'object') {
      const mapped = {};
      for (const [key, child] of Object.entries(value)) {
        if (ID_FIELDS.has(key)) {
          mapped[key] = toIndex(child);
          continue;
        }
        if (ID_LIST_FIELDS.has(key) && Array.isArray(child)) {
          mapped[key] = child.map((item) => toIndex(item));
          continue;
        }
        mapped[key] = mapNodeIdFields(child, key);
      }
      return mapped;
    }

    return value;
  }

  function mapEventData(_eventType, eventData) {
    return mapNodeIdFields(eventData);
  }

  function mapMessagePayload(payload) {
    if (!payload || typeof payload !== 'object') return payload;
    const mapped = mapNodeIdFields(payload);

    if (mapped?.message && typeof mapped.message === 'object') {
      const [messageType] = Object.keys(mapped.message);
      if (messageType && mapped.message[messageType]) {
        mapped.message[messageType] = mapEventData(
          messageType,
          mapped.message[messageType]
        );
      }
    }

    return mapped;
  }

  function setMode(nextMode) {
    labelMode = nextMode === 'greek' ? 'greek' : 'number';
  }

  function getMode() {
    return labelMode;
  }

  function toLabel(value) {
    const index = toIndex(value);
    if (!Number.isInteger(index) || index < 0) return String(value);

    const uuid = indexToUuid[index] ?? null;
    if (typeof customLabelFormatter === 'function') {
      const custom = customLabelFormatter(index, { uuid, mode: labelMode });
      if (custom !== undefined && custom !== null && custom !== '') {
        return String(custom);
      }
    }

    if (labelMode === 'greek') {
      return greekLabel(index);
    }
    return String(index);
  }

  function toNodeText(value) {
    return `Node ${toLabel(value)}`;
  }

  return {
    ingestClusterInfo,
    mapEventData,
    mapMessagePayload,
    toIndex,
    toLabel,
    toNodeText,
    setMode,
    getMode,
  };
}
