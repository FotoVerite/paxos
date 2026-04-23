import { state } from '/demo-state.js';
import { createPaxosDemoController } from '/js/paxos-demo/core/runtime.js';
import { createEventLogPlugin } from '/js/paxos-demo/plugins/event-log.js';
import { createVisualizeEventsPlugin } from '/js/paxos-demo/plugins/visualize-events.js';
import { createNodeCapabilitiesPlugin } from '/js/paxos-demo/plugins/node-capabilities.js';
import { createVerticalMessagePlugin } from '/js/paxos-demo/plugins/vertical-messages.js';
import { createDecreePanelPlugin } from '/js/paxos-demo/plugins/decree-panel.js';
import { createPlaybackDelayPlugin } from '/js/paxos-demo/plugins/playback-delay.js';

const FRAME_WINDOW_MICROS = 12;
const VERTICAL_SCENARIOS = {
  reconfiguration_only: {
    title: 'Reconfiguration',
    description: 'One clean handoff from configuration A to B.',
    initialHint: 'Run the walkthrough. Watch configuration A become active, then watch B take over cleanly.',
  },
  activation_only: {
    title: 'Activation only',
    description: 'Show that installed does not mean active yet.',
    initialHint: 'Run the walkthrough. The whole point here is the gap between install and active.',
  },
  redirect_after_handoff: {
    title: 'Redirect after handoff',
    description: 'Old replicas reject new work once the successor takes over.',
    initialHint: 'Run the walkthrough. The key moment is the stale replica redirecting the client.',
  },
  vp1_recovery_cost: {
    title: 'VP1 recovery cost',
    description: 'Build history, then show the successor recovering from the full prefix again.',
    initialHint: 'Run the walkthrough. Watch how much work the successor has to do before it can become active.',
  },
  vp2_bounded_recovery: {
    title: 'VP2 bounded recovery',
    description: 'Show the same handoff with a bounded recovery window instead of replaying the full prefix.',
    initialHint: 'Run the walkthrough. The point is that the successor should have less recovery work to do.',
  },
};
const VERTICAL_LOG_PRESETS = [
  { key: 'all', label: 'All', types: null },
  {
    key: 'activation',
    label: 'Activation',
    types: [
      'VerticalConfigurationInstalled',
      'VerticalActivationStarted',
      'VerticalActivationReady',
      'VerticalActivationSuperseded',
      'VerticalReplicaSetActivated',
      'VerticalReplicaSetDecommissioned',
    ],
  },
  {
    key: 'replica',
    label: 'Replica',
    types: ['VerticalReplicaRedirected', 'VerticalReplicaApplied'],
  },
];

let controller = null;
let visualizer = null;
let runBtn;
let resetBtn;
let scenarioButtons = [];
let scenarioDescription;
let pauseBtn;
let stepBackBtn;
let stepForwardBtn;
let playbackCursor;
let phaseChip;
let watchHint;
let eventLog;
let filterBar;
let filterChips;
let filterToggle;
let filterClose;
let filterStatus;
let copyAllBtn;
let statsContainer;
let decreePanel;
let configPrimaryCard;
let configPrimaryState;
let configPrimaryTitle;
let configPrimaryLeader;
let configPrimaryReplicas;
let configPrimaryAcceptors;
let configSecondaryCard;
let configSecondaryState;
let configSecondaryTitle;
let configSecondaryLeader;
let configSecondaryReplicas;
let configSecondaryAcceptors;
let selectedScenario = 'reconfiguration_only';

const configurationSlots = new Map();

function phaseLabel(value) {
  return typeof value === 'string' && value ? value : 'Waiting';
}

function shortId(value) {
  if (typeof value !== 'string') return 'unknown';
  return value.slice(0, 8);
}

function nodeText(value) {
  return value === null || value === undefined ? '--' : `Node ${value}`;
}

function nodeList(values) {
  if (!Array.isArray(values) || values.length === 0) return '--';
  return values.map((value) => nodeText(value)).join(', ');
}

function cardRefs(slot) {
  return slot === 'primary'
    ? {
        card: configPrimaryCard,
        state: configPrimaryState,
        title: configPrimaryTitle,
        leader: configPrimaryLeader,
        replicas: configPrimaryReplicas,
        acceptors: configPrimaryAcceptors,
      }
    : {
        card: configSecondaryCard,
        state: configSecondaryState,
        title: configSecondaryTitle,
        leader: configSecondaryLeader,
        replicas: configSecondaryReplicas,
        acceptors: configSecondaryAcceptors,
      };
}

function resolveConfigurationSlot(configurationId) {
  if (configurationSlots.has(configurationId)) {
    return configurationSlots.get(configurationId);
  }
  const slot = configurationSlots.size === 0 ? 'primary' : 'secondary';
  configurationSlots.set(configurationId, slot);
  return slot;
}

function setPhaseChip(value) {
  if (!phaseChip) return;
  const label = phaseLabel(value);
  phaseChip.textContent = label;
  phaseChip.dataset.phase = label.toLowerCase();
}

function currentScenarioMeta() {
  return VERTICAL_SCENARIOS[selectedScenario] || VERTICAL_SCENARIOS.reconfiguration_only;
}

function syncScenarioUi() {
  const meta = currentScenarioMeta();
  for (const button of scenarioButtons) {
    button.classList.toggle('is-active', button.dataset.scenario === selectedScenario);
  }
  if (scenarioDescription) {
    scenarioDescription.textContent = meta.description;
  }
  if (watchHint) {
    watchHint.textContent = meta.initialHint;
  }
}

function setSelectedScenario(nextScenario) {
  if (!VERTICAL_SCENARIOS[nextScenario]) return;
  selectedScenario = nextScenario;
  syncScenarioUi();
}

function updateConfigurationCard(slot, eventData, stateLabel) {
  const refs = cardRefs(slot);
  if (!refs.state) return;
  refs.state.textContent = stateLabel;
  if (refs.card) {
    refs.card.dataset.state = stateLabel.toLowerCase();
  }
  if (refs.title) {
    const base = slot === 'primary' ? 'Configuration A' : 'Configuration B';
    const suffix = eventData?.configuration_id ? ` · ${shortId(eventData.configuration_id)}` : '';
    refs.title.textContent = `${base}${suffix}`;
  }
  if (eventData?.leader !== undefined) {
    refs.leader.textContent = nodeText(eventData.leader);
  }
  if (eventData?.replicas !== undefined) {
    refs.replicas.textContent = nodeList(eventData.replicas);
  }
  if (eventData?.acceptors !== undefined) {
    refs.acceptors.textContent = nodeList(eventData.acceptors);
  }
}

function resetConfigurationBoard() {
  configurationSlots.clear();
  const defaults = {
    primary: {
      card: configPrimaryCard,
      state: configPrimaryState,
      title: configPrimaryTitle,
      leader: configPrimaryLeader,
      replicas: configPrimaryReplicas,
      acceptors: configPrimaryAcceptors,
      stateLabel: 'Not installed',
      titleLabel: 'Configuration A',
    },
    secondary: {
      card: configSecondaryCard,
      state: configSecondaryState,
      title: configSecondaryTitle,
      leader: configSecondaryLeader,
      replicas: configSecondaryReplicas,
      acceptors: configSecondaryAcceptors,
      stateLabel: 'Queued',
      titleLabel: 'Configuration B',
    },
  };

  for (const slot of Object.values(defaults)) {
    if (slot.card) {
      slot.card.dataset.state = 'idle';
    }
    if (slot.state) slot.state.textContent = slot.stateLabel;
    if (slot.title) slot.title.textContent = slot.titleLabel;
    if (slot.leader) slot.leader.textContent = '--';
    if (slot.replicas) slot.replicas.textContent = '--';
    if (slot.acceptors) slot.acceptors.textContent = '--';
  }
}

function canCommunicate(from, to) {
  const snapshot = state.snapshot();
  const fromNode = snapshot.nodes.get(from);
  const toNode = snapshot.nodes.get(to);
  if (!fromNode || !toNode) return true;
  return fromNode.partitioned === toNode.partitioned;
}

function setStatus(_eyebrow, _title, _text) {}

function buildController() {
  const configurationBoardPlugin = {
    onEvent({ eventType, eventData }) {
      switch (eventType) {
        case 'VerticalConfigurationInstalled': {
          const slot = resolveConfigurationSlot(eventData.configuration_id);
          updateConfigurationCard(slot, eventData, 'Installed');
          break;
        }
        case 'VerticalActivationStarted': {
          const slot = resolveConfigurationSlot(eventData.configuration_id);
          updateConfigurationCard(slot, eventData, 'Synchronizing');
          setPhaseChip('Synchronizing');
          break;
        }
        case 'VerticalActivationReady': {
          const slot = resolveConfigurationSlot(eventData.configuration_id);
          updateConfigurationCard(slot, eventData, 'Active');
          setPhaseChip('Active');
          break;
        }
        case 'VerticalActivationSuperseded': {
          const slot = resolveConfigurationSlot(eventData.configuration_id);
          updateConfigurationCard(slot, eventData, 'Superseded');
          setPhaseChip('Superseded');
          break;
        }
        case 'VerticalReplicaSetDecommissioned': {
          const slot = resolveConfigurationSlot(eventData.configuration_id);
          updateConfigurationCard(slot, eventData, 'Redirecting');
          setPhaseChip('Redirect');
          break;
        }
        default:
          break;
      }
    },
  };

  const clusterRenderPlugin = {
    onCluster(clusterInfo, ctx) {
      ctx.visualizer.render(clusterInfo);
      if (ctx.state.snapshot().simulation.selectedNode === null) {
        ctx.state.selectNode(0);
      }
    },
  };

  const statusPlugin = {
    onEvent({ eventType, eventData }) {
      switch (eventType) {
        case 'VerticalConfigurationInstalled':
          setStatus(
            'Installed',
            'Configuration loaded',
            'The master wrote a configuration. Nothing is live yet.'
          );
          if (watchHint) {
            watchHint.textContent =
              'Watch configuration A. The leader still has to prove safety before replicas can accept work.';
          }
          break;
        case 'VerticalActivationStarted':
          setStatus(
            'Synchronizing',
            'Leader is proving it is safe',
            'The designated leader is collecting predecessor state before the replica set can take work.'
          );
          if (watchHint) {
            watchHint.textContent =
              'Stay on the leader. This is the quiet part: history sync, not client work.';
          }
          break;
        case 'VerticalActivationReady':
          setStatus(
            'Active',
            'Replica set is now live',
            'Clients can enter through the active replicas now.'
          );
          if (watchHint) {
            watchHint.textContent =
              'Configuration A is active. Now watch a replica feed the leader.';
          }
          break;
        case 'VerticalReplicaRedirected':
          setStatus(
            'Redirect',
            'Old replica is refusing new work',
            'That replica belongs to the old configuration now. It points the client at the active set.'
          );
          if (watchHint) {
            watchHint.textContent =
              'This is the handoff. The old replica knows it is stale and redirects the client.';
          }
          break;
        case 'VerticalReplicaApplied':
          setStatus(
            'Applied',
            `Replica applied slot ${eventData.slot}`,
            'A decided value reached the replica state machine and applied in slot order.'
          );
          if (watchHint) {
            watchHint.textContent =
              'Use the replica ledger now. That is the proof the protocol finished.';
          }
          break;
        default:
          break;
      }
    },
  };

  controller = createPaxosDemoController({
    state,
    visualizer,
    canCommunicate,
    frameWindowMicros: FRAME_WINDOW_MICROS,
    nodeLabelMode: 'greek',
    onPlaybackUpdate: updatePlaybackControls,
    plugins: [
      configurationBoardPlugin,
      clusterRenderPlugin,
      statusPlugin,
      createEventLogPlugin({
        eventLog,
        limit: 300,
        presets: VERTICAL_LOG_PRESETS,
        defaultFilter: 'activation',
        excludeTypes: ['NodeCapabilities'],
        filterBar,
        filterChips,
        filterToggle,
        filterClose,
        filterStatus,
        copyAllButton: copyAllBtn,
      }),
      createVisualizeEventsPlugin(),
      createNodeCapabilitiesPlugin(),
      createVerticalMessagePlugin(),
      createDecreePanelPlugin({
        statsContainer,
        decreePanel,
        nodeFilter: (node, _nodeId, snapshot) => {
          const roles = node?.role?.roles || [];
          return roles.includes('Replica');
        },
        itemLabelSingular: 'command',
        itemLabelPlural: 'commands',
        itemRenderLabel: 'slot',
        emptySelectionHint: 'Select a replica to inspect the commands it has applied.',
      }),
      createPlaybackDelayPlugin({ minDelay: 320, baseDelay: 760 }),
    ],
  });
}

function updatePlaybackControls(playbackState) {
  if (!playbackState) return;
  const { batchCount, cursor, playerMode, isBusy } = playbackState;
  if (pauseBtn) {
    pauseBtn.textContent = playerMode === 'follow' ? 'Pause' : 'Resume';
    pauseBtn.disabled = batchCount === 0 && playerMode === 'follow';
  }
  if (stepBackBtn) {
    stepBackBtn.disabled = playerMode === 'follow' || isBusy || cursor < 0;
  }
  if (stepForwardBtn) {
    stepForwardBtn.disabled = playerMode === 'follow' || isBusy || cursor + 1 >= batchCount;
  }
  if (playbackCursor) {
    playbackCursor.textContent = `Batch ${Math.max(0, cursor + 1)}/${batchCount}`;
  }
}

async function runDemo() {
  resetConfigurationBoard();
  controller.invalidate({ clearHistory: true });
  controller.pauseIngestion({ untilClusterInitialized: true });
  controller.setCaptureEnabled(true);
  await controller.resume();

  const response = await fetch(`/api/vertical/run?scenario=${encodeURIComponent(selectedScenario)}`, {
    method: 'POST',
  });
  if (!response.ok) {
    throw new Error(await response.text());
  }

  const meta = currentScenarioMeta();
  setStatus('Queued', 'Demo run started', meta.description);
}

async function resetDemo() {
  resetConfigurationBoard();
  controller.pause({ waitForFrame: false });
  controller.setCaptureEnabled(false);
  controller.pauseIngestion({ untilClusterInitialized: true });
  controller.reset({ clearHistory: true });
  state.resetSimulation();

  const response = await fetch('/api/vertical/reset', { method: 'POST' });
  if (!response.ok) {
    throw new Error(await response.text());
  }

  setStatus(
    'Idle',
    'Ready for a new run',
    'Watch configuration ownership move, then watch the replicas obey it.'
  );
  syncScenarioUi();
  setPhaseChip('Waiting');
}

async function togglePause() {
  const playbackState = controller.getPlaybackState();
  if (playbackState.playerMode === 'follow') {
    await controller.pause();
  } else {
    await controller.resume();
  }
}

function wireControls() {
  for (const button of scenarioButtons) {
    button.addEventListener('click', () => {
      if (runBtn?.disabled || resetBtn?.disabled) return;
      setSelectedScenario(button.dataset.scenario);
    });
  }

  runBtn?.addEventListener('click', async () => {
    runBtn.disabled = true;
    try {
      await runDemo();
    } catch (error) {
      console.error('Failed to start vertical demo:', error);
      setStatus('Error', 'Run failed', String(error));
    } finally {
      runBtn.disabled = false;
    }
  });

  resetBtn?.addEventListener('click', async () => {
    resetBtn.disabled = true;
    try {
      await resetDemo();
    } catch (error) {
      console.error('Failed to reset vertical demo:', error);
      setStatus('Error', 'Reset failed', String(error));
    } finally {
      resetBtn.disabled = false;
    }
  });

  pauseBtn?.addEventListener('click', async () => {
    try {
      await togglePause();
    } catch (error) {
      console.error('Failed to toggle playback:', error);
    }
  });

  stepBackBtn?.addEventListener('click', async () => {
    await controller.stepBack();
  });

  stepForwardBtn?.addEventListener('click', async () => {
    await controller.stepForward();
  });
}

function init() {
  runBtn = document.getElementById('verticalRunBtn');
  resetBtn = document.getElementById('verticalResetBtn');
  scenarioButtons = Array.from(document.querySelectorAll('[data-scenario]'));
  scenarioDescription = document.getElementById('verticalScenarioDescription');
  pauseBtn = document.getElementById('verticalPauseBtn');
  stepBackBtn = document.getElementById('verticalStepBackBtn');
  stepForwardBtn = document.getElementById('verticalStepForwardBtn');
  playbackCursor = document.getElementById('verticalPlaybackCursor');
  eventLog = document.getElementById('verticalEventLog');
  filterBar = document.getElementById('verticalEventFilterBar');
  filterChips = document.getElementById('verticalEventFilterChips');
  filterToggle = document.getElementById('verticalEventFilterToggle');
  filterClose = document.getElementById('verticalEventFilterClose');
  filterStatus = document.getElementById('verticalEventFilterStatus');
  copyAllBtn = document.getElementById('verticalEventCopyBtn');
  statsContainer = document.getElementById('verticalReplicaStats');
  decreePanel = document.getElementById('verticalReplicaPanel');
  phaseChip = document.getElementById('verticalPhaseChip');
  watchHint = document.getElementById('verticalWatchHint');
  configPrimaryCard = document.getElementById('verticalConfigPrimary');
  configPrimaryState = document.getElementById('verticalConfigPrimaryState');
  configPrimaryTitle = document.getElementById('verticalConfigPrimaryTitle');
  configPrimaryLeader = document.getElementById('verticalConfigPrimaryLeader');
  configPrimaryReplicas = document.getElementById('verticalConfigPrimaryReplicas');
  configPrimaryAcceptors = document.getElementById('verticalConfigPrimaryAcceptors');
  configSecondaryCard = document.getElementById('verticalConfigSecondary');
  configSecondaryState = document.getElementById('verticalConfigSecondaryState');
  configSecondaryTitle = document.getElementById('verticalConfigSecondaryTitle');
  configSecondaryLeader = document.getElementById('verticalConfigSecondaryLeader');
  configSecondaryReplicas = document.getElementById('verticalConfigSecondaryReplicas');
  configSecondaryAcceptors = document.getElementById('verticalConfigSecondaryAcceptors');
  const params = new URLSearchParams(window.location.search);
  setSelectedScenario(params.get('scenario') || selectedScenario);

  visualizer = new PaxosVisualizer('verticalDemoSvg', {
    layoutMode: 'circle',
    useRoleShapes: true,
    topologyGradientColor: '#0f766e',
    nodeRadius: window.innerWidth < 900 ? 120 : 190,
    nodeCircleRadius: 22,
    centerYOffset: 6,
  });

  buildController();
  controller.connect();
  updatePlaybackControls(controller.getPlaybackState());
  wireControls();
  setStatus(
    'Idle',
    'Ready for a new run',
    'Watch configuration ownership move, then watch the replicas obey it.'
  );
  resetConfigurationBoard();
  setPhaseChip('Waiting');
  syncScenarioUi();
}

window.addEventListener('DOMContentLoaded', init);
