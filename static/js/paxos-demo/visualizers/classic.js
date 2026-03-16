import {
  drawBeamsTo,
  flashNodeWithMotion,
  formatDecree,
  getBeamDuration,
  getReachableTargets,
  nodeText,
  scheduleNodeReset
} from '/js/paxos-demo/visualizers/shared.js';

export const CLASSIC_EVENT_VISUALIZERS = {
  Proposal: {
    color: '#60a5fa',
    name: 'NextBallot',

    format(event, labels) {
      return `[NextBallot] ${nodeText(labels, event.id)}: "${formatDecree(event)}"`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.setNodeState(event.id, 'propose');
      flashNodeWithMotion(visualizer, event.id, this.color, { motion: 'proposal' });

      const snapshot = state.snapshot();
      const duration = getBeamDuration(snapshot, 500);

      const targets = getReachableTargets(
        event.id,
        snapshot.cluster?.total_nodes,
        canCommunicate
      );
      await drawBeamsTo(visualizer, event.id, targets, this.color, duration, 'solid', { motion: 'proposal' });

      scheduleNodeReset(visualizer, event.id, duration + 50);
    }
  },

  Promise: {
    color: '#ec4899',
    name: 'LastVote',

    format(event, labels) {
      return `[LastVote] ${nodeText(labels, event.id)} → ${nodeText(labels, event.from)}: Ballot ${event.ballot}`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.setNodeState(event.id, 'promise');
      flashNodeWithMotion(visualizer, event.id, this.color, { motion: 'reply' });

      if (event.from !== undefined && event.from !== event.id) {
        const snapshot = state.snapshot();
        const duration = getBeamDuration(snapshot, 500);
        if (canCommunicate(event.id, event.from)) {
          await visualizer.drawBeam(event.id, event.from, this.color, duration, 'dashed', { motion: 'reply' });
        }
        scheduleNodeReset(visualizer, event.id, duration + 50);
      } else {
        scheduleNodeReset(visualizer, event.id, 200);
      }
    }
  },

  Accept: {
    color: '#f87171',
    name: 'BeginBallot',

    format(event, labels) {
      return `[BeginBallot] ${nodeText(labels, event.id)}: Ballot ${event.ballot}, Decree #${event.decree_num}`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.setNodeState(event.id, 'accept');
      flashNodeWithMotion(visualizer, event.id, this.color, { motion: 'proposal' });

      const snapshot = state.snapshot();
      const duration = getBeamDuration(snapshot, 500);

      const quorum = Array.isArray(event.quorum) ? event.quorum : [];
      const targets = quorum.filter(
        (nodeId) => nodeId !== event.id && canCommunicate(event.id, nodeId)
      );
      await drawBeamsTo(visualizer, event.id, targets, this.color, duration, 'solid', { motion: 'proposal' });

      scheduleNodeReset(visualizer, event.id, duration + 50);
    }
  },

  Accepted: {
   color: '#10b981',
   name: 'Voted',

   format(event, labels) {
     return `[Voted] ${nodeText(labels, event.id)} → ${nodeText(labels, event.from)}: Ballot ${event.ballot}`;
   },

   async visualize(event, visualizer, state, canCommunicate) {
     visualizer.setNodeState(event.id, 'voted');
     flashNodeWithMotion(visualizer, event.id, this.color, { motion: 'reply' });

     const snapshot = state.snapshot();
     const duration = getBeamDuration(snapshot, 500);

     if (event.from !== undefined && event.from !== event.id) {
       if (canCommunicate(event.id, event.from)) {
         await visualizer.drawBeam(event.id, event.from, this.color, duration, 'dotted', { motion: 'reply' });
       }
     }

     scheduleNodeReset(visualizer, event.id, duration + 50);
   }
  },

  Learn: {
   color: '#34d399',
   name: 'Learn',

   format(event, labels) {
     const decree = formatDecree(event);
     return `[Learn] ${nodeText(labels, event.id)}: "${decree}"`;
   },

   async visualize(event, visualizer, state, canCommunicate) {
     visualizer.setNodeState(event.id, 'learn');
     flashNodeWithMotion(visualizer, event.id, this.color, { motion: 'success', resetDelay: 700 });

     scheduleNodeReset(visualizer, event.id, 350);
   }
  },

  LearnedValue: {
    color: '#34d399',
    name: 'LearnedValue',

    format(event, labels) {
      const decree = formatDecree(event);
      return `[LearnedValue] ${nodeText(labels, event.id)}: "${decree}"`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      visualizer.setNodeState(event.id, 'learn');
      flashNodeWithMotion(visualizer, event.id, this.color, { motion: 'success', resetDelay: 700 });

      scheduleNodeReset(visualizer, event.id, 350);
    }
  },

  InitialDecree: {
    color: '#8b5cf6',
    name: 'InitialDecree',

    format(event, labels) {
      const decree = formatDecree(event);
      return `[InitialDecree] ${nodeText(labels, event.id)}: [${event.decree_num}] "${decree}"`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      state.addDecree(event.id, {
        decree_num: event.decree_num,
        decree: formatDecree(event),
        timestamp: Date.now()
      });
    }
  },

  BatchInitialDecrees: {
    color: '#8b5cf6',
    name: 'BatchInitialDecrees',

    format(event, labels) {
      return `[Batch Init] ${nodeText(labels, event.id)}: ${event.decrees.length} decrees loaded`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      for (const [decree_num, value] of event.decrees) {
        state.addDecree(event.id, {
          decree_num,
          decree: typeof value === 'string' ? value : JSON.stringify(value),
          timestamp: Date.now()
        });
      }
    }
  },

  LedgerDump: {
    color: '#8b5cf6',
    name: 'LedgerDump',

    format(event, labels) {
      return `[Ledger Dump] ${nodeText(labels, event.id)}: ${event.decrees.length} total decrees`;
    },

    async visualize(event, visualizer, state, canCommunicate) {
      for (const [decree_num, value] of event.decrees) {
        state.addDecree(event.id, {
          decree_num,
          decree: typeof value === 'string' ? value : JSON.stringify(value),
          timestamp: Date.now()
        });
      }
    }
  },

  Success: {
   color: '#6366f1',
   name: 'Success',

   format(event, labels) {
     const proposerInfo =
       event.ballot_proposer !== undefined
         ? ` (proposed by ${nodeText(labels, event.ballot_proposer)})`
         : '';
     return `[Success] ${nodeText(labels, event.id)}: Decree #${event.decree_num} chosen${proposerInfo}`;
   },

   async visualize(event, visualizer, state, canCommunicate) {
     visualizer.setNodeState(event.id, 'success');
     flashNodeWithMotion(visualizer, event.id, this.color, { motion: 'success', resetDelay: 850 });

     const snapshot = state.snapshot();
     if (snapshot.cluster) {
       const targets = getReachableTargets(
         event.id,
         snapshot.cluster.total_nodes,
         canCommunicate
       );
       await drawBeamsTo(visualizer, event.id, targets, this.color, 350, 'solid', { motion: 'success' });
       scheduleNodeReset(visualizer, event.id, 400);
     }
   }
  }
};
