// Scenario D: Two Proposers (Interleaving)
const scenarioTwoProposers = {
  name: "Two Proposers (Interleaving)",
  description:
    "Demonstrating gap constraints: P1 interrupts P0, creating conflicting promises",
  nodeCount: 7,
  getPhases(colors, utils) {
    const { visualizer, addEvent, sleep, eventCounts, updateCounts } = utils;
    return [
      {
        title: "Step 1: Proposer 0 → NextBallot(100)",
        description:
          "Proposer 0 queries acceptors for highest votes below ballot 100",
        action: async () => {
          visualizer.setNodeState(0, "propose");
          visualizer.activateNode(0, colors.nextballot);
          addEvent(
            '[NextBallot] Proposer 0: "What\'s your highest vote < 100?"',
            colors.nextballot
          );
          const acceptors = [1, 2, 3, 4, 5, 6];
          const beams = [];
          for (const node of acceptors) {
            beams.push(visualizer.drawBeam(0, node, colors.nextballot));
            await sleep(80);
          }
          await Promise.all(beams);
          eventCounts.nextballot++;
          updateCounts();
          await sleep(400);
        },
      },
      {
        title: "Step 2: Acceptors respond (partial quorum)",
        description:
          "Nodes 1-4 reply: no prior votes. Creates gap constraint [⊥, 100)",
        action: async () => {
          visualizer.clearBeams();
          const beams = [];
          for (let i of [1, 2, 3, 4]) {
            visualizer.setNodeState(i, "respond");
            visualizer.activateNode(i, colors.lastvote);
            addEvent(
              `[LastVote] Node ${i} → null (no votes yet)`,
              colors.lastvote
            );
            eventCounts.lastvote++;
            beams.push(visualizer.drawBeam(i, 0, colors.lastvote, 500, "solid"));
            await sleep(100);
          }
          await Promise.all(beams);
          updateCounts();
          addEvent(
            '[Gap Constraint] Nodes 1-4: "We promise no vote < 100"',
            "#94a3b8"
          );
          await sleep(300);
        },
      },
      {
        title:
          "Step 1b: Proposer 1 → NextBallot(101) [INTERLEAVING]",
        description:
          "Before P0 sends BeginBallot, P1 interrupts with higher ballot",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(1, "propose");
          visualizer.activateNode(1, "#a78bfa");
          addEvent(
            '[NextBallot] Proposer 1: "What\'s your highest vote < 101?" [BLOCKS P0]',
            "#a78bfa"
          );
          const acceptors = [1, 2, 3, 4, 5, 6];
          const beams = [];
          for (const node of acceptors) {
            beams.push(visualizer.drawBeam(1, node, colors.nextballot));
            await sleep(80);
          }
          await Promise.all(beams);
          eventCounts.nextballot++;
          updateCounts();
          await sleep(400);
        },
      },
      {
        title:
          "Step 2b: Acceptors respond to P1 (UPDATED PROMISE)",
        description:
          "Nodes 1-5 reply: null. Node 6 times out. NEW gap constraint [⊥, 101)",
        action: async () => {
          visualizer.clearBeams();
          const beams = [];
          for (let i of [1, 2, 3, 4, 5]) {
            visualizer.setNodeState(i, "respond");
            visualizer.activateNode(i, colors.lastvote);
            addEvent(
              `[LastVote] Node ${i} → null (highest vote < 101)`,
              colors.lastvote
            );
            eventCounts.lastvote++;
            beams.push(visualizer.drawBeam(i, 1, colors.lastvote, 500, "dashed"));
            await sleep(100);
          }
          await Promise.all(beams);
          visualizer.setNodeState(6, "timeout");
          visualizer.setNodeColor(6, "#64748b");
          addEvent(`[Timeout] Node 6 no response`, "#94a3b8");
          updateCounts();
          addEvent(
            '[NEW Gap] Nodes 1-5: "Promise no vote in [⊥, 101) - replaces [⊥, 100)"',
            "#ef4444"
          );
          await sleep(300);
        },
      },
      {
        title:
          "Step 3: Proposer 0 tries BeginBallot(100) [VIOLATES GAP]",
        description:
          "P0 attempts to vote with ballot 100, but acceptors promised [⊥, 101)",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(0, "send");
          visualizer.activateNode(0, colors.beginballot);
          addEvent(
            "[BeginBallot] P0 tries to vote with ballot 100",
            colors.beginballot
          );
          const beams = [];
          for (let i of [1, 2, 3, 4]) {
            beams.push(visualizer.drawBeam(0, i, "#ef4444", 500, "dotted"));
            await sleep(80);
          }
          await Promise.all(beams);
          await sleep(200);
          addEvent(
            "[REJECT] Gap violation: 100 < 101 (promised gap)",
            "#ef4444"
          );
          addEvent(
            "[REJECT] Acceptors won't vote in gap [⊥, 101)",
            "#ef4444"
          );
          eventCounts.beginballot++;
          updateCounts();
          await sleep(400);
        },
      },
      {
        title:
          "Step 3b: Proposer 1 → BeginBallot(101)",
        description:
          "P1 sends BeginBallot with ballot 101 (inside its own gap as boundary)",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(1, "send");
          visualizer.activateNode(1, "#a78bfa");
          addEvent(
            "[BeginBallot] Proposer 1 votes with ballot 101 (no violation)",
            "#a78bfa"
          );
          const quorum = [1, 2, 3, 4, 5];
          const beams = [];
          for (const node of quorum) {
            visualizer.setNodeState(node, "wait");
            beams.push(visualizer.drawBeam(1, node, colors.beginballot, 500, "solid"));
            await sleep(80);
          }
          await Promise.all(beams);
          eventCounts.beginballot++;
          updateCounts();
          await sleep(300);
        },
      },
      {
        title: "Step 4: Quorum votes for Proposer 1",
        description:
          "Quorum accepts ballot 101 - gap constraint satisfied",
        action: async () => {
          visualizer.clearBeams();
          const quorum = [1, 2, 3, 4, 5];
          const beams = [];
          for (const node of quorum) {
            visualizer.setNodeState(node, "voted");
            visualizer.activateNode(node, colors.voted);
            addEvent(
              `[Voted] Node ${node} accepts ballot 101`,
              colors.voted
            );
            eventCounts.voted++;
            beams.push(visualizer.drawBeam(node, 1, colors.voted, 500, "solid"));
            await sleep(100);
          }
          await Promise.all(beams);
          updateCounts();
        },
      },
      {
        title: "Step 5-6: Success",
        description:
          "Proposer 1 reached quorum - decree chosen with ballot 101",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(1, "learn");
          visualizer.activateNode(1, colors.success);
          addEvent(
            "[Success] Decree chosen in ballot 101",
            colors.success
          );
          eventCounts.success++;
          updateCounts();
          await sleep(500);
        },
      },
      {
        title: "Key: Gap Constraints",
        description:
          "Each NextBallot response creates a promise about a gap of ballots",
        action: async () => {
          addEvent(
            "[Gap Semantics] LastVote(b, v) promises: no vote < b except v",
            colors.success
          );
          addEvent(
            "[Conflict] P0: gap [⊥, 100) vs P1: gap [⊥, 101)",
            "#ef4444"
          );
          addEvent(
            "[Result] Higher gap wins - P0 blocked, P1 succeeds",
            colors.voted
          );
          await sleep(1000);
        },
      },
    ];
  },
};
