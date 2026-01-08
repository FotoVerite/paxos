// Scenario F: Multiple Proposers, Both Make Progress
const scenarioMultiProposerProgress = {
  name: "Multiple Proposers, Both Make Progress",
  description:
    "Two proposers succeed sequentially - first completes, second respects B3",
  nodeCount: 7,
  getPhases(colors, utils) {
    const { visualizer, addEvent, sleep, eventCounts, updateCounts } = utils;
    return [
      {
        title: "Phase 1: Proposer A → NextBallot(100)",
        description:
          "Proposer A (node 0) initiates first ballot (empty ledgers)",
        action: async () => {
          // Reset all nodes to default state
          for (let i = 0; i < 7; i++) {
            visualizer.setNodeState(i, '--');
            visualizer.setNodeColor(i, '#3b82f6');
          }
          visualizer.clearBeams();
          
          visualizer.setNodeState(0, "propose");
          visualizer.activateNode(0, colors.nextballot);
          addEvent("[NextBallot] Proposer A sends ballot 100", colors.nextballot);
          const acceptors = [1, 2, 3, 4, 5, 6];
          await visualizer.drawBeamsTo(0, acceptors, colors.nextballot, 500, 'solid', 80);
          eventCounts.nextballot++;
          updateCounts();
          await sleep(300);
        },
      },
      {
        title: "Phase 2: LastVote responses (empty)",
        description:
          "Acceptors respond - no prior votes, all return null",
        action: async () => {
          visualizer.clearBeams();
          for (let i of [1, 2, 3, 4, 5, 6]) {
            visualizer.setNodeState(i, "respond");
            visualizer.activateNode(i, colors.lastvote);
            addEvent(
              `[LastVote] Node ${i} → null (no prior votes)`,
              colors.lastvote
            );
            eventCounts.lastvote++;
          }
          await visualizer.drawBeamsFrom([1, 2, 3, 4, 5, 6], 0, colors.lastvote, 500, 'dashed', 80);
          updateCounts();
          await sleep(200);
        },
      },
      {
        title: 'Phase 3: BeginBallot(100, "Decree X")',
        description:
          "Proposer A sends decree to quorum - no B3 constraint (no prior votes)",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(0, "send");
          visualizer.activateNode(0, colors.beginballot);
          addEvent(
            '[BeginBallot] Proposer A sends "Decree X" to quorum',
            colors.beginballot
          );
          const quorum = [1, 2, 3, 4, 5];
          for (const node of quorum) {
            visualizer.setNodeState(node, "wait");
          }
          await visualizer.drawBeamsTo(0, quorum, colors.beginballot, 500, 'solid', 100);
          eventCounts.beginballot++;
          updateCounts();
          await sleep(300);
        },
      },
      {
        title: "Phase 4: Voted(100, q)",
        description:
          "Quorum members vote for Decree X in ballot 100",
        action: async () => {
          visualizer.clearBeams();
          const quorum = [1, 2, 3, 4, 5];
          for (const node of quorum) {
            visualizer.setNodeState(node, "voted");
            visualizer.activateNode(node, colors.voted);
            addEvent(
              `[Voted] Node ${node} votes for "Decree X"`,
              colors.voted
            );
            eventCounts.voted++;
          }
          await visualizer.drawBeamsFrom(quorum, 0, colors.voted, 500, 'dotted', 150);
          updateCounts();
          await sleep(200);
        },
      },
      {
        title: "Phase 5-6: Success(Decree X)",
        description:
          "Proposer A reached quorum - writes Decree X to ledger",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(0, "learn");
          visualizer.activateNode(0, colors.success);
          addEvent(
            '[Success] Proposer A: "Decree X" chosen in ballot 100!',
            colors.success
          );
          // Draw success beams from proposer A to all nodes
          const acceptors = [1, 2, 3, 4, 5, 6];
          await visualizer.drawBeamsTo(0, acceptors, colors.success, 500, 'solid', 80);
          eventCounts.success += acceptors.length;
          eventCounts.learned += acceptors.length;
          updateCounts();
          await sleep(500);
        },
      },
      {
        title: "Phase 1b: Proposer B → NextBallot(101)",
        description: "Proposer B (node 1) starts later with higher ballot",
        action: async () => {
          visualizer.setNodeState(1, "propose");
          visualizer.activateNode(1, "#a78bfa");
          addEvent(
            "[NextBallot] Proposer B sends ballot 101",
            "#a78bfa"
          );
          const acceptors = [1, 2, 3, 4, 5, 6];
          await visualizer.drawBeamsTo(1, acceptors, colors.nextballot, 500, 'solid', 80);
          eventCounts.nextballot++;
          updateCounts();
          await sleep(300);
        },
      },
      {
        title: "Phase 2b: LastVote responses (with decree)",
        description:
          'Acceptors respond with their vote for ballot 100: "Decree X"',
        action: async () => {
          visualizer.clearBeams();
          for (let i of [1, 2, 3, 4, 5, 6]) {
            visualizer.setNodeState(i, "respond");
            visualizer.activateNode(i, colors.lastvote);
            addEvent(
              `[LastVote] Node ${i} → ballot 100 voted "Decree X"`,
              colors.lastvote
            );
            eventCounts.lastvote++;
          }
          await visualizer.drawBeamsFrom([1, 2, 3, 4, 5, 6], 1, colors.lastvote, 500, 'dashed', 80);
          updateCounts();
          await sleep(200);
        },
      },
      {
        title: 'Phase 3b: BeginBallot(101, "Decree X")',
        description:
          'By B3: must use "Decree X" (highest voted decree < 101)',
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(1, "send");
          visualizer.activateNode(1, "#a78bfa");
          addEvent(
            '[BeginBallot] Proposer B must send "Decree X" (B3 constraint)',
            "#a78bfa"
          );
          const quorum = [1, 2, 3, 4, 5];
          for (const node of quorum) {
            visualizer.setNodeState(node, "wait");
          }
          await visualizer.drawBeamsTo(1, quorum, colors.beginballot, 500, 'solid', 100);
          eventCounts.beginballot++;
          updateCounts();
          await sleep(300);
        },
      },
      {
        title: "Phase 4b: Voted(101, q)",
        description:
          'Quorum votes for same Decree X in ballot 101',
        action: async () => {
          visualizer.clearBeams();
          const quorum = [1, 2, 3, 4, 5];
          for (const node of quorum) {
            visualizer.setNodeState(node, "voted");
            visualizer.activateNode(node, colors.voted);
            addEvent(
              `[Voted] Node ${node} votes for "Decree X" again`,
              colors.voted
            );
            eventCounts.voted++;
          }
          await visualizer.drawBeamsFrom(quorum, 1, colors.voted, 500, 'dotted', 150);
          updateCounts();
          await sleep(200);
        },
      },
      {
        title: "Phase 5b-6b: Success(Decree X)",
        description:
          "Proposer B reached quorum - same decree chosen again!",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(1, "learn");
          visualizer.activateNode(1, colors.success);
          addEvent(
            '[Success] Proposer B: "Decree X" chosen in ballot 101!',
            colors.success
          );
          // Draw success beams from proposer B to all nodes
          const acceptors = [1, 2, 3, 4, 5, 6];
          await visualizer.drawBeamsTo(1, acceptors, colors.success, 500, 'solid', 80);
          eventCounts.success += acceptors.length;
          eventCounts.learned += acceptors.length;
          updateCounts();
          await sleep(500);
        },
      },
      {
        title: "Key Insight: Safety via B3",
        description:
          "Both proposers succeeded - same decree in both ballots (consistency guaranteed)",
        action: async () => {
          addEvent(
            "[Insight] Multiple proposers → same decree chosen",
            colors.success
          );
          addEvent(
            "[B3 Property] Once a decree is voted, all future ballots must use it",
            colors.success
          );
          addEvent(
            '[Safety] All ledgers consistent - "Decree X" in both ballot 100 and 101',
            colors.success
          );
          await sleep(1000);
        },
      },
    ];
  },
};
