// Scenario C: Higher Ballot Success
const scenarioHigherBallot = {
  name: "Higher Ballot Success",
  description: "Proposer uses higher ballot number (102) and succeeds",
  nodeCount: 7,
  getPhases(colors, utils) {
    const { visualizer, addEvent, sleep, eventCounts, updateCounts } = utils;
    return [
      {
        title: "Attempt 4: NextBallot(b)",
        description: "Proposer uses ballot 102 (higher than all previous)",
        action: async () => {
          // Reset all nodes to default state
          for (let i = 0; i < 7; i++) {
            visualizer.setNodeState(i, "--");
            visualizer.setNodeColor(i, "#3b82f6");
          }
          visualizer.clearBeams();

          visualizer.setNodeState(0, "propose");
          visualizer.activateNode(0, colors.nextballot);
          addEvent(
            "[NextBallot] Node 0 sends ballot 100 to all acceptors",
            colors.nextballot
          );
          const acceptors = [1, 2, 3, 4, 5, 6];
          await visualizer.drawBeamsTo(
            0,
            acceptors,
            colors.nextballot,
            500,
            "solid",
            80
          );
          eventCounts.nextballot++;
          updateCounts();
          await sleep(300);
        },
      },
      {
        title: "Step 2: LastVote(b, v)",
        description: "All nodes respond",
        action: async () => {
          visualizer.clearBeams();
          const respondents = [1, 2, 3, 4, 5, 6];
          for (let i = 1; i <= 6; i++) {
            visualizer.setNodeState(i, "respond");
            visualizer.activateNode(i, colors.lastvote);
            addEvent(`[LastVote] Node ${i} responds`, colors.lastvote);
            eventCounts.lastvote++;
          }
          await visualizer.drawBeamsFrom(
            respondents,
            0,
            colors.lastvote,
            500,
            "dashed",
            150
          );
          updateCounts();
          await sleep(300);
        },
      },
      {
        title: "Step 3: BeginBallot(b, d)",
        description: "Proposer sends to nodes 1-5 (quorum)",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(0, "send");
          visualizer.activateNode(0, colors.beginballot);
          addEvent("[BeginBallot] Node 0 sends decree", colors.beginballot);
          const quorum = [1, 2, 3, 4, 5];
          for (const node of quorum) {
            visualizer.setNodeState(node, "wait");
          }
          await visualizer.drawBeamsTo(
            0,
            quorum,
            colors.beginballot,
            500,
            "solid",
            100
          );
          eventCounts.beginballot++;
          updateCounts();
          await sleep(300);
        },
      },
      {
        title: "Step 4: Voted(b, q)",
        description: "All quorum members vote",
        action: async () => {
          visualizer.clearBeams();
          const quorum = [1, 2, 3, 4, 5];
          for (const node of quorum) {
            visualizer.setNodeState(node, "voted");
            visualizer.activateNode(node, colors.voted);
            addEvent(`[Voted] Node ${node} votes`, colors.voted);
            eventCounts.voted++;
          }
          await visualizer.drawBeamsFrom(
            quorum,
            0,
            colors.voted,
            500,
            "dotted",
            150
          );
          updateCounts();
        },
      },
      {
        title: "Step 5-6: Success(d)",
        description: "Quorum achieved! Decree is chosen.",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(0, "learn");
          visualizer.activateNode(0, colors.success);
          addEvent("[Success] Decree chosen with ballot 102!", colors.success);
          // Draw success beams from proposer to all nodes
          const acceptors = [1, 2, 3, 4, 5, 6];
          await visualizer.drawBeamsTo(
            0,
            acceptors,
            colors.success,
            500,
            "solid",
            80
          );
          eventCounts.success += acceptors.length;
          eventCounts.learned += acceptors.length;
          updateCounts();
          await sleep(600);
        },
      },
    ];
  },
};
