// Scenario B: Quorum Fails
const scenarioQuorumFail = {
  name: "Quorum Fails",
  description: "Only 3 nodes respond (not enough for quorum of 4)",
  nodeCount: 7,
  getPhases(colors, utils) {
    const { visualizer, addEvent, sleep, eventCounts, updateCounts } = utils;
    return [
      {
        title: "Step 1: NextBallot(b)",
        description: "Proposer sends NextBallot with ballot 101",
        action: async () => {
          visualizer.setNodeState(0, "propose");
          visualizer.activateNode(0, colors.nextballot);
          addEvent(
            "[NextBallot] Node 0 sends ballot 101 to all acceptors",
            colors.nextballot
          );
          const acceptors = [1, 2, 3, 4, 5, 6];
          for (const node of acceptors) {
            visualizer.drawBeam(0, node, colors.nextballot);
            await sleep(80);
          }
          eventCounts.nextballot++;
          updateCounts();
          await sleep(300);
        },
      },
      {
        title: "Step 2: LastVote(b, v)",
        description:
          "Only nodes 1, 2, 3 respond (network issues for others)",
        action: async () => {
          visualizer.clearBeams();
          for (let i of [1, 2, 3]) {
            visualizer.setNodeState(i, "respond");
            visualizer.activateNode(i, colors.lastvote);
            addEvent(`[LastVote] Node ${i} responds`, colors.lastvote);
            eventCounts.lastvote++;
            visualizer.drawBeam(i, 0, colors.lastvote);
            await sleep(150);
          }
          // Others don't respond
          for (let i of [4, 5, 6]) {
            visualizer.setNodeState(i, "timeout");
            visualizer.setNodeColor(i, "#64748b");
            addEvent(`[Timeout] Node ${i} no response`, "#94a3b8");
          }
          updateCounts();
          await sleep(300);
        },
      },
      {
        title: "Step 3: BeginBallot(b, d)",
        description:
          "Proposer only has 3 LastVote responses, needs quorum of 4 - FAILS",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(0, "fail");
          addEvent(
            "[Failure] Insufficient LastVote responses",
            "#ef4444"
          );
          await sleep(800);
        },
      },
    ];
  },
};
