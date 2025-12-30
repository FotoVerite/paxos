// Scenario A: Clean Success
const scenarioSuccess = {
  name: "Clean Success",
  description: "All nodes participate, quorum achieved on first try",
  nodeCount: 7,
  getPhases(colors, utils) {
    const { visualizer, addEvent, sleep, eventCounts, updateCounts } = utils;
    return [
      {
        title: "Step 1: NextBallot(b)",
        description:
          "Proposer (node 0) sends NextBallot with ballot number 100",
        action: async () => {
          visualizer.setNodeState(0, "propose");
          visualizer.activateNode(0, colors.nextballot);
          addEvent(
            "[NextBallot] Node 0 sends ballot 100 to all acceptors",
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
        description: "Acceptors (nodes 1-6) respond with their previous votes",
        action: async () => {
          visualizer.clearBeams();
          for (let i = 1; i <= 6; i++) {
            visualizer.setNodeState(i, "respond");
            visualizer.activateNode(i, colors.lastvote);
            addEvent(`[LastVote] Node ${i} responds`, colors.lastvote);
            eventCounts.lastvote++;
            visualizer.drawBeam(i, 0, colors.lastvote);
            await sleep(150);
          }
          updateCounts();
          await sleep(300);
        },
      },
      {
        title: "Step 3: BeginBallot(b, d)",
        description:
          "Proposer sends BeginBallot with decree to nodes 1-5 (quorum of 4)",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(0, "send");
          visualizer.activateNode(0, colors.beginballot);
          addEvent(
            "[BeginBallot] Node 0 sends decree to quorum",
            colors.beginballot
          );
          const quorum = [1, 2, 3, 4, 5, 6];
          for (const node of quorum) {
            visualizer.setNodeState(node, "wait");
            visualizer.drawBeam(0, node, colors.beginballot);
            await sleep(100);
          }
          eventCounts.beginballot++;
          updateCounts();
          await sleep(300);
        },
      },
      {
        title: "Step 4: Voted(b, q)",
        description: "Acceptors in quorum cast votes and respond",
        action: async () => {
          visualizer.clearBeams();
          const quorum = [1, 2, 3, 4, 5, 6];
          for (const node of quorum) {
            visualizer.setNodeState(node, "voted");
            visualizer.activateNode(node, colors.voted);
            addEvent(`[Voted] Node ${node} votes`, colors.voted);
            eventCounts.voted++;
            visualizer.drawBeam(node, 0, colors.voted);
            await sleep(150);
          }
          updateCounts();
        },
      },
      {
        title: "Step 5-6: Success(d)",
        description:
          "Proposer received quorum of votes, decree is chosen",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(0, "learn");
          visualizer.activateNode(0, colors.success);
          addEvent("[Success] Decree is chosen!", colors.success);
          eventCounts.success++;
          updateCounts();
          await sleep(500);
        },
      },
    ];
  },
};
