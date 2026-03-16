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
          // Reset all nodes to default state
          for (let i = 0; i < 7; i++) {
            visualizer.setNodeState(i, '--');
            visualizer.setNodeColor(i, '#3b82f6');
          }
          visualizer.clearBeams();
          
          visualizer.setNodeState(0, "propose");
          visualizer.flashNode(0, colors.nextballot, { motion: "proposal" });
          addEvent(
            "[NextBallot] Node 0 sends ballot 100 to all acceptors",
            colors.nextballot
          );
          const acceptors = [1, 2, 3, 4, 5, 6];
          await visualizer.drawBeamsTo(0, acceptors, colors.nextballot, 500, 'solid', 80, { motion: "proposal" });
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
          const respondents = [1, 2, 3, 4, 5, 6];
          for (let i = 1; i <= 6; i++) {
            visualizer.setNodeState(i, "respond");
            visualizer.flashNode(i, colors.lastvote, { motion: "reply" });
            addEvent(`[LastVote] Node ${i} responds`, colors.lastvote);
            eventCounts.lastvote++;
          }
          await visualizer.drawBeamsFrom(respondents, 0, colors.lastvote, 500, 'dashed', 150, { motion: "reply" });
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
          visualizer.flashNode(0, colors.beginballot, { motion: "proposal" });
          addEvent(
            "[BeginBallot] Node 0 sends decree to quorum",
            colors.beginballot
          );
          const quorum = [1, 2, 3, 4, 5, 6];
          for (const node of quorum) {
            visualizer.setNodeState(node, "wait");
          }
          await visualizer.drawBeamsTo(0, quorum, colors.beginballot, 500, 'solid', 100, { motion: "proposal" });
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
            visualizer.flashNode(node, colors.voted, { motion: "reply" });
            addEvent(`[Voted] Node ${node} votes`, colors.voted);
            eventCounts.voted++;
          }
          await visualizer.drawBeamsFrom(quorum, 0, colors.voted, 500, 'dotted', 150, { motion: "reply" });
          updateCounts();
        },
      },
      {
        title: "Step 5-6: Success(d)",
        description:
          "Proposer received quorum of votes, decree is chosen",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(0, "learned");
          visualizer.flashNode(0, colors.success, { motion: "success" });
          addEvent("[Success] Decree is chosen!", colors.success);
          // Draw success beams from proposer to all nodes
          const acceptors = [1, 2, 3, 4, 5, 6];
          for (const node of acceptors) {
            visualizer.setNodeState(node, "learned");
          }
          await visualizer.drawBeamsTo(0, acceptors, colors.success, 500, 'solid', 80, { motion: "success" });
          eventCounts.success += acceptors.length;
          eventCounts.learned += acceptors.length;
          updateCounts();
          await sleep(500);
        },
      },
    ];
  },
};
