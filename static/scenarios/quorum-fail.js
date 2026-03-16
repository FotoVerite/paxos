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
          // Reset all nodes to default state
          for (let i = 0; i < 7; i++) {
            visualizer.setNodeState(i, '--');
            visualizer.setNodeColor(i, '#3b82f6');
          }
          visualizer.clearBeams();
          
          visualizer.setNodeState(0, "propose");
          visualizer.flashNode(0, colors.nextballot, { motion: "proposal" });
          addEvent(
            "[NextBallot] Node 0 sends ballot 101 to all acceptors",
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
        description:
          "Only nodes 1, 2, 3 respond (network issues for others)",
        action: async () => {
          visualizer.clearBeams();
          for (let i of [1, 2, 3]) {
            visualizer.setNodeState(i, "respond");
            visualizer.flashNode(i, colors.lastvote, { motion: "reply" });
            addEvent(`[LastVote] Node ${i} responds`, colors.lastvote);
            eventCounts.lastvote++;
            visualizer.drawBeam(i, 0, colors.lastvote, 500, "solid", { motion: "reply" });
            await sleep(150);
          }
          // Others don't respond - timeout
          for (let i of [4, 5, 6]) {
            visualizer.setNodeState(i, "timeout");
            visualizer.setNodeMuted(i, true, {
              color: "#64748b",
              opacity: 0.58,
            });
            visualizer.pulseNode(i, "#94a3b8", {
              resetDelay: 420,
              glowRadius: 8,
              glowOpacity: 0.35,
              strokeWidth: 3,
            });
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
          visualizer.setNodeState(0, "fail");
          visualizer.flashNode(0, "#ef4444", { motion: "failure", resetDelay: 700 });
          visualizer.pulseNode(0, "#ef4444", {
            resetDelay: 520,
            glowRadius: 12,
            glowOpacity: 0.8,
          });
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
