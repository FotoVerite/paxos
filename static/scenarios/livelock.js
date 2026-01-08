// Scenario E: Livelock (No Progress)
const scenarioLivelock = {
  name: "Livelock (No Progress)",
  description:
    "Two proposers get partial votes, each blocks the other before completion",
  nodeCount: 7,
  getPhases(colors, utils) {
    const { visualizer, addEvent, sleep, eventCounts, updateCounts } = utils;
    return [
      {
        title: "Proposer 0 → NextBallot(100)",
        description: "Proposer 0 initiates with ballot 100",
        action: async () => {
          // Reset all nodes to default state
          for (let i = 0; i < 7; i++) {
            visualizer.setNodeState(i, '--');
            visualizer.setNodeColor(i, '#3b82f6');
          }
          visualizer.clearBeams();
          
          visualizer.setNodeState(0, "propose");
          visualizer.activateNode(0, colors.nextballot);
          addEvent(
            "[NextBallot] Proposer 0 sends ballot 100 to all acceptors",
            colors.nextballot
          );
          const acceptors = [1, 2, 3, 4, 5, 6];
          await visualizer.drawBeamsTo(0, acceptors, colors.nextballot, 500, 'solid', 80);
          eventCounts.nextballot++;
          updateCounts();
          await sleep(200);
        },
      },
      {
        title: "Acceptors promise 100",
        description: "Nodes 1-6 respond with LastVote",
        action: async () => {
          visualizer.clearBeams();
          for (let i of [1, 2, 3, 4, 5, 6]) {
            visualizer.setNodeState(i, "respond");
            visualizer.activateNode(i, colors.lastvote);
            addEvent(
              `[LastVote] Node ${i} → Proposer 0 (ballot 100)`,
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
        title: "Proposer 0 → BeginBallot(100)",
        description: "Proposer 0 sends decree to quorum [1-5]",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(0, "send");
          visualizer.activateNode(0, colors.beginballot);
          addEvent("[BeginBallot] Proposer 0 sends decree", colors.beginballot);
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
        title: "Proposer 1 → NextBallot(101) - INTERLEAVES",
        description:
          "Before Proposer 0 collects votes, Proposer 1 sends higher ballot",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(1, "propose");
          visualizer.activateNode(1, "#a78bfa");
          addEvent(
            "[NextBallot] Proposer 1 sends ballot 101 (BLOCKS P0)",
            "#a78bfa"
          );
          const acceptors = [1, 2, 3, 4, 5, 6];
          await visualizer.drawBeamsTo(1, acceptors, colors.nextballot, 500, 'solid', 80);
          eventCounts.nextballot++;
          updateCounts();
          await sleep(200);
        },
      },
      {
        title: "Acceptors promise 101",
        description:
          "All acceptors update min_ballot to 101, rejecting P0's BeginBallot",
        action: async () => {
          visualizer.clearBeams();
          for (let i of [1, 2, 3, 4, 5, 6]) {
            visualizer.setNodeState(i, "respond");
            visualizer.activateNode(i, colors.lastvote);
            addEvent(
              `[LastVote] Node ${i} → Proposer 1 (ballot 101)`,
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
        title: "Proposer 1 → BeginBallot(101)",
        description: "Proposer 1 sends decree to quorum [1-5]",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(1, "send");
          visualizer.activateNode(1, "#a78bfa");
          addEvent("[BeginBallot] Proposer 1 sends decree", "#a78bfa");
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
        title: "Nodes 1-3 vote for Proposer 1",
        description: "Partial votes received (need 4 for quorum)",
        action: async () => {
          visualizer.clearBeams();
          const voters = [1, 2, 3];
          for (let i of voters) {
            visualizer.setNodeState(i, "voted");
            visualizer.activateNode(i, colors.voted);
            addEvent(
              `[Voted] Node ${i} → Proposer 1 (ballot 101)`,
              colors.voted
            );
            eventCounts.voted++;
          }
          await visualizer.drawBeamsFrom(voters, 1, colors.voted, 500, 'dotted', 150);
          updateCounts();
          await sleep(200);
        },
      },
      {
        title: "Proposer 0 → NextBallot(102) - COUNTER-INTERLEAVES",
        description:
          "Before Proposer 1 gets full quorum, Proposer 0 raises ballot",
        action: async () => {
          visualizer.clearBeams();
          visualizer.setNodeState(0, "propose");
          visualizer.activateNode(0, colors.nextballot);
          addEvent(
            "[NextBallot] Proposer 0 sends ballot 102 (BLOCKS P1)",
            colors.nextballot
          );
          const acceptors = [1, 2, 3, 4, 5, 6];
          await visualizer.drawBeamsTo(0, acceptors, colors.nextballot, 500, 'solid', 80);
          eventCounts.nextballot++;
          updateCounts();
          await sleep(200);
        },
      },
      {
        title: "Acceptors promise 102 again",
        description:
          "All acceptors switch to ballot 102, rejecting P1's votes",
        action: async () => {
          visualizer.clearBeams();
          for (let i of [1, 2, 3, 4, 5, 6]) {
            visualizer.setNodeState(i, "respond");
            visualizer.activateNode(i, colors.lastvote);
            addEvent(
              `[LastVote] Node ${i} → Proposer 0 (ballot 102)`,
              colors.lastvote
            );
            eventCounts.lastvote++;
          }
          await visualizer.drawBeamsFrom([1, 2, 3, 4, 5, 6], 0, colors.lastvote, 500, 'dashed', 80);
          updateCounts();
          await sleep(300);
        },
      },
      {
        title: "LIVELOCK: Cycle repeats forever",
        description:
          "Both proposers get partial progress but neither completes all 6 steps",
        action: async () => {
          addEvent(
            "[LIVELOCK] P0 raises ballot → P1 responds → P1 sends decree → P0 blocks",
            "#ef4444"
          );
          addEvent(
            "[LIVELOCK] P1 has 3/4 votes → P0 interrupts with higher ballot",
            "#ef4444"
          );
          addEvent(
            "[NO PROGRESS] Neither reaches Success(d) - cycle repeats",
            "#ef4444"
          );
          await sleep(1000);
        },
      },
    ];
  },
};
