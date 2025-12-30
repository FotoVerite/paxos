/**
 * Scenario Loader
 * Builds scenario objects from external scenario definitions
 */
function buildScenarios(colors, utils) {
  return {
    success: Object.assign({}, scenarioSuccess, {
      phases: scenarioSuccess.getPhases(colors, utils),
    }),
    quorum_fail: Object.assign({}, scenarioQuorumFail, {
      phases: scenarioQuorumFail.getPhases(colors, utils),
    }),
    higher_ballot: Object.assign({}, scenarioSuccess, {
      phases: scenarioSuccess.getPhases(colors, utils),
    }),
    two_proposers: Object.assign({}, scenarioTwoProposers, {
      phases: scenarioTwoProposers.getPhases(colors, utils),
    }),
    livelock: Object.assign({}, scenarioSuccess, {
      phases: scenarioSuccess.getPhases(colors, utils),
    }),
    multi_proposer_progress: Object.assign({}, scenarioSuccess, {
      phases: scenarioSuccess.getPhases(colors, utils),
    }),
  };
}
