export const PMMC_SCENARIO_METADATA = Object.freeze({
  pmmc_single_client: { nodeCount: 5 },
  pmmc_role_split: { nodeCount: 7 },
  pmmc_leader_crash: { nodeCount: 7 },
  pmmc_replica_crash_failover: { nodeCount: 7 },
  pmmc_leader_partition_heal: { nodeCount: 7 },
  pmmc_acceptor_majority_loss_then_recover: { nodeCount: 7 },
  pmmc_staggered_leader_join: { nodeCount: 7 },
  pmmc_reconfig_joint_consensus: { nodeCount: 5 },
  pmmc_reconfig_stop_sign: { nodeCount: 5 },
  pmmc_reconfig_delayed_stop_sign: { nodeCount: 5 },
  pmmc_reconfig_padding: { nodeCount: 5 },
  pmmc_reconfig_brick_wall: { nodeCount: 5 },
});

export function getPmmcScenarioNodeCount(scenarioType, fallback = 5) {
  return PMMC_SCENARIO_METADATA[scenarioType]?.nodeCount ?? fallback;
}
