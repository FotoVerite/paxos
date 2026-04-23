# Scenario Builder Notes

This repo already has a canonical scenario DSL in [src/scenario.rs](/Users/matthewbergman/learning/paxos/src/scenario.rs).

The important rule is simple:

- author scenarios with `ScenarioBuilder`
- execute them with a protocol-specific runner
- do not add one-off scripted flows in `cluster_manager` when the scenario belongs in the builder

## Current Shape

`ScenarioBuilder` builds a generic `Scenario`:

- `name`
- `description`
- `node_count`
- `phases`
- `steps`

The builder itself is intentionally dumb. It only records ordered steps. It does not know how a protocol executes them.

That execution belongs in the runner:

- [src/scenario_runner.rs](/Users/matthewbergman/learning/paxos/src/scenario_runner.rs) is the classic runner
- PMMC uses its own spec/runner path under [src/web/scenarios/pmmc](/Users/matthewbergman/learning/paxos/src/web/scenarios/pmmc)
- vertical should do the same instead of using hardcoded sleeps in `cluster_manager`

## Extension Rule

When a new protocol needs new scripted behavior:

1. add explicit step variants to `ScenarioStep`
2. add fluent builder methods to `ScenarioBuilder`
3. add a protocol-specific runner that executes only the steps it understands
4. keep scenario definitions in protocol-specific spec files
5. call those scenarios from the web layer

Do not:

- hide scenario logic inside `cluster_manager`
- add anonymous async scripts with sleeps as the primary control flow
- force every protocol through the classic runner

## Vertical Rules

Vertical scenarios should be:

- configuration-first
- milestone-driven
- client-driven where possible

That means vertical scripts should prefer:

- install configuration
- start activation
- wait for activation ready
- send client request to a replica
- verify redirect or accepted reply
- only use short pauses to let the visualizer breathe

Vertical scenarios should not use long arbitrary sleeps to fake structure.

If a vertical step needs configuration details, keep those details in a vertical scenario spec next to the scenario, not embedded ad hoc in `cluster_manager`.

## Where Vertical Logic Should Live

- generic DSL: [src/scenario.rs](/Users/matthewbergman/learning/paxos/src/scenario.rs)
- vertical specs: `src/web/scenarios/vertical/spec.rs`
- vertical runner: `src/web/scenarios/vertical/runner.rs`
- web entrypoint: `cluster_manager` should only select/build/run a named vertical scenario

## Practical Reminder

If you are about to write:

- `run_vertical_demo_script(...)`
- a chain of `sleep_or_stop(...)`
- hardcoded install/activate/request logic in `cluster_manager`

stop and move that behavior into:

- a `ScenarioBuilder` definition
- a vertical scenario runner

That is the boundary.
