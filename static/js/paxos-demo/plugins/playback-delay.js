/**
 * Playback Delay Plugin
 * Adds a small pause between events for readability
 */

export function createPlaybackDelayPlugin({ minDelay = 300, baseDelay = 400 } = {}) {
  return {
    async onEvent(_, ctx) {
      const snapshot = ctx.state.snapshot();
      const speed = snapshot.simulation.speed || 1;
      const delay = Math.max(minDelay, baseDelay / speed);
      await new Promise((resolve) => setTimeout(resolve, delay));
    },
  };
}
