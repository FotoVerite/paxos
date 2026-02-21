/**
 * Playback Delay Plugin
 * Adds a small pause between events for readability
 */

export function createPlaybackDelayPlugin({ minDelay = 300, baseDelay = 400 } = {}) {
  return {
    async onEvent(event, ctx) {
      if (ctx.playbackMode === 'step' || ctx.playbackMode === 'step-back') {
        return;
      }
      const snapshot = ctx.state.snapshot();
      const speed = snapshot.simulation.speed || 1;
      const eventType = event?.eventType;
      const isFastControlPlane =
        eventType === 'PmmcPreempted' || eventType === 'PmmcAdopted';
      const localBaseDelay = isFastControlPlane ? 70 : baseDelay;
      const localMinDelay = isFastControlPlane ? 30 : minDelay;
      const delay = Math.max(localMinDelay, localBaseDelay / speed);
      await new Promise((resolve) => setTimeout(resolve, delay));
    },
  };
}
