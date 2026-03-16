/**
 * Playback Delay Plugin
 * Adds a small pause between events for readability
 */

export function createPlaybackDelayPlugin({ minDelay = 70, baseDelay = 180 } = {}) {
  return {
    async onEvent(event, ctx) {
      if (ctx.playbackMode === 'step' || ctx.playbackMode === 'step-back') {
        return;
      }
      const snapshot = ctx.state.snapshot();
      const speed = snapshot.simulation.speed || 1;
      const eventType = event?.eventType;
      const isFastControlPlane =
        eventType === 'PmmcPreempted' ||
        eventType === 'PmmcAdopted' ||
        eventType === 'LeaderSteppedDown' ||
        eventType === 'NodeCrashed';
      const metrics =
        typeof ctx.getPlaybackMetrics === 'function'
          ? ctx.getPlaybackMetrics()
          : null;
      const backlogPressure =
        (metrics?.queueLength || 0) +
        (metrics?.backlogBatches || 0) * 2 +
        (metrics?.queueProcessing ? 1 : 0);

      let localBaseDelay = isFastControlPlane ? 70 : baseDelay;
      let localMinDelay = isFastControlPlane ? 30 : minDelay;

      if (backlogPressure >= 6) {
        localBaseDelay = isFastControlPlane ? 16 : 28;
        localMinDelay = isFastControlPlane ? 8 : 12;
      } else if (backlogPressure >= 3) {
        localBaseDelay = isFastControlPlane ? 30 : 70;
        localMinDelay = isFastControlPlane ? 14 : 28;
      }

      const delay = Math.max(localMinDelay, localBaseDelay / speed);
      await new Promise((resolve) => setTimeout(resolve, delay));
    },
  };
}
