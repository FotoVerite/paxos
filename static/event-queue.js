/**
 * Event Queue
 * Batches and processes events with configurable timing
 */

export class EventQueue {
  constructor(handleEvent, batchWindow = 50, options = {}) {
    this.queue = [];
    this.batchWindow = batchWindow; // Microseconds for batching window
    this.processing = false;
    this.processingTimeout = null;
    this.handleEvent = typeof handleEvent === 'function' ? handleEvent : null;
    this.handleBatch = typeof options.handleBatch === 'function' ? options.handleBatch : null;
  }

  /**
   * Push event to queue
   * @param {Object} event - Event object with created_at timestamp
   */
  push(event) {
    this.queue.push(event);
    this.scheduleBatch();
  }

  /**
   * Schedule batch processing
   * Uses debounce to wait for events to accumulate
   */
  scheduleBatch() {
    if (this.processingTimeout) {
      clearTimeout(this.processingTimeout);
    }
    this.processingTimeout = setTimeout(() => {
      this.processBatch();
    }, 5); // Wait 5ms for more events to arrive
  }

  /**
   * Process next batch of events
   */
  async processBatch() {
    if (this.processing || this.queue.length === 0) {
      return;
    }

    this.processing = true;

    try {
      const batch = this.extractBatch();
      if (this.handleBatch) {
        await this.handleBatch(batch);
      } else if (this.handleEvent) {
        const promises = batch.map(event => this.handleEvent(event));
        await Promise.all(promises);
      }
    } catch (err) {
      console.error('Error processing event batch:', err);
    }

    this.processing = false;

    // Continue if more events waiting
    if (this.queue.length > 0) {
      setTimeout(() => this.processBatch(), 100);
    }
  }

  /**
   * Extract next batch of events based on timestamp window
   * @returns {Array} Batch of events
   */
  extractBatch() {
    const batch = [];
    if (this.queue.length === 0) {
      return batch;
    }

    const startTime = this.queue[0].created_at;

    while (
      this.queue.length > 0 &&
      this.queue[0].created_at - startTime < this.batchWindow
    ) {
      batch.push(this.queue.shift());
    }

    return batch;
  }

  /**
   * Get current queue length
   * @returns {number} Number of events waiting
   */
  length() {
    return this.queue.length;
  }

  /**
   * Clear queue without processing
   */
  clear() {
    this.queue = [];
    if (this.processingTimeout) {
      clearTimeout(this.processingTimeout);
    }
  }

  /**
   * Check if currently processing
   * @returns {boolean}
   */
  isProcessing() {
    return this.processing;
  }
}
