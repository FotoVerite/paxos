/**
 * Paxos Protocol Visualizer
 * Reusable visualization engine for Paxos consensus demonstrations
 */

class PaxosVisualizer {
    constructor(svgElementId, options = {}) {
        this.svg = document.getElementById(svgElementId);
        if (!this.svg) {
            throw new Error(`SVG element with id "${svgElementId}" not found`);
        }

        // Configuration
        this.nodeRadius = options.nodeRadius || 150;
        this.nodeCircleRadius = options.nodeCircleRadius || 20;
        this.nodeCount = 0;
        this.clusterInfo = null;

        // State
        this.nodeElements = {};
        this.center = { x: 0, y: 0 };
        this.eventCounts = {};

        // Event colors - customizable
        this.eventColors = options.eventColors || {
            proposal: '#8b5cf6',  // Purple
            promise: '#fbbf24',   // Amber
            accept: '#f87171',    // Red
            learn: '#34d399',     // Emerald
            nextballot: '#60a5fa', // Blue
            lastvote: '#ec4899',   // Pink
            beginballot: '#f59e0b', // Orange
            voted: '#10b981',      // Green
            success: '#6366f1'     // Indigo
        };

        // SVG setup
        this.setupSVG();
    }

    setupSVG() {
        // Create defs for reusable elements
        const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');

        // Glow filter - with proper bounds to avoid clipping
        const filter = document.createElementNS('http://www.w3.org/2000/svg', 'filter');
        filter.setAttribute('id', 'paxos-glow');
        filter.setAttribute('x', '-50%');
        filter.setAttribute('y', '-50%');
        filter.setAttribute('width', '200%');
        filter.setAttribute('height', '200%');
        filter.innerHTML = `
            <feGaussianBlur stdDeviation="3" result="coloredBlur"/>
            <feMerge>
                <feMergeNode in="coloredBlur"/>
                <feMergeNode in="SourceGraphic"/>
            </feMerge>
        `;
        defs.appendChild(filter);

        // Background gradient
        const gradient = document.createElementNS('http://www.w3.org/2000/svg', 'radialGradient');
        gradient.setAttribute('id', 'paxos-gradient');
        gradient.innerHTML = `
            <stop offset="0%" style="stop-color:#1e40af;stop-opacity:0.3" />
            <stop offset="100%" style="stop-color:#1e40af;stop-opacity:0.1" />
        `;
        defs.appendChild(gradient);

        this.svg.appendChild(defs);
    }

    /**
     * Render the cluster visualization with nodes arranged in a circle
     * @param {Object} clusterInfo - Cluster information including total_nodes
     */
    render(clusterInfo) {
        this.clusterInfo = clusterInfo;
        this.nodeCount = clusterInfo.total_nodes;

        // Check if we already have the right number of nodes
        if (Object.keys(this.nodeElements).length === this.nodeCount) {
            return; // Already rendered, don't re-render and clear beams
        }

        // Clear existing
        this.svg.innerHTML = '';
        this.nodeElements = {};
        this.eventCounts = {};
        
        // Re-add defs
        this.setupSVG();

        // Calculate center
        const rect = this.svg.getBoundingClientRect();
        this.center = { x: rect.width / 2, y: rect.height / 2 };

        // Background
        const bg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        bg.setAttribute('width', rect.width);
        bg.setAttribute('height', rect.height);
        bg.setAttribute('fill', '#0f172a');
        this.svg.appendChild(bg);

        // Message beams layer (added BEFORE nodes so beams appear behind)
        const beamsLayer = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        beamsLayer.setAttribute('id', 'paxos-beams');
        this.svg.appendChild(beamsLayer);
        
        // Ring (optional decorative circle)
        this.drawRing();

        // Place nodes around the circle
        this.placeNodes();
    }

    drawRing() {
        // Outer ring
        const ring = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        ring.setAttribute('cx', this.center.x);
        ring.setAttribute('cy', this.center.y);
        ring.setAttribute('r', this.nodeRadius);
        ring.setAttribute('fill', 'url(#paxos-gradient)');
        ring.setAttribute('stroke', '#3b82f6');
        ring.setAttribute('stroke-width', '2');
        ring.setAttribute('opacity', '0.4');
        this.svg.appendChild(ring);

        // Inner glow
        const glow = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        glow.setAttribute('cx', this.center.x);
        glow.setAttribute('cy', this.center.y);
        glow.setAttribute('r', this.nodeRadius - 5);
        glow.setAttribute('fill', 'none');
        glow.setAttribute('stroke', '#60a5fa');
        glow.setAttribute('stroke-width', '1');
        glow.setAttribute('opacity', '0.2');
        glow.setAttribute('stroke-dasharray', '5,5');
        this.svg.appendChild(glow);
    }

    placeNodes() {
        const angleStep = (Math.PI * 2) / this.nodeCount;

        for (let i = 0; i < this.nodeCount; i++) {
            const angle = angleStep * i - Math.PI / 2;
            const x = this.center.x + this.nodeRadius * Math.cos(angle);
            const y = this.center.y + this.nodeRadius * Math.sin(angle);

            const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            group.setAttribute('id', `paxos-node-${i}`);
            group.setAttribute('data-node-id', i);

            // Glow ring
            const glowRing = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            glowRing.setAttribute('cx', x);
            glowRing.setAttribute('cy', y);
            glowRing.setAttribute('r', this.nodeCircleRadius + 8);
            glowRing.setAttribute('fill', 'none');
            glowRing.setAttribute('stroke', '#60a5fa');
            glowRing.setAttribute('stroke-width', '1');
            glowRing.setAttribute('opacity', '0');
            glowRing.setAttribute('class', 'paxos-node-glow');
            group.appendChild(glowRing);

            // Main circle
            const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            circle.setAttribute('cx', x);
            circle.setAttribute('cy', y);
            circle.setAttribute('r', this.nodeCircleRadius);
            circle.setAttribute('fill', '#3b82f6');
            circle.setAttribute('stroke', '#1e40af');
            circle.setAttribute('stroke-width', '2');
            circle.setAttribute('class', 'paxos-node-circle');
            group.appendChild(circle);

            // Node label
            const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            text.setAttribute('x', x);
            text.setAttribute('y', y);
            text.setAttribute('text-anchor', 'middle');
            text.setAttribute('dominant-baseline', 'middle');
            text.setAttribute('fill', '#fff');
            text.setAttribute('font-weight', 'bold');
            text.setAttribute('font-size', '14');
            text.textContent = i;
            group.appendChild(text);

            // State text
            const stateText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            stateText.setAttribute('x', x);
            stateText.setAttribute('y', y + 35);
            stateText.setAttribute('text-anchor', 'middle');
            stateText.setAttribute('fill', '#94a3b8');
            stateText.setAttribute('font-size', '10');
            stateText.setAttribute('class', 'paxos-node-state');
            stateText.textContent = '--';
            group.appendChild(stateText);

            this.svg.appendChild(group);
            this.nodeElements[i] = { x, y, element: group };

            // Initialize event count for this node
            if (!this.eventCounts[i]) {
                this.eventCounts[i] = {};
            }
        }
    }

    /**
     * Activate a node with a flash animation
     * @param {number} nodeId - Node ID to activate
     * @param {string} color - Color to flash (defaults to blue)
     */
    activateNode(nodeId, color = '#60a5fa') {
        const element = this.nodeElements[nodeId];
        if (!element) return;

        const circle = element.element.querySelector('.paxos-node-circle');
        const glow = element.element.querySelector('.paxos-node-glow');

        // Flash color
        circle.setAttribute('fill', color);
        circle.style.filter = `drop-shadow(0 0 12px ${color})`;
        glow.setAttribute('stroke', color);
        glow.setAttribute('opacity', '0.8');

        // Return to default
        setTimeout(() => {
            circle.setAttribute('fill', '#3b82f6');
            circle.style.filter = '';
            glow.setAttribute('stroke', '#60a5fa');
            glow.setAttribute('opacity', '0');
        }, 300);
    }

    /**
     * Set node color persistently
     * @param {number} nodeId - Node ID to color
     * @param {string} color - Color to set
     */
    setNodeColor(nodeId, color) {
        const element = this.nodeElements[nodeId];
        if (!element) return;

        const circle = element.element.querySelector('.paxos-node-circle');
        if (circle) {
            circle.style.fill = color;
        }
    }

    /**
     * Mark a node as partitioned (grey it out)
     * @param {number} nodeId - Node ID to partition
     */
    setNodePartitioned(nodeId, partitioned = true) {
        const element = this.nodeElements[nodeId];
        if (!element) return;

        const circle = element.element.querySelector('.paxos-node-circle');
        const label = element.element.querySelector('.paxos-node-label');
        if (partitioned) {
            circle.setAttribute('fill', '#4b5563');
            circle.style.opacity = '0.5';
            if (label) label.style.opacity = '0.5';
        } else {
            circle.setAttribute('fill', '#3b82f6');
            circle.style.opacity = '1';
            if (label) label.style.opacity = '1';
        }
    }

    /**
     * Draw beams to multiple target nodes in parallel
     * @param {number} fromId - Source node ID
     * @param {array} toIds - Array of target node IDs
     * @param {string} color - Beam color
     * @param {number} duration - Animation duration in ms (default 500)
     * @param {string} pattern - Line pattern: 'solid', 'dashed', 'dotted' (default 'solid')
     * @param {number} staggerMs - Stagger start time between beams in ms (0 for parallel, default 0)
     * @returns {Promise} - Resolves when all beams complete
     */
    async drawBeamsTo(fromId, toIds, color, duration = 500, pattern = 'solid', staggerMs = 0) {
        if (staggerMs === 0) {
            // Draw all in parallel
            const promises = toIds.map(toId => this.drawBeam(fromId, toId, color, duration, pattern));
            await Promise.all(promises);
        } else {
            // Draw with staggered start times
            const promises = toIds.map((toId, index) => {
                return new Promise(resolve => {
                    setTimeout(() => {
                        this.drawBeam(fromId, toId, color, duration, pattern).then(resolve);
                    }, index * staggerMs);
                });
            });
            await Promise.all(promises);
        }
    }

    /**
     * Draw beams from multiple source nodes to one target node in parallel
     * @param {array} fromIds - Array of source node IDs
     * @param {number} toId - Target node ID
     * @param {string} color - Beam color
     * @param {number} duration - Animation duration in ms (default 500)
     * @param {string} pattern - Line pattern: 'solid', 'dashed', 'dotted' (default 'solid')
     * @param {number} staggerMs - Stagger start time between beams in ms (0 for parallel, default 0)
     * @returns {Promise} - Resolves when all beams complete
     */
    async drawBeamsFrom(fromIds, toId, color, duration = 500, pattern = 'solid', staggerMs = 0) {
        if (staggerMs === 0) {
            // Draw all in parallel
            const promises = fromIds.map(fromId => this.drawBeam(fromId, toId, color, duration, pattern));
            await Promise.all(promises);
        } else {
            // Draw with staggered start times
            const promises = fromIds.map((fromId, index) => {
                return new Promise(resolve => {
                    setTimeout(() => {
                        this.drawBeam(fromId, toId, color, duration, pattern).then(resolve);
                    }, index * staggerMs);
                });
            });
            await Promise.all(promises);
        }
    }

    /**
     * Draw a beam (message) between two nodes
     * @param {number} fromId - Source node ID
     * @param {number} toId - Target node ID
     * @param {string} color - Beam color
     * @param {number} duration - Animation duration in ms (default 500)
     * @param {string} pattern - Line pattern: 'solid', 'dashed', 'dotted' (default 'solid')
     * @returns {Promise} - Resolves when animation completes
     */
    drawBeam(fromId, toId, color, duration = 500, pattern = 'solid') {
        return new Promise((resolve) => {
            const from = this.nodeElements[fromId];
            const to = this.nodeElements[toId];
            if (!from || !to) {
                resolve();
                return;
            }

            let beamsLayer = document.getElementById('paxos-beams');
            
            // Create beams layer if it doesn't exist
            if (!beamsLayer) {
                beamsLayer = document.createElementNS('http://www.w3.org/2000/svg', 'g');
                beamsLayer.setAttribute('id', 'paxos-beams');
                // Insert after background but before nodes
                const firstNode = this.svg.querySelector('[data-node-id]');
                if (firstNode && firstNode.parentNode === this.svg) {
                    this.svg.insertBefore(beamsLayer, firstNode);
                } else {
                    this.svg.appendChild(beamsLayer);
                }
            }
            
            // Calculate control point for bezier curve (offset perpendicular to line)
            const dx = to.x - from.x;
            const dy = to.y - from.y;
            const dist = Math.sqrt(dx*dx + dy*dy);
            const perpX = -dy / dist;
            const perpY = dx / dist;
            const curveOffset = 40 + ((fromId * 7 + toId * 11) % 30); // Varying curve for different beam pairs
            const controlX = from.x + dx/2 + perpX * curveOffset;
            const controlY = from.y + dy/2 + perpY * curveOffset;
            
            // Create animated path (quadratic bezier)
            const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            path.setAttribute('stroke', color);
            path.setAttribute('stroke-width', '3');
            path.setAttribute('opacity', '0.8');
            path.setAttribute('stroke-linecap', 'round');
            path.setAttribute('fill', 'none');

            // Set the full bezier path
            path.setAttribute('d', `M ${from.x} ${from.y} Q ${controlX} ${controlY} ${to.x} ${to.y}`);
            
            // Calculate path length for stroke-dasharray animation
            const pathLength = path.getTotalLength();
            
            // All patterns animate the same way (draw from source to target)
            // Pattern styling applied after animation completes
            path.dataset.pattern = pattern;
            path.setAttribute('stroke-dasharray', pathLength);
            path.setAttribute('stroke-dashoffset', pathLength);
            
            beamsLayer.appendChild(path);

            // Animate beam with duration control
            let progress = 0;
            let timeElapsed = 0;
            const animationInterval = setInterval(() => {
                timeElapsed += 30;
                progress = Math.min(1, timeElapsed / duration);
                
                if (progress >= 1) {
                    progress = 1;
                    clearInterval(animationInterval);
                    path.setAttribute('stroke-dashoffset', 0);
                    
                    // Apply pattern styling after animation completes
                    if (path.dataset.pattern === 'dashed') {
                        path.setAttribute('stroke-dasharray', '8,4');
                    } else if (path.dataset.pattern === 'dotted') {
                        path.setAttribute('stroke-dasharray', '2,3');
                    }
                    
                    // Keep beam visible for a bit longer
                    setTimeout(() => {
                        // Fade out
                        let opacity = 0.9;
                        const fadeInterval = setInterval(() => {
                            opacity -= 0.1;
                            path.setAttribute('opacity', Math.max(0, opacity));
                            if (opacity <= 0) {
                                clearInterval(fadeInterval);
                                path.remove();
                                resolve();
                            }
                        }, 40);
                    }, 300);
                } else {
                    // Animate stroke-dashoffset to reveal the path
                    const dashOffset = pathLength * (1 - progress);
                    path.setAttribute('stroke-dashoffset', dashOffset);
                }
            }, 30);
        });
    }

    /**
     * Update node state text
     * @param {number} nodeId - Node ID
     * @param {string} state - State text to display
     */
    setNodeState(nodeId, state) {
        const element = this.nodeElements[nodeId];
        if (!element) return;
        
        const stateText = element.element.querySelector('.paxos-node-state');
        if (stateText) {
            stateText.textContent = state;
        }
    }

    /**
     * Get event count for a specific event type
     * @param {string} eventType - Event type (e.g., 'proposal', 'promise')
     * @returns {number} - Total count of that event type
     */
    getEventCount(eventType) {
        let total = 0;
        for (const nodeId in this.eventCounts) {
            total += this.eventCounts[nodeId][eventType] || 0;
        }
        return total;
    }

    /**
     * Increment event count
     * @param {number} nodeId - Node ID
     * @param {string} eventType - Event type
     */
    incrementEventCount(nodeId, eventType) {
        if (!this.eventCounts[nodeId]) {
            this.eventCounts[nodeId] = {};
        }
        this.eventCounts[nodeId][eventType] = (this.eventCounts[nodeId][eventType] || 0) + 1;
    }

    /**
     * Reset all event counts
     */
    resetEventCounts() {
        this.eventCounts = {};
        for (let i = 0; i < this.nodeCount; i++) {
            this.eventCounts[i] = {};
        }
    }

    /**
     * Clear all beams from the visualization
     */
    clearBeams() {
        const beamsLayer = document.getElementById('paxos-beams');
        if (beamsLayer) {
            beamsLayer.innerHTML = '';
        }
    }

    /**
     * Handle window resize
     */
    onResize() {
        if (this.clusterInfo) {
            this.render(this.clusterInfo);
        }
    }
}

// Export for use in modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = PaxosVisualizer;
}
