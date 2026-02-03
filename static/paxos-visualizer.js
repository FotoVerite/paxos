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
        this.layoutMode = options.layoutMode || 'circle'; // 'circle' or 'role-grouped'
        this.topologyGradientColor = options.topologyGradientColor || '#1e40af'; // Default blue

        // State
        this.nodeElements = {};
        this.center = { x: 0, y: 0 };
        this.eventCounts = {};
        this.nodeCapabilities = {}; // Store role info per node

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
        
        // Create gradient stops with proper color
        const stop1 = document.createElementNS('http://www.w3.org/2000/svg', 'stop');
        stop1.setAttribute('offset', '0%');
        stop1.setAttribute('style', `stop-color:${this.topologyGradientColor};stop-opacity:0.3`);
        gradient.appendChild(stop1);
        
        const stop2 = document.createElementNS('http://www.w3.org/2000/svg', 'stop');
        stop2.setAttribute('offset', '100%');
        stop2.setAttribute('style', `stop-color:${this.topologyGradientColor};stop-opacity:0.1`);
        gradient.appendChild(stop2);
        
        defs.appendChild(gradient);

        this.svg.appendChild(defs);
    }

    /**
     * Set the layout mode and re-render
     * @param {string} mode - 'circle' or 'role-grouped'
     */
    setLayoutMode(mode) {
        this.layoutMode = mode;
        if (this.clusterInfo) {
            this.render(this.clusterInfo);
        }
    }

    /**
     * Normalize nodeId to number for consistent object key lookup
     */
    #normalizeNodeId(nodeId) {
        return Number(nodeId);
    }

    /**
     * Set node capabilities for role-aware visualization
     */
    setNodeCapabilities(nodeId, roles, learningStrategy) {
        nodeId = this.#normalizeNodeId(nodeId);
        this.nodeCapabilities[nodeId] = { roles, learningStrategy };
    }

    /**
     * Get color based on node roles
     * Full (3 roles): blue
     * Acceptor only: orange
     * Proposer only: purple
     * Learner only: green
     * Mixed (2 roles): gray
     */
    getColorForRoles(roles) {
        if (!roles || roles.length === 0) return '#3b82f6'; // default blue
        if (roles.length === 3) return '#3b82f6'; // full roles - blue
        if (roles.length === 1) {
            const role = roles[0];
            if (role === 'Acceptor') return '#f59e0b';  // orange
            if (role === 'Proposer') return '#8b5cf6';  // purple
            if (role === 'Learner') return '#10b981';   // green
        }
        return '#6b7280'; // gray for mixed 2-role nodes
    }

    /**
     * Render the cluster visualization
     * @param {Object} clusterInfo - Cluster information including total_nodes
     */
    render(clusterInfo) {
        this.clusterInfo = clusterInfo;
        this.nodeCount = clusterInfo.total_nodes;

        this.nodeCount = clusterInfo.total_nodes;

        // Clear existing

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

        // Ring and node placement based on layout mode
        if (this.layoutMode === 'circle') {
            this.drawRing();
            this.placeNodesCircle();
        } else {
            this.placeNodesRoleGrouped();
        }
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

    placeNodesCircle() {
        // Initialize capabilities if empty
        if (Object.keys(this.nodeCapabilities).length === 0) {
            for (let i = 0; i < this.nodeCount; i++) {
                this.nodeCapabilities[i] = { roles: ['Proposer', 'Acceptor', 'Learner'] };
            }
        }

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

            // Main circle - color by roles
            const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            circle.setAttribute('cx', x);
            circle.setAttribute('cy', y);
            circle.setAttribute('r', this.nodeCircleRadius);
            const roleColor = this.getColorForRoles(this.nodeCapabilities[i]?.roles);
            circle.setAttribute('fill', roleColor);
            circle.setAttribute('stroke', '#1e40af');
            circle.setAttribute('stroke-width', '2');
            circle.setAttribute('class', 'paxos-node-circle');
            circle.setAttribute('data-base-color', roleColor); // Store for later restoration
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

    placeNodesRoleGrouped() {
        const groups = {
            proposers: [],
            acceptors: [],
            learners: []
        };

        for (let i = 0; i < this.nodeCount; i++) {
            const caps = this.nodeCapabilities[i] || { roles: ['Proposer', 'Acceptor', 'Learner'] };
            if (caps.roles.includes('Proposer')) groups.proposers.push(i);
            if (caps.roles.includes('Acceptor')) groups.acceptors.push(i);
            if (caps.roles.includes('Learner')) groups.learners.push(i);
        }

        const sectionHeight = 160;
        const startY = 80;

        this.placeRoleGrid(groups.proposers, startY, 'PROPOSERS', '#60a5fa');
        this.placeRoleGrid(groups.acceptors, startY + sectionHeight, 'ACCEPTORS', '#f59e0b');
        this.placeRoleGrid(groups.learners, startY + sectionHeight * 2, 'LEARNERS', '#10b981');
    }

    placeRoleGrid(nodeIds, startY, label, color) {
        const nodesPerRow = 8;
        const nodeSpacing = 80;
        const rowHeight = 90;
        const startX = 100;

        // Section label
        const sectionLabel = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        sectionLabel.setAttribute('x', 20);
        sectionLabel.setAttribute('y', startY - 20);
        sectionLabel.setAttribute('fill', color);
        sectionLabel.setAttribute('font-weight', 'bold');
        sectionLabel.setAttribute('font-size', '14');
        sectionLabel.textContent = label;
        this.svg.appendChild(sectionLabel);

        // Grid line
        const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        line.setAttribute('x1', 20);
        line.setAttribute('y1', startY - 10);
        line.setAttribute('x2', this.svg.getBoundingClientRect().width - 20);
        line.setAttribute('y2', startY - 10);
        line.setAttribute('stroke', color);
        line.setAttribute('stroke-width', '1');
        line.setAttribute('opacity', '0.3');
        this.svg.appendChild(line);

        nodeIds.forEach((nodeId, index) => {
            const col = index % nodesPerRow;
            const row = Math.floor(index / nodesPerRow);
            const x = startX + col * nodeSpacing;
            const y = startY + row * rowHeight;

            this.drawNode(nodeId, x, y, color);
        });
    }

    drawNode(nodeId, x, y, color) {
        const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        group.setAttribute('id', `paxos-node-${nodeId}`);
        group.setAttribute('data-node-id', nodeId);

        // Determine node color based on roles
        const caps = this.nodeCapabilities[nodeId];
        let nodeFill = '#3b82f6'; // Default Blue
        let nodeStroke = '#1e40af';

        if (caps) {
            const roles = caps.roles;
            if (roles.includes('Proposer') && roles.includes('Acceptor') && roles.includes('Learner')) {
                nodeFill = '#6366f1'; // Indigo for full nodes
                nodeStroke = '#4338ca';
            } else if (roles.includes('Proposer')) {
                nodeFill = '#3b82f6'; // Blue
                nodeStroke = '#1e40af';
            } else if (roles.includes('Acceptor')) {
                nodeFill = '#f59e0b'; // Amber
                nodeStroke = '#b45309';
            } else if (roles.includes('Learner')) {
                nodeFill = '#10b981'; // Emerald
                nodeStroke = '#047857';
            }
        }

        // Color can be overridden by param (used in Grid layout sections)
        if (color && this.layoutMode === 'role-grouped') {
            nodeFill = color;
            nodeStroke = color;
        }

        // Node circle
        const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circle.setAttribute('cx', x);
        circle.setAttribute('cy', y);
        circle.setAttribute('r', this.nodeCircleRadius);
        circle.setAttribute('fill', nodeFill);
        circle.setAttribute('stroke', nodeStroke);
        circle.setAttribute('stroke-width', '2');
        circle.setAttribute('class', 'paxos-node-circle');
        circle.setAttribute('data-base-color', nodeFill); // Store for later restoration
        group.appendChild(circle);

        // Label
        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', x);
        text.setAttribute('y', y);
        text.setAttribute('text-anchor', 'middle');
        text.setAttribute('dominant-baseline', 'middle');
        text.setAttribute('fill', '#fff');
        text.setAttribute('font-weight', 'bold');
        text.setAttribute('font-size', '12');
        text.textContent = nodeId;
        group.appendChild(text);

        // State text
        const stateText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        stateText.setAttribute('x', x);
        stateText.setAttribute('y', y + 30);
        stateText.setAttribute('text-anchor', 'middle');
        stateText.setAttribute('fill', '#94a3b8');
        stateText.setAttribute('font-size', '9');
        stateText.setAttribute('class', 'paxos-node-state');
        stateText.textContent = '--';
        group.appendChild(stateText);

        // Render role badges
        this.renderNodeBadges(group, x, y, nodeId);

        this.svg.appendChild(group);
        this.nodeElements[nodeId] = { x, y, element: group };

        if (!this.eventCounts[nodeId]) {
            this.eventCounts[nodeId] = {};
        }
    }

    renderNodeBadges(group, x, y, nodeId) {
        const caps = this.nodeCapabilities[nodeId] || { roles: ['Proposer', 'Acceptor', 'Learner'] };
        const badgeY = y - 38;
        const badgeSize = 16;
        const badgeSpacing = 18;

        const roles = [
            { id: 'Proposer', label: 'P', color: '#60a5fa' },
            { id: 'Acceptor', label: 'A', color: '#f59e0b' },
            { id: 'Learner', label: 'L', color: '#10b981' }
        ];

        let activeRoles = roles.filter(r => caps.roles.includes(r.id));
        const totalWidth = activeRoles.length * badgeSpacing;
        let startX = x - totalWidth / 2 + badgeSize / 2;

        activeRoles.forEach((role, i) => {
            const rx = startX + i * badgeSpacing;

            const badge = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
            badge.setAttribute('x', rx - badgeSize / 2);
            badge.setAttribute('y', badgeY - badgeSize / 2);
            badge.setAttribute('width', badgeSize);
            badge.setAttribute('height', badgeSize);
            badge.setAttribute('fill', role.color);
            badge.setAttribute('stroke', '#0f172a');
            badge.setAttribute('stroke-width', '1');
            badge.setAttribute('rx', 3);
            group.appendChild(badge);

            const badgeText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            badgeText.setAttribute('x', rx);
            badgeText.setAttribute('y', badgeY);
            badgeText.setAttribute('text-anchor', 'middle');
            badgeText.setAttribute('dominant-baseline', 'middle');
            badgeText.setAttribute('fill', '#fff');
            badgeText.setAttribute('font-size', '11');
            badgeText.setAttribute('font-weight', 'bold');
            badgeText.textContent = role.label;
            group.appendChild(badgeText);
        });
    }

    /**
     * Activate a node with a flash animation
     * @param {number} nodeId - Node ID to activate
     * @param {string} color - Color to flash (defaults to blue)
     */
    activateNode(nodeId, color = '#60a5fa') {
        nodeId = this.#normalizeNodeId(nodeId);
        const element = this.nodeElements[nodeId];
        if (!element) return;

        const circle = element.element.querySelector('.paxos-node-circle');
        const glow = element.element.querySelector('.paxos-node-glow');

        circle.setAttribute('fill', color);
        circle.style.filter = `drop-shadow(0 0 12px ${color})`;
        glow.setAttribute('stroke', color);
        glow.setAttribute('opacity', '0.8');
    }

    /**
     * Set node color persistently
     * @param {number} nodeId - Node ID to color
     * @param {string} color - Color to set
     */
    setNodeColor(nodeId, color) {
        nodeId = this.#normalizeNodeId(nodeId);
        const element = this.nodeElements[nodeId];
        if (!element) return;

        const circle = element.element.querySelector('.paxos-node-circle');
        if (circle) {
            circle.style.fill = color;
        }
    }

    /**
     * Reset node to its topology color based on roles
     * @param {number} nodeId - Node ID to reset
     */
    resetNodeToRoleColor(nodeId) {
        nodeId = this.#normalizeNodeId(nodeId);
        const element = this.nodeElements[nodeId];
        if (!element) return;

        const circle = element.element.querySelector('.paxos-node-circle');
        if (!circle) return;

        const roles = this.nodeCapabilities[nodeId]?.roles || [];
        const color = this.getColorForRoles(roles);
        circle.setAttribute('fill', color);
        circle.style.filter = '';

        const glow = element.element.querySelector('.paxos-node-glow');
        if (glow) {
            glow.setAttribute('stroke', '#60a5fa');
            glow.setAttribute('opacity', '0');
        }
    }

    /**
     * Mark a node as partitioned (grey it out)
     * @param {number} nodeId - Node ID to partition
     */
    setNodePartitioned(nodeId, partitioned = true) {
        nodeId = this.#normalizeNodeId(nodeId);
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
     * Mark a node as the leader with a bright border and glow
     * @param {number} nodeId - Node ID to mark as leader
     */
    setLeader(nodeId) {
        nodeId = this.#normalizeNodeId(nodeId);
        const element = this.nodeElements[nodeId];
        if (!element) return;

        const circle = element.element.querySelector('.paxos-node-circle');
        if (circle) {
            circle.setAttribute('stroke', '#fbbf24');
            circle.setAttribute('stroke-width', '4');
            circle.style.filter = 'drop-shadow(0 0 8px #fbbf24)';
        }
    }

    /**
     * Remove leader marking from a node
     * @param {number} nodeId - Node ID to unmark as leader
     */
    clearLeader(nodeId) {
        nodeId = this.#normalizeNodeId(nodeId);
        const element = this.nodeElements[nodeId];
        if (!element) return;

        const circle = element.element.querySelector('.paxos-node-circle');
        if (circle) {
            circle.setAttribute('stroke', '#64748b');
            circle.setAttribute('stroke-width', '2');
            circle.style.filter = 'none';
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
            fromId = this.#normalizeNodeId(fromId);
            toId = this.#normalizeNodeId(toId);
            
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
            const dist = Math.sqrt(dx * dx + dy * dy);
            const perpX = -dy / dist;
            const perpY = dx / dist;
            const curveOffset = 40 + ((fromId * 7 + toId * 11) % 30); // Varying curve for different beam pairs
            const controlX = from.x + dx / 2 + perpX * curveOffset;
            const controlY = from.y + dy / 2 + perpY * curveOffset;

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
        nodeId = this.#normalizeNodeId(nodeId);
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
