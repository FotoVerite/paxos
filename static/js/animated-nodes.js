/**
 * Animated Nodes - Creates pulsing/glowing nodes that form "PAXOS" text
 * Uses canvas for smooth animation with vaporwave aesthetic
 */

class AnimatedNodes {
    constructor() {
        this.canvas = document.getElementById('nodeCanvas');
        this.ctx = this.canvas.getContext('2d');
        this.nodes = [];
        this.particles = [];
        this.time = 0;
        
        this.setupCanvas();
        this.initializeNodes();
        this.animate();
        
        // Handle window resize
        window.addEventListener('resize', () => this.setupCanvas());
    }
    
    setupCanvas() {
        this.canvas.width = window.innerWidth;
        this.canvas.height = window.innerHeight;
        // Re-initialize canvas context after resize
        this.ctx = this.canvas.getContext('2d');
    }
    
    initializeNodes() {
        // Create nodes in a grid pattern that forms "PAXOS" letters
        const nodeRadius = 4;
        const spacing = 20;
        const centerX = this.canvas.width / 2;
        const centerY = this.canvas.height / 2 - 130; // Moved up 50px from -80
        
        // Define letter patterns (simplified)
        // Each letter is a grid of points that will become glowing nodes
        const letters = this.getLetterPatterns(spacing);
        
        // Calculate total width needed for all letters
        let totalWidth = 0;
        letters.forEach((letterPoints) => {
            const maxX = Math.max(...letterPoints.map(p => p[0]));
            totalWidth += (maxX + 1) * spacing + 30; // +30 for letter spacing
        });
        
        // Start position to center all letters
        let currentX = centerX - (totalWidth / 2);
        
        letters.forEach((letterPoints) => {
            letterPoints.forEach(point => {
                const x = currentX + point[0] * spacing;
                const y = centerY + point[1] * spacing;
                
                this.nodes.push({
                    x: x,
                    y: y,
                    baseX: x,
                    baseY: y,
                    radius: nodeRadius,
                    opacity: 0.8,
                    pulse: Math.random() * Math.PI * 2,
                    color: this.getRandomVaporwaveColor(),
                    connected: [],
                    vx: (Math.random() - 0.5) * 0.1,
                    vy: (Math.random() - 0.5) * 0.1,
                    cycleOffset: Math.random() * Math.PI * 2, // Random cycle start time
                    pulseSpeed: 1.5 + Math.random() * 1.5, // Random pulse speed (1.5 to 3)
                    wobbleSpeed: 0.8 + Math.random() * 0.6, // Random wobble speed (0.8 to 1.4)
                    wobbleAmount: 1.5 + Math.random() * 2, // Random wobble magnitude (1.5 to 3.5)
                });
            });
            
            // Move to next letter position
            const maxX = Math.max(...letterPoints.map(p => p[0]));
            currentX += (maxX + 1) * spacing + 30;
        });
        
        // Calculate connections between nearby nodes (only adjacent nodes)
        this.nodes.forEach((node, i) => {
            this.nodes.forEach((otherNode, j) => {
                if (i !== j) {
                    const distance = Math.hypot(
                        node.baseX - otherNode.baseX,
                        node.baseY - otherNode.baseY
                    );
                    // Only connect to immediate neighbors (within one diagonal)
                    if (distance < spacing * 1.5) {
                        node.connected.push(j);
                    }
                }
            });
        });
    }
    
    getLetterPatterns(spacing) {
        // Simplified node patterns - connections form the letters
        
        // P - minimal nodes with clean lines
        const P = [
            [0, 0], [1, 0], [2, 0],
            [0, 1],             [3, 1],
            [0, 2], [1, 2], [2, 2],
            [0, 3],
            [0, 4],
        ];
        
        // A - triangle structure with middle connector
        const A = [
            [2, 0],
            [1, 1], [2.5, 1],
            [0.8, 2],  [3.2, 2],
            [0.7, 3],  [2, 3],[3.3, 3],
            [0.5, 4],       [3.7, 4],
        ];
        
        // X - simple cross
        const X = [
            [0, 0],       [4, 0],
            [1, 1],   [3, 1],
            [2, 2],
            [1, 3],   [3, 3],
            [0, 4],       [4, 4],
        ];
        
        // O - simple circle
        const O = [
            [1, 0], [2, 0], [3, 0],
            [0, 1],             [4, 1],
            [0, 2],             [4, 2],
            [0, 3],             [4, 3],
            [1, 4], [2, 4], [3, 4],
        ];
        
        // S - clean S with fewer nodes
        const S = [
            [1, 0], [2, 0],
            [0, 1],
            [1, 2], [2, 2],
                        [3, 3],
            [0, 4], [1, 4], [2, 4],
        ];
        
        return [P, A, X, O, S];
    }
    
    getRandomVaporwaveColor() {
        // All pink for consistent vaporwave aesthetic
        return 'rgba(255, 0, 110, 0.8)';  // Hot pink
    }
    
    update() {
        this.time += 0.016; // ~60fps increment
        
        this.nodes.forEach((node) => {
            // Wobble with per-node randomized speed and amount
            const wobblePhase = this.time * node.wobbleSpeed + node.cycleOffset;
            const wobbleX = Math.sin(wobblePhase) * node.wobbleAmount;
            const wobbleY = Math.cos(wobblePhase * 0.75) * node.wobbleAmount;
            
            node.x = node.baseX + wobbleX;
            node.y = node.baseY + wobbleY;
            
            // Randomized pulsing - each node has its own cycle speed
            // Sin: -1 to 1 → multiply by 0.35 → add 0.55 → gives 0.2 to 0.9
            const pulsePhase = this.time * node.pulseSpeed + node.cycleOffset;
            const sineValue = Math.sin(pulsePhase);
            node.opacity = 0.55 + sineValue * 0.35;
        });
    }
    
    draw() {
        // Clear canvas completely
        this.ctx.fillStyle = 'rgba(10, 14, 39, 1)';
        this.ctx.fillRect(0, 0, this.canvas.width, this.canvas.height);
        
        // Draw connections with flowing pulse animation
        this.nodes.forEach((node, nodeIndex) => {
            node.connected.forEach((connectedIndex) => {
                const connectedNode = this.nodes[connectedIndex];
                
                // Base connection line with soft glow
                const baseOpacity = 0.15;
                this.ctx.strokeStyle = `rgba(255, 0, 110, ${baseOpacity})`;
                this.ctx.lineWidth = 1;
                this.ctx.beginPath();
                this.ctx.moveTo(node.x, node.y);
                this.ctx.lineTo(connectedNode.x, connectedNode.y);
                this.ctx.stroke();
                
                // Flowing pulse along the line - randomized per connection
                const flowSpeed = 3;
                const flowPhase = this.time * flowSpeed + node.cycleOffset + connectedNode.cycleOffset * 0.5;
                const flowPosition = (flowPhase) % 1;
                const startX = node.x + (connectedNode.x - node.x) * flowPosition;
                const startY = node.y + (connectedNode.y - node.y) * flowPosition;
                
                // Bright pulse marker - randomized per connection
                const pulsePhase = this.time * 4 + node.cycleOffset;
                const pulseOpacity = Math.sin(pulsePhase) * 0.3 + 0.4;
                this.ctx.fillStyle = `rgba(255, 0, 110, ${pulseOpacity})`;
                this.ctx.beginPath();
                this.ctx.arc(startX, startY, 1.5, 0, Math.PI * 2);
                this.ctx.fill();
                
                // Glow around pulse
                const glowGradient = this.ctx.createRadialGradient(startX, startY, 0, startX, startY, 4);
                glowGradient.addColorStop(0, `rgba(255, 0, 110, ${pulseOpacity * 0.5})`);
                glowGradient.addColorStop(1, 'rgba(255, 0, 110, 0)');
                this.ctx.fillStyle = glowGradient;
                this.ctx.beginPath();
                this.ctx.arc(startX, startY, 4, 0, Math.PI * 2);
                this.ctx.fill();
            });
        });
        
        // Draw nodes - all pink
        this.nodes.forEach((node) => {
            // Glow effect
            const glowSize = node.radius * 2.5;
            const gradient = this.ctx.createRadialGradient(node.x, node.y, 0, node.x, node.y, glowSize);
            gradient.addColorStop(0, `rgba(255, 0, 110, ${node.opacity * 0.3})`);
            gradient.addColorStop(0.6, `rgba(255, 0, 110, ${node.opacity * 0.1})`);
            gradient.addColorStop(1, 'rgba(255, 0, 110, 0)');
            
            this.ctx.fillStyle = gradient;
            this.ctx.beginPath();
            this.ctx.arc(node.x, node.y, glowSize, 0, Math.PI * 2);
            this.ctx.fill();
            
            // Core node
            this.ctx.fillStyle = `rgba(255, 0, 110, ${node.opacity})`;
            this.ctx.beginPath();
            this.ctx.arc(node.x, node.y, node.radius, 0, Math.PI * 2);
            this.ctx.fill();
            
            // Bright center
            this.ctx.fillStyle = `rgba(255, 255, 255, ${node.opacity * 0.9})`;
            this.ctx.beginPath();
            this.ctx.arc(node.x, node.y, node.radius * 0.5, 0, Math.PI * 2);
            this.ctx.fill();
        });
    }
    
    animate = () => {
        this.update();
        this.draw();
        requestAnimationFrame(this.animate);
    }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    new AnimatedNodes();
});
