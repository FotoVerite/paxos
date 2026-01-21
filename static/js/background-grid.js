/**
 * Morphing Z-Axis Grid Background Animation
 * Creates a grid with organic morphing and depth layering
 * 
 * Usage: Add `data-background-grid` attribute to body to enable
 * Example: <body data-background-grid>
 */

class BackgroundGrid {
    constructor() {
        this.canvas = null;
        this.ctx = null;
        this.animationId = null;
        this.time = 0;
        this.speed = 0.3;
        
        // Color palette
        this.colors = {
            primary: { r: 96, g: 165, b: 250 },      // #60a5fa - cool blue
            secondary: { r: 139, g: 92, b: 246 },    // #8b5cf6 - purple
            tertiary: { r: 59, g: 130, b: 246 },     // #3b82f6 - deeper blue
            accent: { r: 255, g: 0, b: 110 },        // #ff006e - hot pink
        };
        
        this.gridSizeX = 100;
        this.gridSizeY = 120; // Larger vertical spacing to compensate for perspective
        this.layers = 3; // Depth layers
        this.cellCount = { x: 0, y: 0 };
        
        this.init();
    }
    
    init() {
        if (!document.body.hasAttribute('data-background-grid')) {
            return;
        }
        
        this.setupCanvas();
        this.setupEventListeners();
        this.animate();
    }
    
    setupCanvas() {
        let container = document.querySelector('.background-grid');
        if (!container) {
            container = document.createElement('div');
            container.className = 'background-grid';
            document.body.insertBefore(container, document.body.firstChild);
        }
        
        this.canvas = document.getElementById('gridCanvas');
        if (!this.canvas) {
            this.canvas = document.createElement('canvas');
            this.canvas.id = 'gridCanvas';
            container.appendChild(this.canvas);
        }
        
        this.ctx = this.canvas.getContext('2d');
        this.resizeCanvas();
        document.body.classList.add('has-grid-background');
    }
    
    resizeCanvas() {
        this.canvas.width = window.innerWidth;
        this.canvas.height = window.innerHeight;
        this.cellCount.x = Math.ceil(this.canvas.width / this.gridSizeX) + 2;
        this.cellCount.y = Math.ceil(this.canvas.height / this.gridSizeY) + 3;
    }
    
    setupEventListeners() {
        window.addEventListener('resize', () => this.resizeCanvas());
    }
    
    /**
     * Get morphing color based on position and time
     */
    getColor(x, y, layer) {
        const colors = Object.values(this.colors);
        const phase = this.time * this.speed * 0.05;
        const spatialPhase = (x + y) * 0.1;
        const layerPhase = layer * 0.3;
        
        const cycleTime = phase + spatialPhase + layerPhase;
        const colorIndex = Math.floor(cycleTime) % colors.length;
        const nextIndex = (colorIndex + 1) % colors.length;
        const blend = cycleTime % 1;
        
        const c1 = colors[colorIndex];
        const c2 = colors[nextIndex];
        
        return {
            r: this.lerp(c1.r, c2.r, blend),
            g: this.lerp(c1.g, c2.g, blend),
            b: this.lerp(c1.b, c2.b, blend),
        };
    }
    
    lerp(a, b, t) {
        return a + (b - a) * t;
    }
    
    /**
     * Organic noise function for morphing
     */
    noise(x, y, z, time) {
        const n1 = Math.sin(x * 0.02 + time * 0.08) * 0.5 + 0.5;
        const n2 = Math.sin(y * 0.02 + time * 0.1) * 0.5 + 0.5;
        const n3 = Math.sin((x + y) * 0.01 + z * 0.3 + time * 0.06) * 0.5 + 0.5;
        return (n1 + n2 + n3) / 3;
    }
    
    /**
     * Calculate grid point position with organic morphing and perspective
     */
    getGridPoint(gridX, gridY, layer, depthFactor) {
        const vanishingX = this.canvas.width / 2;
        const vanishingY = this.canvas.height * 0.8; // Vanishing point near bottom
        
        const baseX = gridX * this.gridSizeX;
        const baseY = gridY * this.gridSizeY;
        
        // Perspective: closer to vanishing point = more scaled down
        const relativeY = baseY - vanishingY;
        const perspective = relativeY / (this.canvas.height - vanishingY);
        
        // Converge towards vanishing point
        const perspectiveX = vanishingX + (baseX - vanishingX) * perspective;
        const perspectiveY = vanishingY + (baseY - vanishingY) * perspective;
        
        // Morphing offset based on noise
        const morph = this.noise(gridX, gridY, layer, this.time);
        const offsetMagnitude = 20 * depthFactor * perspective;
        const offsetX = Math.sin(this.time * 0.02 + gridY) * offsetMagnitude * morph;
        const offsetY = Math.cos(this.time * 0.02 + gridX) * offsetMagnitude * morph;
        
        return {
            x: perspectiveX + offsetX,
            y: perspectiveY + offsetY,
        };
    }
    
    /**
     * Draw a grid cell with morphing and depth
     */
    drawCell(gridX, gridY, layer) {
        const yPercent = (gridY * this.gridSizeY) / this.canvas.height;
        
        // Only render in bottom 30% of page
        if (yPercent < 0.68) {
            return;
        }
        
        // Fade in towards bottom
        const fadeIn = Math.min(1, (yPercent - 0.68) / 0.32);
        
        const depthFactor = (layer + 1) / this.layers;
        
        // Get morphed grid points for this cell
        const p1 = this.getGridPoint(gridX, gridY, layer, depthFactor);
        const p2 = this.getGridPoint(gridX + 1, gridY, layer, depthFactor);
        const p3 = this.getGridPoint(gridX + 1, gridY + 1, layer, depthFactor);
        const p4 = this.getGridPoint(gridX, gridY + 1, layer, depthFactor);
        
        // Get color for this cell
        const color = this.getColor(gridX, gridY, layer);
        
        // Draw morphed cell as quadrilateral
        this.ctx.fillStyle = `rgba(${Math.round(color.r)}, ${Math.round(color.g)}, ${Math.round(color.b)}, 0.6)`;
        this.ctx.strokeStyle = `rgba(${Math.round(color.r)}, ${Math.round(color.g)}, ${Math.round(color.b)}, 0.8)`;
        this.ctx.lineWidth = 0.5 + depthFactor * 0.5;
        
        this.ctx.beginPath();
        this.ctx.moveTo(p1.x, p1.y);
        this.ctx.lineTo(p2.x, p2.y);
        this.ctx.lineTo(p3.x, p3.y);
        this.ctx.lineTo(p4.x, p4.y);
        this.ctx.closePath();
        
        // Apply depth and fade
        this.ctx.globalAlpha = 0.5 * depthFactor * fadeIn;
        this.ctx.fill();
        
        this.ctx.globalAlpha = 0.6 * depthFactor * fadeIn;
        this.ctx.stroke();
    }
    
    /**
     * Main animation loop
     */
    animate = () => {
        // Clear canvas
        this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);
        
        // Draw layers from back to front
        for (let layer = 0; layer < this.layers; layer++) {
            for (let y = -1; y < this.cellCount.y; y++) {
                for (let x = -1; x < this.cellCount.x; x++) {
                    this.drawCell(x, y, layer);
                }
            }
        }
        
        // Reset globalAlpha
        this.ctx.globalAlpha = 1;
        
        this.time += 1;
        this.animationId = requestAnimationFrame(this.animate);
    }
    
    /**
     * Stop animation and cleanup
     */
    destroy() {
        if (this.animationId) {
            cancelAnimationFrame(this.animationId);
        }
        if (this.canvas) {
            this.canvas.remove();
        }
        document.body.classList.remove('has-grid-background');
    }
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        window.backgroundGrid = new BackgroundGrid();
    });
} else {
    window.backgroundGrid = new BackgroundGrid();
}
