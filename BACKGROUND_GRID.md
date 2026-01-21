# Background Grid Animation

A subtle, morphing grid animation with cool blues, purples, and hints of hot pink for ambient visual interest without distraction.

## Overview

The background grid creates an abstract, space-like ambiance with:
- Morphing gradient colors (cool blues, purples, subtle hot pink at bottom)
- Subtle grid pattern (represents Paxos nodes/consensus)
- Smooth, slow animation (~30% speed factor)
- Low opacity to stay in background
- Fixed background so content scrolls over it

## Usage

### Enable on a Page

Add `data-background-grid` attribute to the `<body>` tag in any page template:

```html
{% extends "base.html" %}

{% block background_grid %}data-background-grid{% endblock %}

{% block content %}
    <!-- Your content here -->
{% endblock %}
```

### Disable on a Page

Simply omit the `{% block background_grid %}` or leave it empty:

```html
{% extends "base.html" %}

{% block content %}
    <!-- Content without animation -->
{% endblock %}
```

## Customization

Edit `/static/js/background-grid.js` to adjust:

- **Speed**: Change `this.speed = 0.3` (lower = slower)
- **Grid size**: Change `this.gridSize = 60` (in pixels)
- **Colors**: Modify the `this.colors` object
- **Opacity**: Adjust `cellIntensity` calculations (currently 0.3-0.5)
- **Hot pink intensity**: Modify the `yPercent > 0.7` section for more/less pink at bottom

## Performance

- Uses requestAnimationFrame for smooth 60fps animation
- Canvas-based rendering is efficient
- Fixed positioning means it doesn't affect layout
- Can be disabled per-page with zero overhead

## Files

- `/static/css/animations/background-grid.css` - Styling & container
- `/static/js/background-grid.js` - Animation logic (standalone, ~7KB)
- `templates/base.html` - Updated with `background_grid` block

## Notes

- The grid pattern is intentionally abstract to represent distributed consensus
- Colors are chosen to work with existing Paxos vaporwave aesthetic
- Animation runs indefinitely; consider disabling on pages where users need full focus (e.g., long-form reading)
