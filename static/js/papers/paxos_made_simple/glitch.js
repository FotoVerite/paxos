// PMS Glitch effect for "Simple"

const glitchWords = [
    'Simple',
    'Hard',
    'Complex',
    'Difficult',
    'Impossible',
    'Easy',
    'Elegant',
    'Consensus',
    'Distributed',
    'Reliable'
];

let currentWordIndex = 0;
let glitchContainer = null;

function initGlitch() {
    glitchContainer = document.querySelector('.word-glitch');
    if (!glitchContainer) return;

    // Start the glitch cycle
    glitchCycle();
}

function createGlitchWord(word) {
    const container = document.createElement('div');
    container.className = 'glitch-word';
    
    for (let char of word) {
        const span = document.createElement('span');
        span.className = 'glitch-char';
        span.setAttribute('data-char', char);
        span.textContent = char;
        container.appendChild(span);
    }
    
    return container;
}

function glitchTransition() {
    const word = glitchWords[currentWordIndex];
    const newWord = createGlitchWord(word);
    
    // Clear container
    glitchContainer.innerHTML = '';
    glitchContainer.appendChild(newWord);
    
    // Trigger animation on each character
    const chars = newWord.querySelectorAll('.glitch-char');
    chars.forEach((char, index) => {
        char.style.animationDelay = `${index * 0.05}s`;
    });
}

function glitchCycle() {
    glitchTransition();
    
    // Move to next word after a delay
    currentWordIndex = (currentWordIndex + 1) % glitchWords.length;
    
    // Random delay between 2-4 seconds before next glitch
    const delay = Math.random() * 2000 + 2000;
    setTimeout(glitchCycle, delay);
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', initGlitch);

// Also handle dynamic page loads
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initGlitch);
} else {
    initGlitch();
}
