/**
 * Decree Simulator - Interactive Paxos decree lifecycle visualization
 * 
 * Usage:
 *   const sim = new DecreeSimulator({
 *       containerId: 'ballotContainer',
 *       totalPriests: 5,
 *       priestNames: ['Cicero', 'Marcus', 'Pompey', 'Brutus', 'Augustus'],
 *       onBallotStarted: (ballot) => {},
 *       onBallotFinalized: (ballot) => {}
 *   });
 *   sim.startNewBallot();
 */

class DecreeSimulator {
    constructor(options = {}) {
        // Configuration
        this.totalPriests = options.totalPriests || 5;
        this.quorumSize = options.quorumSize || Math.ceil(this.totalPriests / 2) + 1;
        this.priestNames = options.priestNames || ['Cicero', 'Marcus', 'Pompey', 'Brutus', 'Augustus'];
        this.priestParticipation = options.priestParticipation || [0.9, 0.8, 0.85, 0.95, 0.7];
        this.wordChain = options.wordChain || this._defaultWordChain();
        
        // DOM containers
        this.ballotContainerId = options.ballotContainerId;
        this.learnedListId = options.learnedListId;
        this.historyContainerId = options.historyContainerId;
        this.currentProposalId = options.currentProposalId;
        this.ballotsTried = options.ballotsTried;
        this.decreeValueId = options.decreeValueId;
        this.decreeStatusId = options.decreeStatusId;
        this.voteCountId = options.voteCountId;
        
        // State
        this.ballotHistory = [];
        this.currentBallotData = null;
        this.learnedValue = null;
        this.ballotCounter = 0;
        this.autoRetryTimeout = null;
        this.lastRenderedBallotCount = 0;
        this.statusLocked = false; // Prevent status from being overwritten once chosen
        
        // Callbacks
        this.onBallotStarted = options.onBallotStarted || (() => {});
        this.onBallotFinalized = options.onBallotFinalized || (() => {});
        this.onPriestVoted = options.onPriestVoted || (() => {});
        this.onDisplayUpdated = options.onDisplayUpdated || (() => {});
    }
    
    /**
     * Default word chain for decree generation (Latin-inspired)
     */
    _defaultWordChain() {
        return {
            "": ["Knowledge", "Virtue", "Strength", "Power", "Unity", "Wisdom", "Justice"],
            "Knowledge": ["is", "brings", "demands", "endures"],
            "Virtue": ["is", "endures", "conquers", "prevails"],
            "Strength": ["is", "conquers", "endures", "brings"],
            "Power": ["endures", "is", "conquers", "prevails"],
            "Unity": ["is", "brings", "strength", "peace"],
            "Wisdom": ["is", "brings", "guides", "prevails"],
            "Justice": ["is", "prevails", "brings", "endures"],
            "is": ["eternal", "paramount", "sacred", "divine", "absolute"],
            "brings": ["peace", "order", "strength", "wisdom", "honor"],
            "demands": ["sacrifice", "unity", "strength", "commitment"],
            "endures": ["forever", "eternal", "supreme"],
            "conquers": ["weakness", "chaos", "fear", "doubt"],
            "prevails": ["always", "supreme", "eternal"],
            "strength": ["eternal", "supreme"],
            "peace": ["eternal", "absolute"],
            "order": ["supreme", "eternal"],
            "guides": ["all", "forever"],
            "eternal": null,
            "paramount": null,
            "sacred": null,
            "divine": null,
            "absolute": null,
            "forever": null,
            "supreme": null,
            "weakness": null,
            "chaos": null,
            "fear": null,
            "doubt": null,
            "always": null,
            "peace": null,
            "order": null,
            "wisdom": null,
            "honor": null,
            "sacrifice": null,
            "unity": null,
            "strength": null,
            "commitment": null,
            "all": null
        };
    }
    
    /**
     * Generate a random decree using Markov chain
     */
    generateDecree() {
        let decree = '';
        let currentWord = '';
        
        while (true) {
            const nextOptions = this.wordChain[currentWord];
            if (!nextOptions) break;
            
            const nextWord = nextOptions[Math.floor(Math.random() * nextOptions.length)];
            if (decree) decree += ' ';
            decree += nextWord;
            currentWord = nextWord;
        }
        
        return decree.charAt(0).toUpperCase() + decree.slice(1);
    }
    
    /**
     * Get the current state
     */
    getState() {
        return {
            totalPriests: this.totalPriests,
            quorumSize: this.quorumSize,
            ballotHistory: this.ballotHistory,
            currentBallotData: this.currentBallotData,
            learnedValue: this.learnedValue,
            ballotCounter: this.ballotCounter,
            hasConsensus: this.learnedValue !== null
        };
    }
    
    /**
     * Check if consensus has been reached
     */
    hasConsensus() {
        return this.learnedValue !== null;
    }
    
    /**
     * Start a new ballot
     */
    startNewBallot() {
        if (this.autoRetryTimeout) clearTimeout(this.autoRetryTimeout);
        
        const proposedDecree = this.generateDecree();
        this.ballotCounter++;
        const ballotNumber = this.ballotHistory.length + 1;
        
        this.currentBallotData = {
            number: ballotNumber,
            decree: proposedDecree,
            votes: [],
            startTime: Date.now()
        };
        
        this._updateDisplay();
        this.onBallotStarted(this.currentBallotData);
        
        // Simulate staggered votes
        this._simulateVotes();
    }
    
    /**
     * Simulate staggered priest votes
     */
    _simulateVotes() {
        for (let i = 0; i < this.totalPriests; i++) {
            const voteDelay = 300 + (i * 180) + Math.random() * 120;
            
            setTimeout(() => {
                if (!this.currentBallotData) return; // Ballot was reset
                
                // Check if priest votes (based on participation rate)
                const voteProbability = this.priestParticipation[i];
                if (Math.random() < voteProbability) {
                    this._recordVote(i);
                }
                
                // Finalize after last priest responds
                if (i === this.totalPriests - 1) {
                    setTimeout(() => this._finalizeBallot(), 200);
                }
            }, voteDelay);
        }
    }
    
    /**
     * Record a vote from a priest
     */
    _recordVote(priestId) {
        if (!this.currentBallotData || this.currentBallotData.votes.includes(priestId)) {
            return;
        }
        
        this.currentBallotData.votes.push(priestId);
        this.onPriestVoted({
            priestId,
            priestName: this.priestNames[priestId],
            ballotNumber: this.currentBallotData.number
        });
        
        this._updateDisplay();
    }
    
    /**
     * Finalize the current ballot
     */
    _finalizeBallot() {
        if (!this.currentBallotData) return;
        
        const voteCount = this.currentBallotData.votes.length;
        const isSuccess = voteCount >= this.quorumSize;
        
        if (isSuccess && !this.learnedValue) {
            this.learnedValue = this.currentBallotData.decree;
            this.currentBallotData.learned = true;
        }
        
        this.ballotHistory.push(this.currentBallotData);
        this.onBallotFinalized(this.currentBallotData);
        
        // Keep current ballot data in memory for display purposes, don't clear it
        this._updateDisplay();
    }
    
    /**
     * Update all DOM displays
     */
    _updateDisplay() {
        this._updateCurrentProposal();
        this._updateBallotCount();
        this._updateDecreeValue();
        this._updateDecreeStatus();
        this._updateLearnedList();
        this.onDisplayUpdated(this.getState());
    }
    
    /**
     * Update current proposal display
     */
    _updateCurrentProposal() {
        if (!this.currentProposalId) return;
        const el = document.getElementById(this.currentProposalId);
        if (!el) return;
        
        if (this.currentBallotData && this.currentBallotData.decree) {
            el.textContent = `"${this.currentBallotData.decree}"`;
        } else if (this.learnedValue) {
            el.textContent = `"${this.learnedValue}" (consensus reached)`;
        } else {
            el.textContent = '--';
        }
    }
    
    /**
     * Update ballot counter display
     */
    _updateBallotCount() {
        if (!this.ballotsTried) return;
        const el = document.getElementById(this.ballotsTried);
        if (!el) return;
        
        el.textContent = this.ballotCounter.toString();
    }
    
    /**
     * Update decree value display
     */
    _updateDecreeValue() {
        if (!this.decreeValueId) return;
        const el = document.getElementById(this.decreeValueId);
        if (!el) return;
        
        if (this.learnedValue) {
            el.textContent = this.learnedValue;
        } else if (this.currentBallotData && this.currentBallotData.decree) {
            el.textContent = this.currentBallotData.decree;
        } else {
            el.textContent = '--';
        }
    }
    
    /**
     * Update decree status display
     */
    _updateDecreeStatus() {
        if (!this.decreeStatusId) return;
        const el = document.getElementById(this.decreeStatusId);
        if (!el) return;
        
        // Once chosen, lock the status
        if (this.learnedValue) {
            if (!this.statusLocked) {
                el.textContent = '✓ Chosen by quorum';
                el.className = 'decree-status success';
                this.statusLocked = true;
            }
            return;
        }
        
        // Don't update if status is locked
        if (this.statusLocked) return;
        
        if (!this.currentBallotData) {
            el.textContent = 'Waiting...';
            el.className = 'decree-status';
            return;
        }
        
        const voteCount = this.currentBallotData.votes.length;
        const isSuccess = voteCount >= this.quorumSize;
        
        if (isSuccess) {
            el.textContent = '✓ Chosen by quorum';
            el.className = 'decree-status success';
        } else {
            el.textContent = `⏳ ${voteCount}/${this.quorumSize} votes for quorum`;
            el.className = 'decree-status in-progress';
        }
    }
    
    /**
     * Update learned values list
     */
    _updateLearnedList() {
        if (!this.learnedListId) return;
        const el = document.getElementById(this.learnedListId);
        if (!el) return;
        
        if (!this.learnedValue) {
            el.innerHTML = '<div class="empty-state-message">Waiting for consensus...</div>';
            return;
        }
        
        let html = '';
        for (let i = 0; i < this.totalPriests; i++) {
            html += `<div class="learned-item">${this.priestNames[i]}: "${this.learnedValue}"</div>`;
        }
        el.innerHTML = html;
    }
    
    /**
     * Render ballot history
     */
    renderHistory() {
        if (!this.historyContainerId) return;
        const container = document.getElementById(this.historyContainerId);
        if (!container) return;
        
        if (this.ballotHistory.length === 0) {
            container.innerHTML = '<div class="empty-state">No ballots yet</div>';
            this.lastRenderedBallotCount = 0;
            return;
        }
        
        // Add new ballots that haven't been rendered yet
        for (let i = this.lastRenderedBallotCount; i < this.ballotHistory.length; i++) {
            this._addBallotToHistory(this.ballotHistory[i], container);
        }
        this.lastRenderedBallotCount = this.ballotHistory.length;
    }
    
    /**
     * Add a single ballot to history display
     */
    _addBallotToHistory(ballot, container) {
        const isSuccess = ballot.votes.length >= this.quorumSize;
        const className = ballot.learned ? 'learned' : isSuccess ? 'success' : 'failed';
        const status = ballot.learned ? '✓ LEARNED' : isSuccess ? '✓ Success' : '✗ Failed';
        
        let html = `
            <div class="history-ballot-container">
            <div class="history-item ${className}">
                <div class="history-ballot-header">Ballot ${ballot.number}: ${ballot.votes.length}/${this.quorumSize}</div>
                <div class="history-ballot-decree">Proposed: "${ballot.decree}"</div>
                <div class="vote-grid">
        `;
        
        // Show all priests with vote status
        for (let j = 0; j < this.totalPriests; j++) {
            const voted = ballot.votes.includes(j);
            const bgColor = voted ? '#34d399' : '#475569';
            const textColor = voted ? '#0a0e27' : '#cbd5e1';
            html += `<div class="vote-cell" style="background: ${bgColor}; color: ${textColor};">${j}</div>`;
        }
        
        html += `
                </div>
                <div class="history-ballot-status">${status}</div>
            </div>
        `;
        
        // If this ballot achieved quorum
        if (isSuccess) {
            if (ballot.learned) {
                html += `
                    <div class="success-box">
                        <div class="success-title">✓ Consensus Achieved</div>
                        <div class="success-value">Learned Value: "${ballot.decree}"</div>
                    </div>
                `;
            } else if (this.learnedValue) {
                html += `
                    <div class="warning-box">
                        <div class="warning-title">Note</div>
                        <div class="warning-text">Consensus already reached</div>
                        <div class="warning-value">Actual Learned Value: "${this.learnedValue}"</div>
                    </div>
                `;
            }
        }
        
        html += '</div>'; // Close history-ballot-container
        
        // Prepend (newest first)
        container.insertAdjacentHTML('afterbegin', html);
    }
    
    /**
     * Reset the simulator
     */
    reset() {
        if (this.autoRetryTimeout) clearTimeout(this.autoRetryTimeout);
        
        this.ballotHistory = [];
        this.currentBallotData = null;
        this.learnedValue = null;
        this.ballotCounter = 0;
        this.lastRenderedBallotCount = 0;
        this.statusLocked = false;
        
        this._updateDisplay();
        if (this.historyContainerId) {
            const container = document.getElementById(this.historyContainerId);
            if (container) container.innerHTML = '<div class="empty-state">No ballots yet</div>';
        }
    }
    
    /**
     * Update configuration
     */
    updateConfig(config) {
        if (config.totalPriests !== undefined) this.totalPriests = config.totalPriests;
        if (config.quorumSize !== undefined) this.quorumSize = config.quorumSize;
        if (config.priestNames !== undefined) this.priestNames = config.priestNames;
        if (config.priestParticipation !== undefined) this.priestParticipation = config.priestParticipation;
        if (config.wordChain !== undefined) this.wordChain = config.wordChain;
    }
}

// Export for ES modules or Node.js
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DecreeSimulator;
}
