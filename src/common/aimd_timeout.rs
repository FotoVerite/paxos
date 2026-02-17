use std::time::Duration;

/// Validated, explicit config (no Option fields).
#[derive(Debug, Clone, Copy)]
pub struct AimdTimeoutConfig {
    pub initial: Duration,
    pub min: Duration,
    pub max: Duration,
    pub decrease: Duration,
    pub increase_factor: u32,
}

impl Default for AimdTimeoutConfig {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(250),
            min: Duration::from_millis(50),
            max: Duration::from_secs(5),
            decrease: Duration::from_millis(5),
            increase_factor: 2,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AimdTimeout {
    current: Duration,
    initial: Duration,
    min: Duration,
    max: Duration,
    decrease: Duration,
    increase_factor: u32,
}

impl Default for AimdTimeout {
    fn default() -> Self {
        Self::new(AimdTimeoutConfig::default()).expect("default AIMD config must be valid")
    }
}

impl AimdTimeout {
    pub fn new(cfg: AimdTimeoutConfig) -> Result<Self, AimdTimeoutError> {
        if cfg.min.is_zero() {
            return Err(AimdTimeoutError::MinMustBePositive);
        }
        if cfg.decrease.is_zero() {
            return Err(AimdTimeoutError::DecreaseMustBePositive);
        }
        if cfg.increase_factor < 2 {
            return Err(AimdTimeoutError::IncreaseFactorTooSmall);
        }
        if cfg.min > cfg.max {
            return Err(AimdTimeoutError::MinExceedsMax);
        }
        if cfg.initial < cfg.min || cfg.initial > cfg.max {
            return Err(AimdTimeoutError::InitialOutOfRange);
        }

        Ok(Self {
            current: cfg.initial,
            initial: cfg.initial,
            min: cfg.min,
            max: cfg.max,
            decrease: cfg.decrease,
            increase_factor: cfg.increase_factor,
        })
    }

    // Keep your old call-site shape: read is infallible.
    pub fn interval(&self) -> Duration {
        self.current
    }

    // Multiplicative increase on contention/preemption.
    pub fn backoff(&mut self) {
        self.current = self.current.saturating_mul(self.increase_factor);
        self.clamp();
    }

    // Additive decrease on progress.
    pub fn success(&mut self) {
        self.current = self.current.saturating_sub(self.decrease);
        self.clamp();
    }

    pub fn reset(&mut self) {
        self.current = self.initial;
        self.clamp();
    }

    pub fn min(&self) -> Duration {
        self.min
    }

    pub fn max(&self) -> Duration {
        self.max
    }

    fn clamp(&mut self) {
        if self.current < self.min {
            self.current = self.min;
        } else if self.current > self.max {
            self.current = self.max;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AimdTimeoutError {
    MinMustBePositive,
    DecreaseMustBePositive,
    IncreaseFactorTooSmall,
    MinExceedsMax,
    InitialOutOfRange,
}
