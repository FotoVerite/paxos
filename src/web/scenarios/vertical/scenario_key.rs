#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerticalScenarioKey {
    ActivationOnly,
    ReconfigurationOnly,
    RedirectAfterHandoff,
    Vp1RecoveryCost,
    Vp2BoundedRecovery,
}

impl VerticalScenarioKey {
    pub fn parse(value: &str) -> Self {
        match value {
            "activation_only" => Self::ActivationOnly,
            "redirect_after_handoff" => Self::RedirectAfterHandoff,
            "vp1_recovery_cost" => Self::Vp1RecoveryCost,
            "vp2_bounded_recovery" => Self::Vp2BoundedRecovery,
            "" | "reconfiguration_only" => Self::ReconfigurationOnly,
            _ => Self::ReconfigurationOnly,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ActivationOnly => "activation_only",
            Self::ReconfigurationOnly => "reconfiguration_only",
            Self::RedirectAfterHandoff => "redirect_after_handoff",
            Self::Vp1RecoveryCost => "vp1_recovery_cost",
            Self::Vp2BoundedRecovery => "vp2_bounded_recovery",
        }
    }
}
