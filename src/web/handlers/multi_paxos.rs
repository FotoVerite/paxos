use askama::Template;
use axum::response::{Html, IntoResponse};

/// Macro to define a template struct and its handler in one invocation
/// Generates: template struct with #[derive(Template)], #[template(path = ...)]
/// and async handler that renders the template
macro_rules! page_template {
    ($handler_name:ident, $template_name:ident, $template_path:expr) => {
        #[derive(Template)]
        #[template(path = $template_path)]
        struct $template_name;

        pub async fn $handler_name() -> impl IntoResponse {
            Html($template_name.render().unwrap_or_default())
        }
    };
}

page_template!(
    m_p_overview,
    MPOverview,
    "multi_paxos/overview.html"
);

page_template!(
    m_p_protocol,
    MPProtocol,
    "multi_paxos/protocol.html"
);

page_template!(
    m_p_president,
    MPPresident,
    "./multi_paxos/presidential_problems.html"
);

page_template!(
    m_p_optimizations,
    MPOptimizations,
    "./multi_paxos/optimizations.html"
);
page_template!(
    f_d_picking_a_president,
    FDPickingAPresidentTemplate,
    "./further_developments/picking_a_president.html"
);

page_template!(
    f_d_bureaucrats,
    FDBureaucrats,
    "./further_developments/bureaucrats.html"
);
page_template!(
    f_d_learning_the_law,
    FDLearningTemplate,
    "./further_developments/learning_the_law.html"
);
page_template!(
    f_d_long_ledgers,
    FDLongLedgers,
    "./further_developments/long_ledgers.html"
);

page_template!(
    f_d_dishonest_legislators,
    FDDishonestLegislators,
    "./further_developments/dishonest_legislators.html"
);