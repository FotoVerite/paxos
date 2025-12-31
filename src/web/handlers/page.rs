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

// Section 1
page_template!(
    tiop_handler,
    TiopHandler,
    "the_problem/the_island_of_paxos.html"
);
page_template!(
    requirements_handler,
    RequirementsTemplate,
    "the_problem/requirements.html"
);

page_template!(
    assumptions_handler,
    AssumptionsTemplate,
    "the_problem/assumptions.html"
);

page_template!(
    synod_ballot_definition_handler,
    SynodBallotDefinitionTemplate,
    "synod/ballot_definition.html"
);

page_template!(
    synod_ballot_handler,
    SynodBallotTemplate,
    "synod/ballot.html"
);

// Section 2
page_template!(
    synod_overview_handler,
    SynodOverviewTemplate,
    "synod/overview.html"
);

page_template!(
    synod_decree_handler,
    SynodDecreeTemplate,
    "synod/decree_example.html"
);

page_template!(
    synod_math_terms_handler,
    SynodTerms,
    "./synod/math_terms.html"
);

page_template!(
    synod_votes_function_handler,
    SynodVotesFunctionTemplate,
    "./synod/votes_function.html"
);

page_template!(
    synod_max_vote_quorum_handler,
    SynodMax,
    "./synod/max_vote_quorum.html"
);

page_template!(
    synod_theorem_one_handler,
    SynodTheoremOneTemplate,
    "./synod/theorem_one.html"
);

page_template!(
    synod_theorem_two_handler,
    SynodTheoremTwoTemplate,
    "./synod/theorem_two.html"
);

page_template!(synod_recap_handler, SynodR, "./synod/recap.html");

//section 2.1-2.4
page_template!(
    protocol_preliminary_handler,
    ProtocolPreliminaryTemp,
    "./protocols/preliminary.html"
);

page_template!(
    preliminary_protocol_demo_handler,
    PreliminaryProtocolDemoTemplate,
    "./protocols/preliminary-protocol-demo.html"
);

page_template!(
    protocol_basic_handler,
    ProtocolBasicTemp,
    "./protocols/basic.html"
);

page_template!(
    basic_protocol_demo_handler,
    BasicProtocolDemoTemplate,
    "./protocols/basic-protocol-demo.html"
);

// Define pages
page_template!(landing_handler, LandingTemplate, "landing.html");
page_template!(leslie_handler, LeslieTemplate, "leslie.html");
page_template!(visualizer_handler, VisualizerTemplate, "visualizer.html");

page_template!(overview_handler, OverviewTemplate, "overview.html");
page_template!(senate_handler, SenateTemplate, "senate.html");
page_template!(
    setting_of_paper_handler,
    SettingOfPaper,
    "setting_of_paper.html"
);
page_template!(terms_handler, TermTemplate, "terms.html");
page_template!(the_problem_handler, TheProblem, "the_problem.html");
page_template!(decree_handler, DecreeTemplate, "decree.html");

page_template!(
    synod_constraints_handler,
    SynodConstraints,
    "./synod/constraints.html"
);

page_template!(synod_lemma_handler, SynodLemma, "./synod/lemma.html");
page_template!(
    synod_ballot_number_handler,
    SynodBallotNumber,
    "./synod/ballot_number.html"
);

page_template!(synod_max_vote_handler, Synod, "./synod/max_vote.html");
page_template!(synod_quorum_handler, SynodQuorum, "./synod/quorum.html");
