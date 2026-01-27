use askama::Template;
use axum::response::{Html, IntoResponse};

// Index/Home
#[derive(Template)]
#[template(path = "papers/paxos_made_simple/index.html")]
pub struct IndexTemplate;

pub async fn index_handler() -> impl IntoResponse {
    match IndexTemplate.render() {
        Ok(html) => Html(html).into_response(),
        Err(e) => {
            eprintln!("Template render error: {}", e);
            Html("Template error").into_response()
        }
    }
}

// Abstract
#[derive(Template)]
#[template(path = "papers/paxos_made_simple/abstract.html")]
pub struct AbstractTemplate;

pub async fn abstract_handler() -> impl IntoResponse {
    Html(AbstractTemplate.render().unwrap_or_default())
}

// Introduction
#[derive(Template)]
#[template(path = "papers/paxos_made_simple/introduction.html")]
pub struct IntroductionTemplate;

pub async fn introduction_handler() -> impl IntoResponse {
    Html(IntroductionTemplate.render().unwrap_or_default())
}

// Consensus Algorithm
#[derive(Template)]
#[template(path = "papers/paxos_made_simple/consensus.html")]
pub struct ConsensusTemplate;

pub async fn consensus_handler() -> impl IntoResponse {
    Html(ConsensusTemplate.render().unwrap_or_default())
}

// The Problem
#[derive(Template)]
#[template(path = "papers/paxos_made_simple/the_problem.html")]
pub struct TheProblemTemplate;

pub async fn the_problem_handler() -> impl IntoResponse {
    Html(TheProblemTemplate.render().unwrap_or_default())
}

// Choosing a Value
#[derive(Template)]
#[template(path = "papers/paxos_made_simple/choosing_value.html")]
pub struct ChoosingValueTemplate;

pub async fn choosing_value_handler() -> impl IntoResponse {
    Html(ChoosingValueTemplate.render().unwrap_or_default())
}

// Learning a Chosen Value
#[derive(Template)]
#[template(path = "papers/paxos_made_simple/learning_value.html")]
pub struct LearningValueTemplate;

pub async fn learning_value_handler() -> impl IntoResponse {
    Html(LearningValueTemplate.render().unwrap_or_default())
}

// Progress
#[derive(Template)]
#[template(path = "papers/paxos_made_simple/progress.html")]
pub struct ProgressTemplate;

pub async fn progress_handler() -> impl IntoResponse {
    Html(ProgressTemplate.render().unwrap_or_default())
}

// Implementation
#[derive(Template)]
#[template(path = "papers/paxos_made_simple/implementation.html")]
pub struct ImplementationTemplate;

pub async fn implementation_handler() -> impl IntoResponse {
    Html(ImplementationTemplate.render().unwrap_or_default())
}

// State Machine
#[derive(Template)]
#[template(path = "papers/paxos_made_simple/state_machine.html")]
pub struct StateMachineTemplate;

pub async fn state_machine_handler() -> impl IntoResponse {
    Html(StateMachineTemplate.render().unwrap_or_default())
}
