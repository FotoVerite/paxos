use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingDecision {
    Broadcast,
    SendTo(Uuid),
    SendToMany(Vec<Uuid>),
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalRoutingDecision<TTarget> {
    DeliverTo(TTarget),
    Drop,
}

pub trait ProtocolRouter<TMsg>: Send + Sync {
    type Context;

    fn route(&self, msg: &TMsg, context: &Self::Context) -> RoutingDecision;
}

pub trait ProtocolInboxRouter<TMsg>: Send + Sync {
    type Context;
    type Target;

    fn classify(&self, msg: &TMsg, context: &Self::Context) -> LocalRoutingDecision<Self::Target>;
}
