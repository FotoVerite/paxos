use std::{collections::HashMap, sync::Arc};

use tokio::sync::{Mutex, RwLock, mpsc::Receiver};
use uuid::Uuid;

use crate::cluster::runtime::{
    member::RuntimeMember,
    node::{RuntimeNode, RuntimeNodeFactory},
};

pub type SharedRuntimeNode<TMsg> = Arc<dyn RuntimeNode<TMsg>>;
pub type SharedRuntimeFactory<TMsg> = Arc<dyn RuntimeNodeFactory<TMsg>>;
pub type SharedRuntimeMember<TMsg> = Arc<RuntimeMember<TMsg>>;

pub type RuntimeMembers<TMsg> = Arc<RwLock<HashMap<Uuid, SharedRuntimeMember<TMsg>>>>;
pub type RuntimeMemberIds = Arc<RwLock<Vec<Uuid>>>;
pub type ProvisionedInbox<TMsg> = Arc<Mutex<Option<Receiver<TMsg>>>>;
pub type ProvisionedInboxes<TMsg> = RwLock<HashMap<Uuid, ProvisionedInbox<TMsg>>>;
