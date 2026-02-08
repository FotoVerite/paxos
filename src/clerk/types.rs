use std::any::Any;

use crate::paxos_command::PaxosCommand;

pub struct ClerkRequest {
    cid: usize, 
    sid: usize, 
    cmd: PaxosCommand
}

pub struct ClerkResponse {
//    error: Option<ClerkResponseError>,
   value: Option<Box<dyn Any>>,
}

