pub struct ClerkRequest {
    cid: usize, 
    sid: usize, 
    cmd: PaxosCommand
}

pub struct ClerkResponse {
   error: Option<ErrorKind>,
   value: Option<Any>,
   leader_hint: Option<usize>
}