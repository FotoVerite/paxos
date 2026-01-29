pub struct Clerk {
    cid: Int,
    sid: Int,
    leader_id: Int,
    replicas: &[Int],
    tx: mpsc::Sender<ClerkRequest>,
    rx: mpsc:: Receiver<ClerkResponse>
}

pub fn Request(&self, cmd: PaxosCommand) {}

pub fn HandleResponse(&self) -> Int {}

fn increment_sid(&self) {
    self.sid += 1;
}
