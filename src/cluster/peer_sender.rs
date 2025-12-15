use tokio::sync::mpsc;

use crate::message::Message;

pub struct PeerSender {
    me: usize,
    peers: Vec<mpsc::Sender<Message>>,
}

impl PeerSender {
    pub fn new(me: usize, peers: Vec<mpsc::Sender<Message>>) -> Self {
        return Self { me, peers: peers };
    }
    pub async fn send(&self, to: usize, msg: Message) {
        if to == self.me {
            return;
        }
        let _ = self.peers[to].send(msg).await;
    }

    pub async fn broadcast(&self, msg: Message)
    where
        Message: Clone,
    {
        for (idx, peer) in self.peers.iter().enumerate() {
            if idx == self.me {
                continue;
            }
            let _ = peer.send(msg.clone()).await;
        }
    }
}
