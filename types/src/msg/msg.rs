use crypto::hash::{Hash};
use crypto::hash::{do_mac};
use serde::{Serialize, Deserialize};
use crate::{WireReady, Replica};

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct Msg {
    pub content: Vec<u8>,
    pub origin:Replica,
    // Add your custom fields here
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub enum ProtMsg{
    // Create your custom types of messages
    // Example type is a ping message, which takes a Message and the sender replica
    Ping(Msg,Replica),
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct WrapperMsg{
    pub protmsg: ProtMsg,
    pub sender:Replica,
    pub mac:Hash,
}

impl WrapperMsg{
    pub fn new(msg:ProtMsg,sender:Replica, sk: &[u8]) -> Self{
        let new_msg = msg.clone();
        let bytes = bincode::serialize(&new_msg).expect("Failed to serialize protocol message");
        let mac = do_mac(&bytes.as_slice(), sk);
        Self{
            protmsg: new_msg,
            mac: mac,
            sender:sender
        }
    }
}

impl WireReady for WrapperMsg{
    fn from_bytes(bytes: &[u8]) -> Self {
        let c:Self = bincode::deserialize(bytes)
            .expect("failed to decode the protocol message");
        c.init()
    }

    fn to_bytes(&self) -> Vec<u8> {
        let bytes = bincode::serialize(self).expect("Failed to serialize client message");
        bytes
    }

    fn init(self) -> Self {
        match self {
            _x=>_x
        }
    }
}