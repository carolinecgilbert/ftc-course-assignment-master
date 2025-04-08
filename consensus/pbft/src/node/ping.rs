use types::{Msg, ProtMsg};

use super::Context;

impl Context {
    // A function's input parameter needs to be borrowed as mutable only when
    // we intend to modify the variable in the function. Otherwise, it need not be borrowed as mutable.
    // In this example, the mut can (and must) be removed because we are not modifying the Context inside
    // the function. 
    pub async fn start_ping(self: &mut Context){
        // Draft a message
        let msg = Msg{
            content: "Hi".as_bytes().to_vec(),
            origin: self.myid
        };
        // Wrap the message in a type
        // Use different types of messages like INIT, ECHO, .... for the Bracha's RBC implementation
        let protocol_msg = ProtMsg::Ping(msg, self.myid);
        // Broadcast the message to everyone
        self.broadcast(protocol_msg).await;
    }

    pub async fn handle_ping(self: &Context, msg:Msg){
        log::info!("Received ping message {:?} from node {}",msg.content,msg.origin);
    }
}