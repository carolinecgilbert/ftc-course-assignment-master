use types::{Msg, ProtMsg, Replica};

use super::Context;

impl Context {
    // A function's input parameter needs to be borrowed as mutable only when
    // we intend to modify the variable in the function. Otherwise, it need not be borrowed as mutable.
    // In this example, the mut can (and must) be removed because we are not modifying the Context inside
    // the function. 
    pub async fn start_pbft(self: &mut Context){
        log::info!("Node {} starting PBFT...", self.myid);
        // Draft a message
        let msg = Msg{
            content: self.inp_message.clone(),
            origin: self.myid
        };

        // Automatically record own input value
        self.value_map.insert(self.myid, self.inp_message.clone());
        
        // Send init msg to other nodes to indicate ready to receive vi
        let protocol_msg = ProtMsg::InitPBFT(msg, self.myid);
        // Broadcast the message to everyone
        self.broadcast(protocol_msg).await;

    }

    pub async fn handle_init_pbft(self: &mut Context, msg:Msg, sender:Replica){
        // Only trust init messages from leader node 0
        if sender != 0 {
            log::error!("Received PBFT INIT message {:?} from non-leader node {}", msg.content, sender);
            return;
        }
        log::info!("Received PBFT INIT message {:?} from leader node {}",msg.content,sender);
        
        // Send my input to leader node 0
        let msg = Msg{
            content: self.inp_message.clone(),
            origin: self.myid
        };
        let protocol_msg = ProtMsg::Value(msg, self.myid);
        let leader_id = 0;
        let wrapper_msg = types::WrapperMsg::new(
            protocol_msg, 
            self.myid,
            self.sec_key_map.get(&leader_id).unwrap(),
        );
        self.send(0, wrapper_msg).await;
        
    }

    pub async fn handle_value(self: &mut Context, msg:Msg, sender:Replica){
        // Only leader node 0 should receive value messages
        if self.myid != 0 {
            log::error!("Non-leader received PBFT VALUE message {:?} from node {}", msg.content, sender);
            return;
        }
        log::info!("Received PBFT VALUE message {:?} from node {}",msg.content,sender);
        
        // Honest nodes should only send value message once
        if self.value_map.contains_key(&sender) {
            log::info!("Received multiple PBFT VALUE messages from node {}", sender);
            return;
        }

        // Record received value and broadcast median if n-f values received
        self.value_map.insert(sender, msg.content.clone());
        log::info!("Received {} disctint values", self.value_map.len());
        if !self.started_rbc && self.value_map.len() == self.num_nodes - self.num_faults {
            log::info!("Received sufficient values, starting RBC with median...");
            self.started_rbc = true;
        }
        
    }
}