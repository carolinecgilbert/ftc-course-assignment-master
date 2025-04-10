use std::{sync::Arc};

use crypto::hash::{verf_mac};
use types::{{WrapperMsg, ProtMsg}};
use crate::node::{
    context::Context
};
impl Context{
    // This function verifies the Message Authentication Code (MAC) of a sent message
    // A node cannot impersonate as another node because of MACs
    pub fn check_proposal(&self,wrapper_msg: Arc<WrapperMsg>) -> bool {
        // validate MAC
        let byte_val = bincode::serialize(&wrapper_msg.protmsg).expect("Failed to serialize object");
        let sec_key = match self.sec_key_map.get(&wrapper_msg.clone().sender) {
            Some(val) => {val},
            None => {panic!("Secret key not available, this shouldn't happen")},
        };
        if !verf_mac(&byte_val,&sec_key.as_slice(),&wrapper_msg.mac){
            log::warn!("MAC Verification failed.");
            return false;
        }
        true
    }
    
    pub(crate) async fn process_msg(&mut self, wrapper_msg: WrapperMsg){
        log::debug!("Received protocol msg: {:?}",wrapper_msg);
        let msg = Arc::new(wrapper_msg.clone());
        if self.check_proposal(msg){
            match wrapper_msg.clone().protmsg {
                // Handle each message type appropriately and write functions to evaluate each type of message
                ProtMsg::Ping(main_msg,rep)=> {
                    // RBC initialized
                    log::info!("Received Ping from node : {:?}",rep);
                    self.handle_ping(main_msg).await;
                },
                ProtMsg::InitPBFT(main_msg,rep)=> {
                    // PBFT init handler
                    log::info!("Received InitPBFT from node : {:?}",rep);
                    self.handle_init_pbft(main_msg, rep).await;
                },
                ProtMsg::Value(main_msg,rep)=> {
                    // PBFT input value handler
                    log::info!("Received Value from node : {:?}",rep);
                    self.handle_value(main_msg, rep).await;
                },
                // RBC messages ignore
                ProtMsg::InitRBC(_main_msg,rep)=> {
                    // RBC initialized
                    log::error!("Received InitRBC during PBFT from node : {:?}",rep);
                },
                ProtMsg::Echo(_main_msg,rep)=> {
                    // RBC echo handler
                    log::error!("Received RBC Echo during PBFT from node : {:?}",rep);
                },
                ProtMsg::Vote(_main_msg,rep)=> {
                    // RBC vote handler
                    log::error!("Received RBC Vote during PBFT from node : {:?}",rep);
                },
            }
        }
        else {
            log::warn!("MAC Verification failed for message {:?}",wrapper_msg.protmsg);
        }
    }
}