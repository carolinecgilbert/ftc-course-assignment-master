use types::{Msg, ProtMsg, Replica};
use std::collections::HashSet;

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
        let leader_id = 0;
        log::info!("Sending PBFT VALUE message {:?} to leader node {}", msg.content,leader_id);
        let protocol_msg = ProtMsg::Value(msg, self.myid);
        let wrapper_msg = types::WrapperMsg::new(
            protocol_msg, 
            self.myid,
            self.sec_key_map.get(&leader_id).unwrap(),
        );
        self.send(leader_id, wrapper_msg).await;
        
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
        let num_vals = self.value_map.len();
        log::info!("Received {} disctint values", num_vals);
        if !self.started_rbc && num_vals == self.num_nodes - self.num_faults {
            log::info!("Received sufficient values, starting RBC with median...");
            self.started_rbc = true;

            // Convert each Vec<u8> to an integer and store in values
            let mut converted_vals: Vec<u32> = Vec::default();
            for (replica, value) in &self.value_map {
                match std::str::from_utf8(value) {
                    Ok(s) => match s.parse::<u32>() {
                        Ok(num) => {
                            converted_vals.push(num);
                            log::info!("Node {} submitted integer value: {}", replica, num);
                        }
                        Err(e) => {
                            log::error!("Failed to parse value from node {} as integer: {}", replica, e);
                        }
                    },
                    Err(e) => {
                        log::error!("Invalid UTF-8 from node {}: {}", replica, e);
                    }
                }
            }

            // Sort values and compute median
            converted_vals.sort();
            let mid = converted_vals.len() / 2;
            let median = if converted_vals.len() % 2 == 0 {
                (converted_vals[mid-1] + converted_vals[mid]) / 2
            }
            else {
                converted_vals[mid]
            };
            log::info!("converted_vals: {:?}, median: {}", converted_vals, median);
            
            let msg = Msg{
                content: median.to_string().into_bytes(),
                origin: self.myid
            };
    
            // Automatically record own echo and vote
            self.echo_map.entry(msg.content.clone()).or_default().insert(self.myid);
            self.vote_map.entry(msg.content.clone()).or_default().insert(self.myid);
    
            // Wrap the message in a type
            // Use different types of messages like INIT, ECHO, .... for the Bracha's RBC implementation
            let protocol_msg = ProtMsg::InitRBC(msg, self.myid);
            let self_protocol_msg = protocol_msg.clone();
            // Broadcast the message to everyone
            self.broadcast(protocol_msg).await;

            // Send init message to self as well
            let wrapper_msg = types::WrapperMsg::new(
                self_protocol_msg, 
                self.myid,
                self.sec_key_map.get(&self.myid).unwrap(),
            );
            self.send(self.myid, wrapper_msg).await;

        }
        
    }

    // RBC CODE FROM rbc.ps 
    pub async fn handle_init_rbc(self: &mut Context, msg:Msg){
        log::info!("Received init message {:?} from node {}",msg.content,msg.origin);
        // Send echo to all parties and set echo bool to false
        if self.echo == true{
            // Automatically record own echo
            self.echo = false;
            self.echo_map.entry(msg.content.clone()).or_default().insert(self.myid);
            let echo_msg = ProtMsg::Echo(msg.clone(), self.myid);
            log::info!("Broadcasting echo!");
            self.broadcast(echo_msg).await;
        }
    }

    pub async fn handle_echo(self: &mut Context, msg:Msg, sender:Replica){
        log::info!("Received echo {:?} from node {}",msg.content,sender);
        // Initialize hashset for this value if not already tracked
        if !self.echo_map.contains_key(&msg.content) {
            let new_value_echos = HashSet::default();
            self.echo_map.insert(msg.content.clone(), new_value_echos);
        }

        // Insert msg origin into hash set for this value
        let recvd_echos = self.echo_map.get_mut(&msg.content).unwrap();
        recvd_echos.insert(sender);
        let num_recvd_echos = recvd_echos.len();
        log::info!("Received {} distinct echos.", num_recvd_echos);


        // Send vote to all parties on receiving n-f distinct echos
        if !self.voted && num_recvd_echos == (self.num_nodes-self.num_faults){
            log::info!("Received sufficient echos, broadcasting vote!");
            self.voted = true;
            self.vote_map.entry(msg.content.clone()).or_default().insert(self.myid);
            let vote_msg = ProtMsg::Vote(msg, self.myid);
            self.broadcast(vote_msg).await;
        }

    }

    pub async fn handle_vote(self: &mut Context, msg:Msg, sender:Replica){
        log::info!("Received vote {:?} from node {}",msg.content,sender);
        // Initialize hashset for this value if not already tracked
        if !self.vote_map.contains_key(&msg.content){
            let new_value_votes = HashSet::default();
            self.vote_map.insert(msg.content.clone(), new_value_votes);
        }
        let recvd_msg = msg.content.clone();

        // Insert msg origin into hash set for this value
        let recvd_votes = self.vote_map.get_mut(&msg.content).unwrap();
        recvd_votes.insert(sender);
        let num_recvd_votes = recvd_votes.len();
        log::info!("Received {} distinct votes.", num_recvd_votes);

        // Send vote to all parties on receiving votes from f+1 distinct parties
        if !self.voted && num_recvd_votes == (self.num_faults+1){
            log::info!("Received f+1 votes, broadcasting vote!");
            self.voted = true;
            self.vote_map.entry(msg.content.clone()).or_default().insert(self.myid);
            let vote_msg = ProtMsg::Vote(msg, self.myid);
            self.broadcast(vote_msg).await;
        }

        // Deliver value on receiving votes from n-f distict parties
        if !self.terminated && num_recvd_votes == (self.num_nodes-self.num_faults){
            log::info!("Received sufficient votes, delivering vote!");
            self.terminated = true;
            let v = String::from_utf8(recvd_msg).unwrap();
            self.terminate(v).await;
        }
    }
}