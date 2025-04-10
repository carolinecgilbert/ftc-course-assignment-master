use types::{Msg, ProtMsg, Replica};
use std::collections::HashSet;

use super::Context;

impl Context {
    // A function's input parameter needs to be borrowed as mutable only when
    // we intend to modify the variable in the function. Otherwise, it need not be borrowed as mutable.
    // In this example, the mut can (and must) be removed because we are not modifying the Context inside
    // the function. 
    pub async fn start_rbc(self: &mut Context){
        // Draft a message
        let msg = Msg{
            content: self.inp_message.clone(),
            origin: self.myid
        };

        // Automatically record own echo and vote
        self.echo_map.entry(msg.content.clone()).or_default().insert(self.myid);
        self.vote_map.entry(msg.content.clone()).or_default().insert(self.myid);

        // Wrap the message in a type
        // Use different types of messages like INIT, ECHO, .... for the Bracha's RBC implementation
        let protocol_msg = ProtMsg::InitRBC(msg, self.myid);
        // Broadcast the message to everyone
        self.broadcast(protocol_msg).await;
    }

    pub async fn handle_init(self: &mut Context, msg:Msg){
        log::info!("Received init message {:?} from node {}",msg.content,msg.origin);
        // Send echo to all parties and set echo bool to false
        if self.echo == true{
            self.echo = false;
            let echo_msg = ProtMsg::Echo(msg.clone(), self.myid);
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