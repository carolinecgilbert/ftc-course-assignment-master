use std::{collections::{HashSet, HashMap}, net::{SocketAddr,SocketAddrV4}, time::{SystemTime, UNIX_EPOCH, Duration}};

use anyhow::{Result, anyhow};
use fnv::FnvHashMap;
use network::{plaintcp::{TcpReceiver, TcpReliableSender, CancelHandler}, Acknowledgement};
use tokio::sync::{oneshot, mpsc::{unbounded_channel, UnboundedReceiver}};
use types::{Replica, SyncMsg, SyncState};

use crate::SyncHandler;

pub struct Syncer{
    pub num_nodes: usize,
    pub start_time: u128,
    pub sharing_complete_times: HashMap<Replica,u128>,
    pub recon_start_time: u128,
    pub net_map: FnvHashMap<Replica,String>,
    pub alive: HashSet<Replica>,
    pub timings:HashMap<Replica,u128>,
    pub values: HashMap<Replica,String>,
    pub cli_addr: SocketAddr,
    pub rx_net: UnboundedReceiver<SyncMsg>,
    pub net_send: TcpReliableSender<Replica,SyncMsg,Acknowledgement>,
    exit_rx: oneshot::Receiver<()>,
    /// Cancel Handlers
    pub cancel_handlers: Vec<CancelHandler<Acknowledgement>>,
}

impl Syncer{
    pub fn spawn(
        net_map: FnvHashMap<Replica,String>,
        cli_addr:SocketAddr,
    )-> anyhow::Result<oneshot::Sender<()>>{
        let (exit_tx, exit_rx) = oneshot::channel();
        let (tx_net_to_server, rx_net_to_server) = unbounded_channel();
        let cli_addr_sock = cli_addr.port();
        let new_sock_address = SocketAddrV4::new("0.0.0.0".parse().unwrap(), cli_addr_sock);
        TcpReceiver::<Acknowledgement, SyncMsg, _>::spawn(
            std::net::SocketAddr::V4(new_sock_address),
            SyncHandler::new(tx_net_to_server),
        );
        let mut server_addrs :FnvHashMap<Replica,SocketAddr>= FnvHashMap::default();
        for (replica,address) in net_map.iter(){
            let address:SocketAddr = address.parse().expect("Unable to parse address");
            server_addrs.insert(*replica, SocketAddr::from(address.clone()));
        }
        let net_send = TcpReliableSender::<Replica,SyncMsg,Acknowledgement>::with_peers(server_addrs);
        tokio::spawn(async move{
            let mut syncer = Syncer{
                net_map:net_map.clone(),
                start_time:0,
                sharing_complete_times:HashMap::default(),
                recon_start_time:0,
                num_nodes:net_map.len(),
                alive:HashSet::default(),
                values:HashMap::default(),
                timings:HashMap::default(),
                cli_addr:cli_addr,
                rx_net:rx_net_to_server,
                net_send:net_send,
                exit_rx:exit_rx,
                cancel_handlers:Vec::new()
            };
            if let Err(e) = syncer.run().await {
                log::error!("Consensus error: {}", e);
            }
        });
        Ok(exit_tx)
    }
    pub async fn broadcast(&mut self, sync_msg:SyncMsg){
        for replica in 0..self.num_nodes {
            let cancel_handler:CancelHandler<Acknowledgement> = self.net_send.send(replica, sync_msg.clone()).await;
            self.add_cancel_handler(cancel_handler);    
        }
    }
    pub async fn run(&mut self)-> Result<()>{
        loop {
            tokio::select! {
                // Receive exit handlers
                exit_val = &mut self.exit_rx => {
                    exit_val.map_err(anyhow::Error::new)?;
                    log::info!("Termination signal received by the server. Exiting.");
                    break
                },
                msg = self.rx_net.recv() => {
                    // Received a protocol message
                    // Received a protocol message
                    log::debug!("Got a message from the server: {:?}", msg);
                    let msg = msg.ok_or_else(||
                        anyhow!("Networking layer has closed")
                    )?;
                    match msg.state{
                        SyncState::ALIVE=>{
                            log::info!("Got ALIVE message from node {}",msg.sender);
                            self.alive.insert(msg.sender);
                            if self.alive.len() == self.num_nodes{
                                std::thread::sleep(Duration::from_secs(3));
                                self.broadcast(SyncMsg { 
                                    sender: self.num_nodes, 
                                    state: SyncState::START,
                                    value:"".to_string()
                                }).await;
                                self.start_time = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis();
                            }
                        },
                        SyncState::STARTED=>{
                            log::info!("Node {} started the protocol",msg.sender);
                        },
                        SyncState::COMPLETED=>{
                            log::info!("Got COMPLETED message from node {}",msg.sender);
                            self.timings.insert(msg.sender, SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis());
                            self.values.insert(msg.sender,msg.value);
                            if self.timings.len() == (2*self.num_nodes/3)+1{
                                // All nodes terminated protocol
                                let mut vec_times = Vec::new();
                                for (_rep,time) in self.timings.iter(){
                                    vec_times.push(time.clone()-self.start_time);
                                }
                                vec_times.sort();
                                let mut set_of_reported_values:HashSet<String> = HashSet::default();
                                for(_rep,b_value) in self.values.iter(){
                                    set_of_reported_values.insert(b_value.clone());
                                }
                                if set_of_reported_values.len() > 1{
                                    log::info!("Received multiple values from nodes, broadcast failed, rerun test {:?}",set_of_reported_values);
                                }
                                else{
                                    log::info!("All n nodes completed the protocol {:?} with values {:?}",vec_times,set_of_reported_values);
                                }
                                self.broadcast(SyncMsg { sender: self.num_nodes, state: SyncState::STOP, value:"".to_string()}).await;
                            }
                        }
                        _=>{}
                    }
                },
            }
        }
        Ok(())
    }
    pub fn add_cancel_handler(&mut self, canc: CancelHandler<Acknowledgement>){
        self.cancel_handlers
            .push(canc);
    }
}