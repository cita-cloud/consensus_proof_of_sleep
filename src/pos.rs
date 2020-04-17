// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use blake2b_simd::blake2b;
use cita_ng_proto::common::{Empty, Hash};
use cita_ng_proto::consensus::ConsensusConfiguration;
use cita_ng_proto::controller::consensus2_controller_service_client::Consensus2ControllerServiceClient;
use cita_ng_proto::network::{network_service_client::NetworkServiceClient, NetworkMsg};
use rand::{thread_rng, Rng};
use std::time::Duration;
use tokio::time;
use tonic::Request;

pub struct POS {
    config: Option<ConsensusConfiguration>,
    controller_port: String,
    network_port: String,
}

impl POS {
    pub fn new(controller_port: String, network_port: String) -> Self {
        POS {
            config: None,
            controller_port,
            network_port,
        }
    }

    pub fn get_block_delay_number(&self) -> u32 {
        6
    }

    pub fn reconfigure(&mut self, config: ConsensusConfiguration) {
        self.config = Some(config);
    }

    pub async fn process_network_msg(&self, msg: NetworkMsg) {
        match msg.r#type.as_str() {
            "proposal" => {
                if let Some(node_num) = self.config.as_ref().map(|c| c.validators.len()) {
                    let target = node_num * 3; // 3 is block interval
                    let (nonce_slice, proposal) = msg.msg.split_at(8);
                    let mut nonce_bytes = [0 as u8; 8];
                    nonce_bytes.copy_from_slice(nonce_slice);
                    let nonce = u64::from_be_bytes(nonce_bytes);
                    if self.check_nonce(target, proposal, nonce) {
                        let check_ret = {
                            let ret =
                                check_proposal(self.controller_port.clone(), proposal.to_vec())
                                    .await;
                            if let Ok(result) = ret {
                                result
                            } else {
                                false
                            }
                        };

                        if check_ret {
                            let _ =
                                commit_block(self.controller_port.clone(), proposal.to_vec()).await;
                        }
                    }
                }
            }
            "vote" => {}
            _ => {}
        }
    }

    pub fn check_nonce(&self, target: usize, proposal: &[u8], nonce: u64) -> bool {
        let nonce_bytes = nonce.to_be_bytes();
        let mut bytes = nonce_bytes[0..].to_vec();
        bytes.extend(proposal);
        let hash = blake2b(&bytes);
        let mut target = target;
        for v in hash.as_bytes() {
            if *v == 0 {
                if target <= 8 {
                    return true;
                }
                target -= 8;
            } else {
                let lz = u8::leading_zeros(*v) as usize;
                return lz > target;
            }
        }
        false
    }

    pub async fn miner(&self) {
        let mut interval = time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;
            if let Some(node_num) = self.config.as_ref().map(|c| c.validators.len()) {
                let proposal = {
                    let ret = get_proposal(self.controller_port.clone()).await;
                    if ret.is_err() {
                        continue;
                    }
                    ret.unwrap()
                };
                let target = node_num * 3; // 3 is block interval
                let nonce: u64 = thread_rng().gen();
                let mined = self.check_nonce(target, &proposal, nonce);
                if mined {
                    let _ = broadcast_proposal(self.network_port.clone(), proposal, nonce).await;
                }
            }
        }
    }
}

async fn check_proposal(
    controller_port: String,
    proposal: Vec<u8>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let controller_address = format!("127.0.0.1:{}", controller_port);
    let mut controller_client =
        Consensus2ControllerServiceClient::connect(controller_address).await?;

    let request = Request::new(Hash { hash: proposal });

    let response = controller_client.check_proposal(request).await?;

    Ok(response.into_inner().is_success)
}

async fn commit_block(
    controller_port: String,
    proposal: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let controller_address = format!("127.0.0.1:{}", controller_port);
    let mut controller_client =
        Consensus2ControllerServiceClient::connect(controller_address).await?;

    let request = Request::new(Hash { hash: proposal });

    let _response = controller_client.commit_block(request).await?;

    Ok(())
}

async fn get_proposal(controller_port: String) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let controller_address = format!("127.0.0.1:{}", controller_port);
    let mut controller_client =
        Consensus2ControllerServiceClient::connect(controller_address).await?;

    let request = Request::new(Empty {});

    let response = controller_client.get_proposal(request).await?;

    Ok(response.into_inner().hash)
}

async fn broadcast_proposal(
    network_port: String,
    proposal: Vec<u8>,
    nonce: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let network_address = format!("127.0.0.1:{}", network_port);
    let mut network_client = NetworkServiceClient::connect(network_address).await?;

    let nonce_bytes = nonce.to_be_bytes();
    let mut msg = nonce_bytes[0..].to_vec();
    msg.extend(proposal);

    let request = Request::new(NetworkMsg {
        module: "consensus".to_owned(),
        r#type: "proposal".to_owned(),
        origin: 0,
        msg,
    });

    let _response = network_client.broadcast(request).await?;

    Ok(())
}
