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

use clap::Clap;
use git_version::git_version;
use log::{debug, info, warn};

const GIT_VERSION: &str = git_version!(
    args = ["--tags", "--always", "--dirty=-modified"],
    fallback = "unknown"
);
const GIT_HOMEPAGE: &str = "https://github.com/rink1969/cita_ng_pos";

/// This doc string acts as a help message when the user runs '--help'
/// as do all doc strings on fields
#[derive(Clap)]
#[clap(version = "0.1.0", author = "Rivtower Technologies.")]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    /// print information from git
    #[clap(name = "git")]
    GitInfo,
    /// run this service
    #[clap(name = "run")]
    Run(RunOpts),
}

/// A subcommand for run
#[derive(Clap)]
struct RunOpts {
    /// Sets grpc port of config service.
    #[clap(short = "c", long = "config_port", default_value = "49999")]
    config_port: String,
    /// Sets grpc port of this service.
    #[clap(short = "p", long = "port", default_value = "50003")]
    grpc_port: String,
}

fn main() {
    ::std::env::set_var("RUST_BACKTRACE", "full");

    let opts: Opts = Opts::parse();

    // You can handle information about subcommands by requesting their matches by name
    // (as below), requesting just the name used, or both at the same time
    match opts.subcmd {
        SubCommand::GitInfo => {
            println!("git version: {}", GIT_VERSION);
            println!("homepage: {}", GIT_HOMEPAGE);
        }
        SubCommand::Run(opts) => {
            // init log4rs
            log4rs::init_file("pos-log4rs.yaml", Default::default()).unwrap();
            info!("grpc port of config service: {}", opts.config_port);
            info!("grpc port of this service: {}", opts.grpc_port);
            let _ = run(opts);
        }
    }
}

use cita_ng_proto::config::{
    config_service_client::ConfigServiceClient, Endpoint, RegisterEndpointInfo, ServiceId,
};

async fn register_endpoint(
    config_port: String,
    port: String,
) -> Result<bool, Box<dyn std::error::Error>> {
    let config_addr = format!("http://127.0.0.1:{}", config_port);
    let mut client = ConfigServiceClient::connect(config_addr).await?;

    // id of Consensus service is 1
    let request = Request::new(RegisterEndpointInfo {
        id: 1,
        endpoint: Some(Endpoint {
            hostname: "127.0.0.1".to_owned(),
            port,
        }),
    });

    let response = client.register_endpoint(request).await?;

    Ok(response.into_inner().is_success)
}

async fn get_endpoint(id: u64, config_port: String) -> Result<String, Box<dyn std::error::Error>> {
    let config_addr = format!("http://127.0.0.1:{}", config_port);
    let mut client = ConfigServiceClient::connect(config_addr).await?;

    let request = Request::new(ServiceId { id });

    let response = client.get_endpoint(request).await?;

    Ok(response.into_inner().port)
}

use cita_ng_proto::network::network_service_client::NetworkServiceClient;
use cita_ng_proto::network::RegisterInfo;

async fn register_network_msg_handler(
    config_port: String,
    port: String,
) -> Result<bool, Box<dyn std::error::Error>> {
    let config_addr = format!("http://127.0.0.1:{}", config_port);
    let mut client = ConfigServiceClient::connect(config_addr).await?;

    // id of Network service is 0
    let request = Request::new(ServiceId { id: 0 });

    let response = client.get_endpoint(request).await?;

    let network_grpc_port = response.into_inner().port;
    let network_addr = format!("http://127.0.0.1:{}", network_grpc_port);
    let mut client = NetworkServiceClient::connect(network_addr).await?;

    let request = Request::new(RegisterInfo {
        module_name: "consensus".to_owned(),
        hostname: "127.0.0.1".to_owned(),
        port,
    });

    let response = client.register_network_msg_handler(request).await?;

    Ok(response.into_inner().is_success)
}

use cita_ng_proto::common::{Empty, SimpleResponse};
use cita_ng_proto::consensus::{
    consensus_service_server::ConsensusService, consensus_service_server::ConsensusServiceServer,
    BlockDelayNumber, ConsensusConfiguration,
};
use tonic::{transport::Server, Request, Response, Status};

mod pos;
use pos::POS;
use std::sync::Arc;
use tokio::sync::RwLock;

// grpc server of consensus
pub struct PosServer {
    pos: Arc<RwLock<POS>>,
}

impl PosServer {
    fn new(pos: Arc<RwLock<POS>>) -> Self {
        PosServer { pos }
    }
}

#[tonic::async_trait]
impl ConsensusService for PosServer {
    async fn get_block_delay_number(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<BlockDelayNumber>, Status> {
        info!("get_block_delay_number request: {:?}", request);

        let pos = self.pos.read().await;
        let block_delay_number = pos.get_block_delay_number();
        let reply = BlockDelayNumber { block_delay_number };
        Ok(Response::new(reply))
    }
    async fn reconfigure(
        &self,
        request: Request<ConsensusConfiguration>,
    ) -> Result<Response<SimpleResponse>, Status> {
        info!("reconfigure request: {:?}", request);

        let config = request.into_inner();
        let mut pos = self.pos.write().await;
        pos.reconfigure(config);

        let reply = SimpleResponse { is_success: true };
        Ok(Response::new(reply))
    }
}

use cita_ng_proto::network::{
    network_msg_handler_service_server::NetworkMsgHandlerService,
    network_msg_handler_service_server::NetworkMsgHandlerServiceServer, NetworkMsg,
};
use std::time::Duration;
use tokio::time;

// grpc server of network msg handler
pub struct PosNetworkMsgHandlerServer {
    pos: Arc<RwLock<POS>>,
}

impl PosNetworkMsgHandlerServer {
    fn new(pos: Arc<RwLock<POS>>) -> Self {
        PosNetworkMsgHandlerServer { pos }
    }
}

#[tonic::async_trait]
impl NetworkMsgHandlerService for PosNetworkMsgHandlerServer {
    async fn process_network_msg(
        &self,
        request: Request<NetworkMsg>,
    ) -> Result<Response<SimpleResponse>, Status> {
        info!("process_network_msg request: {:?}", request);

        let msg = request.into_inner();
        if msg.module != "consensus" {
            Err(Status::invalid_argument("wrong module"))
        } else {
            {
                let pos = self.pos.read().await;
                pos.process_network_msg(msg).await;
            }

            let reply = SimpleResponse { is_success: true };
            Ok(Response::new(reply))
        }
    }
}

#[tokio::main]
async fn run(opts: RunOpts) -> Result<(), Box<dyn std::error::Error>> {
    let config_port_clone = opts.config_port.clone();
    let grpc_port_clone = opts.grpc_port.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(3));
        loop {
            // register endpoint
            {
                let ret =
                    register_endpoint(config_port_clone.clone(), grpc_port_clone.clone()).await;
                if ret.is_ok() && ret.unwrap() {
                    info!("register endpoint success!");
                    break;
                }
            }
            warn!("register endpoint failed! Retrying");
            interval.tick().await;
        }
    });

    let config_port_clone = opts.config_port.clone();
    let grpc_port_clone = opts.grpc_port.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(3));
        loop {
            // register endpoint
            {
                let ret = register_network_msg_handler(
                    config_port_clone.clone(),
                    grpc_port_clone.clone(),
                )
                .await;
                if ret.is_ok() && ret.unwrap() {
                    info!("register network msg handler success!");
                    break;
                }
            }
            warn!("register network msg handler failed! Retrying");
            interval.tick().await;
        }
    });

    let controller_port;
    let config_port_clone = opts.config_port.clone();
    let mut interval = time::interval(Duration::from_secs(3));
    loop {
        {
            // id of Controller service is 4
            let ret = get_endpoint(4, config_port_clone.clone()).await;
            if let Ok(port) = ret {
                info!("get controller endpoint success!");
                controller_port = port;
                break;
            }
        }
        warn!("get controller endpoint failed! Retrying");
        interval.tick().await;
    }
    info!("controller port: {}", controller_port);

    let network_port;
    let config_port_clone = opts.config_port.clone();
    let mut interval = time::interval(Duration::from_secs(3));
    loop {
        {
            // id of Network service is 0
            let ret = get_endpoint(0, config_port_clone.clone()).await;
            if let Ok(port) = ret {
                info!("get network endpoint success!");
                network_port = port;
                break;
            }
        }
        warn!("get network endpoint failed! Retrying");
        interval.tick().await;
    }
    info!("network port: {}", network_port);

    let addr_str = format!("127.0.0.1:{}", opts.grpc_port);
    let addr = addr_str.parse()?;

    let pos = Arc::new(RwLock::new(POS::new(controller_port, network_port)));

    let pos_clone = pos.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::new(0, 500000000));
        loop {
            interval.tick().await;
            {
                let pos = pos_clone.read().await;
                pos.mining().await;
            }
        }
    });

    let pos_server = PosServer::new(pos.clone());
    let pos_network_msg_handler = PosNetworkMsgHandlerServer::new(pos);

    info!("start grpc server!");
    Server::builder()
        .add_service(ConsensusServiceServer::new(pos_server))
        .add_service(NetworkMsgHandlerServiceServer::new(pos_network_msg_handler))
        .serve(addr)
        .await?;

    Ok(())
}
