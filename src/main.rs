use std::{env, io, net::{IpAddr, Ipv4Addr, SocketAddr}, panic, path::{Path, PathBuf}, str::FromStr, sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
}, thread::{sleep, spawn, JoinHandle}, time::Duration};

use crossbeam_channel::{Receiver, RecvError, Sender};
use env_logger::TimestampPrecision;
use log::*;
use signal_hook::consts::{SIGINT, SIGTERM};
use solana_client::client_error::{reqwest, ClientError};
use solana_metrics::set_host_id;
use solana_sdk::signature::read_keypair_file;
use solana_streamer::streamer::StreamerReceiveStats;
use thiserror::Error;
use tokio::runtime::Runtime;
use tonic::Status;

use crate::{forwarder::ShredMetrics, token_authenticator::BlockEngineConnectionError};

mod forwarder;
mod heartbeat;
mod token_authenticator;

#[derive(Debug, Error)]
pub enum ShredstreamProxyError {
    #[error("TonicError {0}")]
    TonicError(#[from] tonic::transport::Error),
    #[error("GrpcError {0}")]
    GrpcError(#[from] Status),
    #[error("ReqwestError {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("SerdeJsonError {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("RpcError {0}")]
    RpcError(#[from] ClientError),
    #[error("BlockEngineConnectionError {0}")]
    BlockEngineConnectionError(#[from] BlockEngineConnectionError),
    #[error("RecvError {0}")]
    RecvError(#[from] RecvError),
    #[error("IoError {0}")]
    IoError(#[from] io::Error),
    #[error("Shutdown")]
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct ShredstreamArgs {
    block_engine_url: String,

    /// Manual override for auth service address. For internal use.
    auth_url: Option<String>,

    /// Path to keypair file used to authenticate with the backend.
    auth_keypair: PathBuf,

    desired_regions: Vec<String>,

    common_args: CommonArgs,
}

#[derive(Clone, Debug)]
struct CommonArgs {
    /// Address where Shredstream proxy listens.
    src_bind_addr: IpAddr,

    /// Port where Shredstream proxy listens. Use `0` for random ephemeral port.
    src_bind_port: u16,

    /// Public IP address to use.
    /// Overrides value fetched from `ifconfig.me`.
    public_ip: Option<IpAddr>,

    /// Number of threads to use. Defaults to use up to 4.
    num_threads: Option<usize>,
}

/// Returns public-facing IPV4 address
pub fn get_public_ip() -> reqwest::Result<IpAddr> {
    info!("Requesting public ip from ifconfig.me...");
    let client = reqwest::blocking::Client::builder()
        .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        .build()?;
    let response = client.get("https://ifconfig.me/ip").send()?.text()?;
    let public_ip = IpAddr::from_str(&response).unwrap();
    info!("Retrieved public ip: {public_ip:?}");

    Ok(public_ip)
}

// Creates a channel that gets a message every time `SIGINT` is signalled.
fn shutdown_notifier(exit: Arc<AtomicBool>) -> io::Result<(Sender<()>, Receiver<()>)> {
    let (s, r) = crossbeam_channel::bounded(256);
    let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;

    let s_thread = s.clone();
    spawn(move || {
        for _ in signals.forever() {
            exit.store(true, Ordering::SeqCst);
            // send shutdown signal multiple times since crossbeam doesn't have broadcast channels
            // each thread will consume a shutdown signal
            for _ in 0..256 {
                if s_thread.send(()).is_err() {
                    break;
                }
            }
        }
    });

    Ok((s, r))
}

fn main() -> Result<(), ShredstreamProxyError> {
    dotenvy::dotenv().unwrap();
    env_logger::builder().format_timestamp(Some(TimestampPrecision::Millis)).init();
    set_host_id(hostname::get()?.into_string().unwrap());

    let exit = Arc::new(AtomicBool::new(false));
    let (shutdown_sender, shutdown_receiver) =
        shutdown_notifier(exit.clone()).expect("Failed to set up signal handler");
    let panic_hook = panic::take_hook();
    {
        let exit = exit.clone();
        panic::set_hook(Box::new(move |panic_info| {
            exit.store(true, Ordering::SeqCst);
            let _ = shutdown_sender.send(());
            error!("exiting process");
            sleep(Duration::from_secs(1));
            // invoke the default handler and exit the process
            panic_hook(panic_info);
        }));
    }

    let block_engine_url = &env::var("BLOCK_ENGINE_URL").expect("Block engine url not set");

    let auth_keypair= &env::var("AUTH_KEYPAIR").expect("AUTH_KEYPAIR is not set");

    let desired_regions = &env::var("DESIRED_REGIONS").expect("DEIRED_REGIONS is not set");

    let vec_cities: Vec<String> = desired_regions.split(',').map(String::from).collect();

    let num_threads: usize = (&env::var("NUM_THREADS").unwrap_or("4".to_string())).parse().unwrap();

    let src_bind_addr:Ipv4Addr = (&env::var("BIND_ADDR").unwrap_or("127.0.0.1".to_string())).parse().unwrap();

    let src_bind_port: u16 = (&env::var("BIND_PORT").unwrap_or("20_000".to_string())).parse().unwrap();

    let shredstream_args = ShredstreamArgs {
        block_engine_url: block_engine_url.to_owned(),
        auth_url: None,
        auth_keypair: PathBuf::from(auth_keypair),
        desired_regions: vec_cities,
        common_args: CommonArgs {
            src_bind_addr: src_bind_addr.into(),
            src_bind_port,
            public_ip: None,
            num_threads: Some(num_threads),
        },
    };

    let metrics = Arc::new(ShredMetrics::new());

    let runtime = Runtime::new()?;
    let mut thread_handles = vec![];
    let heartbeat_hdl = start_heartbeat(
        shredstream_args.clone(),
        &exit,
        &shutdown_receiver,
        runtime,
        metrics.clone(),
    );
    thread_handles.push(heartbeat_hdl);

    let forward_stats = Arc::new(StreamerReceiveStats::new("shredstream_proxy-listen_thread"));
    let common_args = shredstream_args.common_args;

    let forwarder_hdls = forwarder::start_forwarder_threads(
        common_args.src_bind_addr,
        common_args.src_bind_port,
        common_args.num_threads,
        metrics.clone(),
        forward_stats.clone(),
        shutdown_receiver.clone(),
        exit.clone(),
    );
    thread_handles.extend(forwarder_hdls);

    let report_metrics_thread = {
        let exit = exit.clone();
        spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                sleep(Duration::from_secs(1));
                forward_stats.report();
            }
        })
    };
    thread_handles.push(report_metrics_thread);

    info!(
        "Shredstream started, listening on {}:{}/udp.",
        common_args.src_bind_addr, common_args.src_bind_port
    );

    for thread in thread_handles {
        thread.join().expect("thread panicked");
    }

    info!(
        "Exiting Shredstream,{} .",
        metrics.agg_received_cumulative.load(Ordering::Relaxed),
    );
    Ok(())
}

fn start_heartbeat(
    args: ShredstreamArgs,
    exit: &Arc<AtomicBool>,
    shutdown_receiver: &Receiver<()>,
    runtime: Runtime,
    metrics: Arc<ShredMetrics>,
) -> JoinHandle<()> {
    let auth_keypair = Arc::new(
        read_keypair_file(Path::new(&args.auth_keypair)).unwrap_or_else(|e| {
            panic!(
                "Unable to parse keypair file. Ensure that file {:?} is readable. Error: {e}",
                args.auth_keypair
            )
        }),
    );

    heartbeat::heartbeat_loop_thread(
        args.block_engine_url.clone(),
        args.auth_url.unwrap_or(args.block_engine_url),
        auth_keypair,
        args.desired_regions,
        SocketAddr::new(
            args.common_args
                .public_ip
                .unwrap_or_else(|| get_public_ip().unwrap()),
            args.common_args.src_bind_port,
        ),
        runtime,
        "shredstream_proxy".to_string(),
        metrics,
        shutdown_receiver.clone(),
        exit.clone(),
    )
}
