use crossbeam_channel::{Receiver, RecvError};
use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use solana_entry::entry::Entry;
use solana_ledger::shred;
use solana_ledger::shred::merkle::Shred;
use solana_ledger::shred::{ ReedSolomonCache, ShredType, Shredder};
use solana_net_utils::SocketConfig;
use solana_perf::{
    packet::{PacketBatch, PacketBatchRecycler},
    recycler::Recycler,
};
use solana_sdk::clock::Slot;
use solana_streamer::{
    streamer,
    streamer::StreamerReceiveStats,
};
use std::collections::{ HashMap, HashSet};
use std::{
    net::{IpAddr},
    panic,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering}, Arc,
    },
    thread::{Builder, JoinHandle},
    time::{Duration},
};

use crate::{ ShredstreamProxyError};


pub fn start_forwarder_threads(
    src_addr: IpAddr,
    src_port: u16,
    num_threads: Option<usize>,
    metrics: Arc<ShredMetrics>,
    forward_stats: Arc<StreamerReceiveStats>,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    let num_threads = num_threads
        .unwrap_or_else(|| usize::from(std::thread::available_parallelism().unwrap()).max(4));

    let recycler: PacketBatchRecycler = Recycler::warmed(100, 1024);

    let last_slot_arc = Arc::new(AtomicU64::new(0));

    // spawn a thread for each listen socket. linux kernel will load balance amongst shared sockets
    let config = SocketConfig::default().reuseport(true);
    solana_net_utils::multi_bind_in_range_with_config(
        src_addr,
        (src_port, src_port + 1),
        config,
        num_threads,
    )
    .unwrap_or_else(|_| {
        panic!("Failed to bind listener sockets. Check that port {src_port} is not in use.")
    })
    .1
    .into_iter()
    .enumerate()
    .flat_map(|(thread_id, incoming_shred_socket)| {
        let (packet_sender, packet_receiver) = crossbeam_channel::unbounded();
        let listen_thread = streamer::receiver(
            format!("ssListen{thread_id}"),
            Arc::new(incoming_shred_socket),
            exit.clone(),
            packet_sender,
            recycler.clone(),
            forward_stats.clone(),
            Duration::default(), // do not coalesce since batching consumes more cpu cycles and adds latency.
            false,
            None,
            false,
        );

        let shutdown_receiver = shutdown_receiver.clone();

        let rs_cache = ReedSolomonCache::default();


        let last_slot = last_slot_arc.clone();

        let mut deshredded_entries:HashMap<Slot,Vec<Entry>> = HashMap::new();

        let metrics = metrics.clone();

        let exit = exit.clone();

        let send_thread = Builder::new()
            .name(format!("ssPxyTx_{thread_id}"))
            .spawn(move || {

                // Track parsed Shred as reconstructed_shreds[ slot ][ fec_set_index ] -> Vec<Shred>
                let mut all_reconstructed_shreds: HashMap<Slot, HashMap<u32, HashSet<Shred>>> = HashMap::new();


                while !exit.load(Ordering::Relaxed) {
                    crossbeam_channel::select! {
                        // forward packets
                        recv(packet_receiver) -> maybe_packet_batch => {
                           let res = recv_from_channel_and_send_multiple_dest(
                               maybe_packet_batch,
                                &mut all_reconstructed_shreds,
                                &metrics,
                                &rs_cache,&mut deshredded_entries,&last_slot);
                            //  avoid unwrap to prevent log spam from panic handler in each thread
                            if res.is_err(){
                                break;
                            }
                        }
                        // handle shutdown (avoid using sleep since it will hang under SIGINT)
                        recv(shutdown_receiver) -> _ => {
                            break;
                        }
                    }
                }
                info!("Exiting forwarder thread {thread_id}.");
            })
            .unwrap();

        [listen_thread, send_thread]
    })
    .collect::<Vec<JoinHandle<()>>>()
}


fn reconstruct_shreds(
    packet_batch: &PacketBatch,
    all_reconstructed_shreds: &mut HashMap<Slot, HashMap<u32, HashSet<Shred>>>,
    deshredded_entries: &mut HashMap<Slot,Vec<Entry>>,
    rs_cache: &ReedSolomonCache,
    last_slot: &Arc<AtomicU64>,
) -> usize {
    debug!("clean deshredded_entries!");
    deshredded_entries.clear();
    let mut recovered_count = 0;
    let mut slot_fec_index_to_iterate = HashSet::new();
    for packet in packet_batch.iter() {
        if let Some(data) = packet.data(..) {
            if let Ok(shred) =
                shred::Shred::new_from_serialized_shred(data.to_vec()).and_then(Shred::try_from)
            {
                let slot = shred.common_header().slot;
                let fec_set_index = shred.fec_set_index();
                all_reconstructed_shreds
                    .entry(slot)
                    .or_insert_with(HashMap::new)
                    .entry(fec_set_index)
                    .or_insert_with(HashSet::new)
                    .insert(shred);
                slot_fec_index_to_iterate.insert((slot, fec_set_index));
                let last = last_slot.load(Ordering::Relaxed);
                if slot > last {
                    info!("update last_slot:{}",slot);
                    last_slot.store(slot, Ordering::Relaxed);
                };
            }
        }
    }
    let last = last_slot.load(Ordering::Relaxed);
    debug!("slot_fec_index_to_iterate len:{},last_slot:{}",slot_fec_index_to_iterate.len(),last);
    for (slot, fec_set_index) in slot_fec_index_to_iterate {
        if let Some(fec_set_indexes) = all_reconstructed_shreds.get_mut(&slot) {
            if let Some(shreds) = fec_set_indexes.get_mut(&fec_set_index) {
                let (num_expected_shreds, num_data_shreds) = can_recover(shreds);
                // haven't received last data shred, haven't seen any coding shreds, so wait until more arrive
                if num_expected_shreds == 0
                    || (num_data_shreds < num_expected_shreds && shreds.len() < num_data_shreds as usize)
                {
                    // not enough data shreds, not enough shreds to recover
                    trace!("not enough data shreds, not enough shreds to recover");
                    continue;
                }


                if num_data_shreds < num_expected_shreds && shreds.len() as u16 >= num_data_shreds {
                    // recover
                    let merkle_shreds = shreds.iter().map(|a|{a.clone()}).collect_vec();
                    let Ok(recovered) = shred::merkle::recover(merkle_shreds, rs_cache)
                    else {
                        trace!("recover error!");
                        continue;
                    };

                    for shred in recovered {
                        match shred {
                            Ok(shred) => {
                                recovered_count += 1;
                                shreds.insert(shred);
                            }
                            Err(e) => warn!(
                        "Failed to recover shreds for slot {slot} fec set: {fec_set_index}: {e}"
                    ),
                        }
                    }
                }

                let sorted_shreds = shreds
                    .iter()
                    .filter(|s| s.shred_type() == ShredType::Data)
                    .sorted_by_key(|s| s.index())
                    .collect_vec();



                let sorted_payloads = shreds
                    .iter()
                    .filter(|s| s.shred_type() == ShredType::Data)
                    .sorted_by_key(|s| s.index())
                    .map(|s| s.payload().as_ref())
                    .collect_vec();
                let deshred_payload = match Shredder::deshred(sorted_payloads) {
                    Ok(v) => v,
                    Err(e) => {
                        trace!(
                            "start idx: {}, end idx: {}",
                            sorted_shreds.first().unwrap().index(),
                            sorted_shreds.last().as_ref().unwrap().index(),
                        );
                        trace!(
                            "slot {slot} failed to deshred fec_set_index {fec_set_index}. num_expected_shreds: {num_expected_shreds}, num_data_shreds: {num_data_shreds}. Err: {e}"
                        );
                        continue;
                    }
                };

                let entries =
                    match bincode::deserialize::<Vec<Entry>>(&deshred_payload) {
                        Ok(e) => e,
                        Err(e) => {
                            error!("slot {slot} failed to deserialize bincode with err: {e}");
                            continue;
                        }
                    };
                debug!(
                    "!slot {slot} fec_index: {fec_set_index} entries count: {}",
                    entries.len()
                );


                deshredded_entries.insert(slot, entries);
                if let Some(fec_set) = all_reconstructed_shreds.get_mut(&slot) {
                    fec_set.remove(&fec_set_index);
                    if fec_set.is_empty() {
                        all_reconstructed_shreds.remove(&slot);
                    }
                }
            } else {
                trace!("fec set index not find :{}",fec_set_index);
                continue;
            }
        } else {
            trace!("slot not find :{}",slot);
            continue;
        }
    }
    let last = last_slot.load(Ordering::Relaxed);
    if !deshredded_entries.is_empty() {
        debug!("deshredded_entries all size:{},last_slot:{}",deshredded_entries.values().map(|v| v.len()).sum::<usize>(),last);
        for (slot,entrys) in &mut *deshredded_entries {
            debug!("entry_slot:{}, entrys len:{} ",slot,
            entrys.len());
        }
        if let Some(last_entry)=deshredded_entries.get(&last){
            info!("last slot:{} entity len:{}",last,last_entry.len());
        }
    };
    recovered_count
}

/// check if we can reconstruct (having minimum number of data + coding shreds)
fn can_recover(
    shreds: &HashSet<Shred>,
) -> (
    u16, /* num_expected_shreds */
    u16, /* num_data_shreds */
) {
    let mut num_expected_shreds = 0;
    let mut data_shred_count = 0;
    for shred in shreds {
        match shred {
            Shred::ShredCode(s_code) => {
                num_expected_shreds = s_code.coding_header.num_coding_shreds;
            }
            Shred::ShredData(s) => {
                data_shred_count += 1;
                if s.data_complete() || s.last_in_slot() {
                    num_expected_shreds = shred.index() as u16 + 1;
                }
            }
        }
    }
    (num_expected_shreds, data_shred_count)
}

/// Broadcasts same packet to multiple recipients
/// Returns Err when unable to receive packets.
fn recv_from_channel_and_send_multiple_dest(
    maybe_packet_batch: Result<PacketBatch, RecvError>,
    all_reconstructed_shreds: &mut HashMap<Slot, HashMap<u32, HashSet<Shred>>>,
    metrics: &ShredMetrics,
    rs_cache: &ReedSolomonCache,
    deshredded_entries: &mut HashMap<Slot,Vec<Entry>>,
    last_slot: &Arc<AtomicU64>,
) -> Result<(), ShredstreamProxyError> {
    let packet_batch = maybe_packet_batch.map_err(ShredstreamProxyError::RecvError)?;

    debug!(
        "Got batch of {} packets, total size in bytes: {}",
        packet_batch.len(),
        packet_batch.iter().map(|x| x.meta().size).sum::<usize>()
    );

    //
    debug!("send reconstruct_shreds");

    let _shreds = reconstruct_shreds(&packet_batch, all_reconstructed_shreds, deshredded_entries,rs_cache,last_slot);

    debug!("add agg_received_cumulative!");
    metrics.agg_received_cumulative.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

pub struct ShredMetrics {
    // cumulative metrics (persist after reset)
    pub agg_received_cumulative: AtomicU64,
}

impl ShredMetrics {
    pub fn new() -> Self {
        Self {
            agg_received_cumulative: Default::default(),
        }
    }
}