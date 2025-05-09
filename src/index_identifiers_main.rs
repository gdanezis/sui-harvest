use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Result;
use futures::{stream::FuturesUnordered, StreamExt};
use move_core_types::language_storage::StructTag;
use sui_sdk::SuiClientBuilder;

use async_trait::async_trait;

use sui_data_ingestion_core::Worker;

use sui_types::{
    base_types::SuiAddress, full_checkpoint_content::CheckpointData, object::Owner,
    transaction::TransactionDataAPI, Identifier, TypeTag,
};

use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use std::io::Write;

use object_store::path::Path;
use object_store::ObjectStore;

use std::sync::atomic::{AtomicU64, Ordering};

use clap::Parser;
use sha2::{Digest, Sha256};

/// A simple event monitor and library to consume events from the Sui blockchain.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Number of checkpoints to process
    #[arg(long, default_value_t = 50)]
    concurrent: u64,

    /// URL of Sui full nodes
    #[arg(long, default_value = "https://fullnode.mainnet.sui.io:443")]
    full_node_url: String,

    /// URL of Sui checkpoint nodes
    #[arg(long, default_value = "https://checkpoints.mainnet.sui.io")]
    checkpoints_node_url: String,
}

pub struct IdentifierIndexWorker {
    data_sender: UnboundedSender<(u64, u64, u64, Vec<IndexItem>)>,
}

fn write_identifiers_to_file(
    all_data: &mut Vec<IndexItem>,
    first: u64,
    epoch: u64,
    all_txs: u64,
    events_folder: &PathBuf,
) {
    // encode the batch with bcs

    all_data.sort_unstable();

    let bcs_data = bcs::to_bytes(&all_data).unwrap();
    let uncompressed_len = bcs_data.len();

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&bcs_data).unwrap();
    let gz_data = encoder.finish().unwrap();

    println!(
        "LEN BYTES: {} transactions: {} uncompressed: {}",
        gz_data.len(),
        all_txs,
        uncompressed_len
    );

    // Filename
    let filename = format!("{:016x}_{:08x}.index.bcs.gz", first, epoch);
    println!("Writing batch to file: {}", filename);

    // Make a file in events folder with the first and last sequence numbers
    let file = events_folder.join(filename);
    // Write the encoded batch to the file
    std::fs::write(&file, gz_data).unwrap();
}

impl IdentifierIndexWorker {
    pub async fn run() {
        let args = Args::parse();

        // define the events folder
        let events_folder = PathBuf::from("events");

        // Open a special file _next in the events folder to store the latest checkpoint
        let next_checkpoint_file = events_folder.join("_next");

        // If the file does not exist, create it and write 0 into it
        if !next_checkpoint_file.exists() {
            std::fs::write(&next_checkpoint_file, "0").expect("Cannot write");
        }

        // Read the next checkpoint from the file
        let next_checkpoint = std::fs::read_to_string(&next_checkpoint_file)
            .expect("Cannot read")
            .parse::<u64>()
            .expect("Cannot parse next checkpoint");

        let initial: u64 = next_checkpoint;
        let remote_store_url = args.checkpoints_node_url;
        let concurrency = args.concurrent as usize;

        let (data_sender, mut data_receiver) = unbounded_channel();
        let worker = Self { data_sender };
        let (worker_sender, mut worker_receiver) = unbounded_channel();

        // Turn the remote store string into a URL
        // let url = Url::parse(&remote_store_url).expect("Cannot parse url");
        // let (store, _path) = object_store::parse_url(&url).expect("Failed to open store from url");
        let store = object_store::http::HttpBuilder::new()
            .with_url(remote_store_url)
            // .with_client_options(client_options)
            // .with_retry(5)
            .build()
            .expect("Failed to build http store");

        // A tokio that downloads checkpoint data and sends it to the worker
        let join = tokio::spawn(async move {
            let checkpoint_number = Arc::new(AtomicU64::new(initial));

            let mut fut = FuturesUnordered::new();

            loop {
                while fut.len() < concurrency {
                    let future = async {
                        let next_checkpoint_id = checkpoint_number.fetch_add(1, Ordering::SeqCst);

                        let path = Path::from(format!("{}.chk", next_checkpoint_id));
                        loop {
                            let response = store.get(&path).await;
                            if response.is_err() {
                                println!("Error: {:?}", response.err());
                                continue;
                            }

                            let bytes = response.unwrap().bytes().await;
                            if bytes.is_err() {
                                println!("Error: {:?}", bytes.err());
                                continue;
                            }

                            let checkpoint_result =
                                bcs::from_bytes::<(u8, CheckpointData)>(&bytes.unwrap());
                            if checkpoint_result.is_err() {
                                println!("Error: {:?}", checkpoint_result.err());
                                continue;
                            }

                            let (_, checkpoint) = checkpoint_result.unwrap();

                            // send the checkpoint
                            worker_sender.send(checkpoint).expect("Fail to send");
                            break;
                        }
                    };
                    fut.push(future);
                }
                fut.next().await;
            }
        });

        // Make a task for the worker that receives checkpoints and processes them
        let _worker_task = tokio::spawn(async move {
            while let Some(checkpoint) = worker_receiver.recv().await {
                worker
                    .process_checkpoint(checkpoint)
                    .await
                    .expect("Fail to process");
            }
        });

        // A tokio task that receives data from the worker and writes it to files
        tokio::spawn(async move {
            let mut initial = initial;
            let mut all_data = Vec::with_capacity(1_000_000);
            let mut all_txs = 0;
            let mut first = initial;
            let mut hash: HashMap<u64, _> = HashMap::new();
            let mut prev_epoch = None;

            while let Some((checkpoint_seq, epoch, txs, index_terms)) = data_receiver.recv().await {
                hash.insert(checkpoint_seq, (checkpoint_seq, epoch, txs, index_terms));
                while let Some((checkpoint_seq, epoch, txs, index_terms)) = hash.remove(&initial) {
                    // First determine if we should flush to disk.
                    // (1) Size of file is > limit
                    // (2) The epoch has changed

                    if prev_epoch.is_none() {
                        prev_epoch = Some(epoch);
                    }

                    // Write buffer to disk
                    if all_txs >= 10_000 || prev_epoch.unwrap() != epoch {
                        write_identifiers_to_file(
                            &mut all_data,
                            first,
                            prev_epoch.unwrap(),
                            all_txs,
                            &events_folder,
                        );

                        // Update the next checkpoint in the _next file
                        first = checkpoint_seq + 1;
                        std::fs::write(&next_checkpoint_file, (first).to_string()).unwrap();

                        // clear data
                        all_data.clear();
                        all_txs = 0;
                    }

                    // Add transaction identifiers to buffer
                    all_txs += txs;
                    all_data.extend(index_terms);
                    initial += 1;
                    prev_epoch = Some(epoch);

                    println!("Checkpoint: {} transactions: {}", checkpoint_seq, all_txs);
                }
            }
        });

        join.await.expect("Fail");
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, Ord, PartialOrd, PartialEq)]
struct IndexItem {
    identifier: SuiAddress,
    epoch: u16,
    checkpoint: u32,
    transaction_sequence: u16,
    id_type: u8,
}

fn add_all_identifiers_type(identifiers: &mut HashSet<IndexAddress>, type_tag: &TypeTag) {
    match type_tag {
        TypeTag::Struct(struct_tag) => {
            add_all_identifiers_struct(identifiers, struct_tag);
        }
        TypeTag::Vector(inner_type_tag) => {
            add_all_identifiers_type(identifiers, inner_type_tag);
        }
        _ => {}
    }
}

// Compute the sha256 hash of an identifier and xor it to an address
fn hash_identifier(address: SuiAddress, identifier: &Identifier) -> SuiAddress {
    let mut hasher = Sha256::new();
    hasher.update(identifier.clone().into_bytes());
    let hash = hasher.finalize();

    let mut bytes = address.to_inner();
    for i in 0..32 {
        bytes[i] ^= hash[i];
    }

    let result = SuiAddress::from_bytes(bytes).expect("Address from bytes will work");
    result
}

fn add_all_identifiers_struct(identifiers: &mut HashSet<IndexAddress>, struct_tag: &StructTag) {
    let module_address: SuiAddress = struct_tag.address.into();
    identifiers.insert(IndexAddress::new(module_address.clone(), PACKAGE_ID));
    let module_name = hash_identifier(module_address, &struct_tag.module);
    identifiers.insert(IndexAddress::new(module_name.clone(), MODULE_ID));
    let struct_name = hash_identifier(module_name, &struct_tag.name);
    identifiers.insert(IndexAddress::new(struct_name, STRUCT_ID));

    for generic_type in struct_tag.type_params.iter() {
        add_all_identifiers_type(identifiers, generic_type);
    }
}

// Some constants
const PACKAGE_ID: u8 = 0;
const MODULE_ID: u8 = 1;
const STRUCT_ID: u8 = 2;
const ADDRESS_ID: u8 = 3;
const OBJECT_ID: u8 = 4;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
struct IndexAddress {
    address: SuiAddress,
    id_type: u8,
}

impl IndexAddress {
    fn new(address: SuiAddress, id_type: u8) -> Self {
        Self { address, id_type }
    }
}

#[async_trait]
impl Worker for IdentifierIndexWorker {
    async fn process_checkpoint(&self, checkpoint: CheckpointData) -> Result<()> {
        let mut index_terms: Vec<IndexItem> = vec![];

        // Go through all the effects and gather identifiers
        let mut identifiers: HashSet<IndexAddress> = HashSet::new();
        let epoch = checkpoint.checkpoint_summary.epoch as u64;

        for (seq, transaction) in checkpoint.transactions.iter().enumerate() {
            // Extract events
            if transaction.events.is_some() {
                for e in transaction.events.as_ref().unwrap().data.iter() {
                    identifiers.insert(IndexAddress::new(e.package_id.into(), PACKAGE_ID));
                    add_all_identifiers_struct(&mut identifiers, &e.type_);
                }
            }

            // Extract commands and index them
            transaction
                .transaction
                .intent_message()
                .value
                .move_calls()
                .iter()
                .for_each(|(package_id, _, _)| {
                    identifiers.insert(IndexAddress::new((*package_id).clone().into(), PACKAGE_ID));
                });

            // Record sender address
            let sender = transaction.transaction.sender_address();
            identifiers.insert(IndexAddress::new(sender, ADDRESS_ID));

            // Record input objects
            for o in transaction.input_objects.iter() {
                // Record type if move object
                if let Some(struct_tag) = o.struct_tag() {
                    add_all_identifiers_struct(&mut identifiers, &struct_tag);
                }

                // Record ID for shared objects - sender is owner of owned objects
                match o.get_owner_and_id() {
                    Some((
                        Owner::Shared {
                            initial_shared_version: _,
                        },
                        id,
                    )) => {
                        identifiers.insert(IndexAddress::new(id.into(), OBJECT_ID));
                    }
                    _ => {}
                }
            }

            // Record output objects
            for o in transaction.output_objects.iter() {
                // Record type if move object
                if let Some(struct_tag) = o.struct_tag() {
                    add_all_identifiers_struct(&mut identifiers, &struct_tag);
                }

                // Record owner or ID for shared objects
                match o.get_owner_and_id() {
                    Some((Owner::AddressOwner(address), id)) => {
                        identifiers.insert(IndexAddress::new(address, ADDRESS_ID));
                        identifiers.insert(IndexAddress::new(id.into(), OBJECT_ID));
                    }
                    Some((Owner::ObjectOwner(address), id)) => {
                        identifiers.insert(IndexAddress::new(address, OBJECT_ID));
                        identifiers.insert(IndexAddress::new(id.into(), OBJECT_ID));
                    }
                    Some((_, id)) => {
                        identifiers.insert(IndexAddress::new(id.into(), OBJECT_ID));
                    }
                    None => {}
                }
            }

            for id in identifiers.drain() {
                index_terms.push(IndexItem {
                    identifier: id.address,
                    epoch: checkpoint.checkpoint_summary.epoch as u16,
                    checkpoint: checkpoint.checkpoint_summary.sequence_number as u32,
                    transaction_sequence: seq as u16,
                    id_type: id.id_type,
                });
            }

            identifiers.clear();
        }

        let checkpoint_sequence = checkpoint.checkpoint_summary.sequence_number;
        let txs = checkpoint.transactions.len() as u64;
        self.data_sender
            .send((checkpoint_sequence, epoch, txs, index_terms))
            .expect("Fail to send data");

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let full_node_url = "https://fullnode.mainnet.sui.io:443";

    let sui_mainnet = SuiClientBuilder::default().build(full_node_url).await?;
    println!("Sui mainnet version: {}", sui_mainnet.api_version());

    // Get and print the latest checkpoint
    let _latest_checkpoint = sui_mainnet
        .read_api()
        .get_latest_checkpoint_sequence_number()
        .await?;

    let _indexer = IdentifierIndexWorker::run().await;

    Ok(())
}
