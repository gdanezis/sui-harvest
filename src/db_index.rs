use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use rocksdb::{ColumnFamilyDescriptor, Options, SliceTransform, WriteBatch, DB};

use anyhow::Result;
use futures::{stream::{FuturesOrdered, FuturesUnordered}, StreamExt};
use sui_sdk::SuiClientBuilder;

use async_trait::async_trait;

use sui_data_ingestion_core::Worker;

use sui_types::{base_types::SuiAddress, full_checkpoint_content::CheckpointData, object::Owner};

use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::signal::ctrl_c;

use serde::{Deserialize, Serialize};

use object_store::path::Path;
use object_store::ObjectStore;

use std::sync::atomic::{AtomicU64, Ordering};

use clap::Parser;

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
    // --checkpoints-node-url https://s3.us-west-2.amazonaws.com/mysten-mainnet-checkpoints
}

pub struct IdentifierIndexWorker {
    data_sender: UnboundedSender<(u64, u64, Vec<IndexItem>)>,
}

impl IdentifierIndexWorker {
    pub async fn run() {

        let args = Args::parse();

        // define the events folder
        let events_folder = PathBuf::from("events_db");

        // Open a special file _next in the events folder to store the latest checkpoint
        let next_checkpoint_file = events_folder.join("_next_epoch");

        // If the file does not exist, create it and write 0 into it
        if !next_checkpoint_file.exists() {
            std::fs::write(&next_checkpoint_file, "59357732").expect("Cannot write");
        }

        // Read the next checkpoint from the file
        let next_checkpoint = std::fs::read_to_string(&next_checkpoint_file)
            .expect("Cannot read")
            .parse::<u64>()
            .expect("Cannot parse next checkpoint");

        // Column family ID table
        let mut cf_opts = Options::default();
        let transform = SliceTransform::create_fixed_prefix(32);
        cf_opts.set_prefix_extractor(transform);
        cf_opts.set_memtable_prefix_bloom_ratio(0.2);
        let cf = ColumnFamilyDescriptor::new("id_table", cf_opts);

        // DB options
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let db = Arc::new(DB::open_cf_descriptors(&db_opts, &events_folder, vec![cf]).unwrap());

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
        .with_url(remote_store_url.clone())
        // .with_client_options(client_options)
        //.with_retry(5)
        .build()
        .expect("Failed to build http store");

        // A tokio that downloads checkpoint data and sends it to the worker
        let join = tokio::spawn(async move {
            let checkpoint_number = Arc::new(AtomicU64::new(initial));

            let mut fut = FuturesUnordered::new();

            loop {
                while fut.len() < concurrency {
                    let future = async {

                        let xxx = checkpoint_number.fetch_add(1, Ordering::SeqCst);

                        let path = Path::from(format!("{}.chk", xxx));
                        let response = store.get(&path).await.expect("Cannot download");
                        let bytes = response.bytes().await.expect("No body");
                        let (_, checkpoint) =
                            bcs::from_bytes::<(u8, CheckpointData)>(&bytes).expect("Cannot parse");

                        // send the checkpoint
                        worker_sender.send(checkpoint).expect("Fail to send");
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
        let db_close = db.clone();
        tokio::spawn(async move {
            let mut all_txs = 0;
            let mut hash: HashMap<u64, _> = HashMap::new();
            let mut initial = initial;

            // STats
            let mut tmp_tx_num = 0;
            // Record the time
            let mut start = std::time::Instant::now();

            while let Some((checkpoint_seq, txs, index_terms)) = data_receiver.recv().await {
                hash.insert(checkpoint_seq, (checkpoint_seq, txs, index_terms));
                while let Some((checkpoint_seq, txs, index_terms)) = hash.remove(&initial) {
                    all_txs += txs;
                    // all_data.extend(index_terms);
                    initial += 1;
                    tmp_tx_num += txs;

                    // println!("Checkpoint: {} transactions: {}", checkpoint_seq, all_txs);

                    let id_handle = DB::cf_handle(&db, "id_table").unwrap();
                    let mut batch = WriteBatch::default();
                    for term in index_terms {
                        let mut key = [0; 32 + 2 + 4 + 2];
                        key[..32].copy_from_slice(term.identifier.as_ref());
                        key[32..34].copy_from_slice(&term.epoch.to_le_bytes());
                        key[34..38].copy_from_slice(&term.checkpoint.to_le_bytes());
                        key[38..40].copy_from_slice(&term.transaction_sequence.to_le_bytes());

                        batch.put_cf(&id_handle, term.identifier.as_ref(), vec![]);
                    }

                    db.write(batch).unwrap();

                    if tmp_tx_num > 1000 {
                        // Update the next checkpoint in the _next file
                        let first = checkpoint_seq + 1;
                        std::fs::write(&next_checkpoint_file, (first).to_string()).unwrap();

                        // Record the time
                        let elapsed = start.elapsed();
                        // Print the transactions per second
                        println!("Transactions per second: {} total: {}", tmp_tx_num as f64 / elapsed.as_secs_f64(), all_txs);

                        // Reset
                        tmp_tx_num = 0;
                        start = std::time::Instant::now();
                    }

                }
            }
        });

        // join.await.expect("Fail");

        match ctrl_c().await {
            Ok(_) => {
                println!("Ctrl-C received, shutting down");
                db_close.flush().unwrap();
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }

    }
}

#[derive(Serialize, Deserialize, Debug, Eq, Ord, PartialOrd, PartialEq)]
struct IndexItem {
    identifier: SuiAddress,
    epoch: u16,
    checkpoint: u32,
    transaction_sequence: u16,
}

#[async_trait]
impl Worker for IdentifierIndexWorker {
    async fn process_checkpoint(&self, checkpoint: CheckpointData) -> Result<()> {
        let mut index_terms: Vec<IndexItem> = vec![];

        // Go through all the effects and gather identifiers
        let mut identifiers: HashSet<SuiAddress> = HashSet::new();

        for (seq, transaction) in checkpoint.transactions.iter().enumerate() {

            // Extract events
            if transaction.events.is_some() {
                for e in transaction.events.as_ref().unwrap().data.iter() {
                    identifiers.insert(e.package_id.into());
                    identifiers.insert(e.type_.address.into());
                }
            }

            // Record sender address
            let sender = transaction.transaction.sender_address();
            identifiers.insert(sender);

            // Record input objects
            for o in transaction.input_objects.iter() {
                // Record type if move object
                if let Some(type_tag) = o.struct_tag() {
                    identifiers.insert(type_tag.address.into());
                }

                // Record owner or ID for shared objects
                match o.get_owner_and_id() {
                    Some((_, id)) => {
                        identifiers.insert(id.into());
                    }
                    None => {}
                }
            }

            // Record output objects
            for o in transaction.output_objects.iter() {
                // Record type if move object
                if let Some(type_tag) = o.struct_tag() {
                    identifiers.insert(type_tag.address.into());
                }

                // Record owner or ID for shared objects
                match o.get_owner_and_id() {
                    Some((Owner::AddressOwner(address), id)) => {
                        identifiers.insert(address);
                        identifiers.insert(id.into());
                    }
                    Some((_, id)) => {
                        identifiers.insert(id.into());

                    }
                    None => {}
                }
            }

            for id in &identifiers {
                index_terms.push(IndexItem {
                    identifier: *id,
                    epoch: checkpoint.checkpoint_summary.epoch as u16,
                    checkpoint: checkpoint.checkpoint_summary.sequence_number as u32,
                    transaction_sequence: seq as u16,
                });
            }

            identifiers.clear();
        }

        let checkpoint_sequence = checkpoint.checkpoint_summary.sequence_number;
        let txs = checkpoint.transactions.len() as u64;
        self.data_sender
            .send((checkpoint_sequence, txs, index_terms))
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
