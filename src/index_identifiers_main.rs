use std::{
    any::Any,
    collections::{HashMap, HashSet},
    path::PathBuf,
};

use anyhow::Result;
use sui_sdk::SuiClientBuilder;

use async_trait::async_trait;
use prometheus::Registry;

use sui_data_ingestion_core::{
    DataIngestionMetrics, IndexerExecutor, ProgressStore, ReaderOptions, Worker, WorkerPool,
};

use sui_types::{
    base_types::SuiAddress,
    event::{Event, EventID},
    full_checkpoint_content::CheckpointData,
    messages_checkpoint::{CertifiedCheckpointSummary, CheckpointSequenceNumber},
    object::Owner,
    transaction,
};

use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
};

use serde::{Serialize, Deserialize};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;

pub struct ShimProgressStore(pub u64);

#[async_trait]
impl ProgressStore for ShimProgressStore {
    async fn load(&mut self, _: String) -> Result<CheckpointSequenceNumber> {
        // println!("Loading checkpoint sequence number: {}", self.0);
        Ok(self.0)
    }
    async fn save(&mut self, _: String, seq: CheckpointSequenceNumber) -> Result<()> {
        // println!("Saving checkpoint sequence number: {}", self.0);
        self.0 = seq;
        Ok(())
    }
}

pub struct IdentifierIndexWorker {
    data_sender: UnboundedSender<(u64, u64, Vec<IndexItem>)>,
}

impl IdentifierIndexWorker {
    pub async fn run() {

        // define the events folder
        let events_folder = PathBuf::from("events");

        // Open a special file _next in the events folder to store the latest checkpoint
        let next_checkpoint_file = events_folder.join("_next");

        // If the file does not exist, create it and write 0 into it
        if !next_checkpoint_file.exists() {
            std::fs::write(&next_checkpoint_file, "0").expect("Cannot write");
        }

        // Read the next checkpoint from the file
        let next_checkpoint = std::fs::read_to_string(&next_checkpoint_file).expect("Cannot read").parse::<u64>().expect("Cannot parse next checkpoint");



        let initial = next_checkpoint;
        let remote_store_url = "https://checkpoints.mainnet.sui.io";
        let cache_folder = PathBuf::from("cache");
        let concurrency = 8;

        let (data_sender, mut data_receiver) = unbounded_channel();
        let worker = Self { data_sender };

        let (exit_sender, exit_receiver) = oneshot::channel();

        // Also make a custom executor
        let metrics = DataIngestionMetrics::new(&Registry::new());
        let progress_store = ShimProgressStore(initial);
        let mut executor = IndexerExecutor::new(progress_store, 1, metrics);
        let worker_pool = WorkerPool::new(worker, "workflow".to_string(), concurrency);
        executor.register(worker_pool).await.expect("Fail");

        let folder = cache_folder;

        let join = executor.run(
            folder,
            Some(remote_store_url.into()),
            vec![],
            ReaderOptions::default(),
            exit_receiver,
        );

        tokio::spawn(async move {
            let mut initial = initial;
            let mut all_data = Vec::with_capacity(1_000_000);
            let mut all_txs = 0;
            let mut first = initial;
            let mut hash: HashMap<u64, _> = HashMap::new();

            while let Some((checkpoint_seq, txs, index_terms)) = data_receiver.recv().await {
                hash.insert(checkpoint_seq, (checkpoint_seq, txs, index_terms));
                while let Some((checkpoint_seq, txs, index_terms)) = hash.remove(&initial) {
                    all_txs += txs;
                    all_data.extend(index_terms);
                    initial += 1;

                    println!("Checkpoint: {} transactions: {}", checkpoint_seq, all_txs);

                    if all_txs >= 1_000_000 {
                        // encode the batch with bcs

                        all_data.sort();

                        let bcs_data = bcs::to_bytes(&all_data).unwrap();
                        let uncompressed_len = bcs_data.len();

                        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                        encoder.write_all(&bcs_data).unwrap();
                        let gz_data = encoder.finish().unwrap();

                        println!("LEN BYTES: {} transactions: {} uncompressed: {}", gz_data.len(), all_txs, uncompressed_len);

                        // Filename
                        let filename = format!("{:016x}.index.bcs.gz", first);
                        println!("Writing batch to file: {}", filename);

                        // Make a file in events folder with the first and last sequence numbers
                        let file = events_folder.join(filename);
                        // Write the encoded batch to the file
                        std::fs::write(&file, gz_data).unwrap();


                        // Update the next checkpoint in the _next file
                        first = checkpoint_seq+1;
                        std::fs::write(&next_checkpoint_file, (first).to_string()).unwrap();

                        // clear
                        all_data.clear();
                        all_txs = 0;

                    }
                }
            }
        });

        join.await.expect("Fail");
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[derive(Eq, Ord, PartialOrd, PartialEq)]
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
                }
            }

            // Record sender address
            let sender = transaction.transaction.sender_address();
            identifiers.insert(sender);

            // Record output objects
            for o in transaction.output_objects.iter() {

                // Record type if move object
                if let Some(type_tag) = o.struct_tag() {
                    identifiers.insert(type_tag.address.into());
                }

                // Record owner or ID for shared objects
                match o.get_owner_and_id() {
                    Some((Owner::AddressOwner(address), id)) => {
                        identifiers.insert(address.into());
                    }
                    Some((Owner::Shared { .. }, id)) => {
                        identifiers.insert(id.into());
                    }
                    Some((Owner::Immutable, id)) => {
                        identifiers.insert(id.into());
                    }

                    Some((_, id)) => {}
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
    let latest_checkpoint = sui_mainnet
        .read_api()
        .get_latest_checkpoint_sequence_number()
        .await?;

    let indexer = IdentifierIndexWorker::run().await;

    Ok(())
}
