use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use sui_data_ingestion_core::ReaderOptions;
use sui_sdk::SuiClientBuilder;

use harvestlib::EventExtractWorker;

use flate2::write::GzEncoder;
use flate2::Compression;

const BATCH_SIZE: usize = 1000;

use clap::Parser;

/// A simple event monitor and library to consume events from the Sui blockchain.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Number of checkpoints to process
    #[arg(long, default_value_t = 5)]
    concurrent: u64,

    /// URL of Sui full nodes
    #[arg(long, default_value = "https://fullnode.mainnet.sui.io:443")]
    full_node_url: String,

    /// URL of Sui checkpoint nodes
    #[arg(long, default_value = "https://checkpoints.mainnet.sui.io")]
    checkpoints_node_url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // define the events folder
    let events_folder = PathBuf::from("events");

    // Open a special file _next in the events folder to store the latest checkpoint
    let next_checkpoint_file = events_folder.join("_next");

    // If the file does not exist, create it and write 0 into it
    if !next_checkpoint_file.exists() {
        std::fs::write(&next_checkpoint_file, "0")?;
    }

    // Read the next checkpoint from the file
    let next_checkpoint = std::fs::read_to_string(&next_checkpoint_file)?.parse::<u64>()?;

    let sui_mainnet = SuiClientBuilder::default()
        .build(args.full_node_url)
        .await?;
    println!("Sui mainnet version: {}", sui_mainnet.api_version());

    // Get and print the latest checkpoint
    let latest_checkpoint = sui_mainnet
        .read_api()
        .get_latest_checkpoint_sequence_number()
        .await?;

    let limit = latest_checkpoint - next_checkpoint;

    // Custom reader options
    let mut options = ReaderOptions::default();
    options.batch_size = args.concurrent as usize;

    // Get a new Custom Worker
    let (executor, mut receiver) = EventExtractWorker::new(
        next_checkpoint,
        limit,
        |_e| true,
        args.checkpoints_node_url.clone(),
        args.concurrent as usize,
        Some(options),
        Some(PathBuf::from("cache")),
    )
    .await?;

    // spawn a task to process the received data
    tokio::spawn(async move {
        // By convention we store batches of 100 checkpoints
        let mut batch = Vec::with_capacity(100);

        while let Some((summary, data)) = receiver.recv().await {
            // If the end of epoch is not None write the checkpoint to the file
            if summary.end_of_epoch_data.is_some() {
                // Make a file name with the sequence number
                let filename = format!("{}_end_of_epoch.bcs", summary.sequence_number);
                println!("Writing end of epoch checkpoint to file: {}", filename);

                // Into the events folder
                let file = events_folder.join(filename);

                // Write the checkpoint to the file
                std::fs::write(&file, bcs::to_bytes(&summary).unwrap()).unwrap();
            }

            batch.push((summary.into_data(), data));

            // If the batch is full, process it
            if batch.len() == BATCH_SIZE {
                // encode the batch with bcs
                let bcs_data = bcs::to_bytes(&batch).unwrap();

                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(&bcs_data).unwrap();
                let gz_data = encoder.finish().unwrap();

                // Extract the first and last sequence numbers
                let first = batch.first().unwrap().0.sequence_number;
                let last = batch.last().unwrap().0.sequence_number;

                // Filename
                let filename = format!("{}_{}.events.bcs.gz", first, last);
                println!("Writing batch to file: {}", filename);

                // Make a file in events folder with the first and last sequence numbers
                let file = events_folder.join(filename);
                // Write the encoded batch to the file
                std::fs::write(&file, gz_data).unwrap();

                // Update the next checkpoint in the _next file
                std::fs::write(&next_checkpoint_file, (last + 1).to_string()).unwrap();

                // clear the batch
                batch.clear();
            }
        }
    });

    executor.await?;
    Ok(())
}
