use std::collections::HashMap;

use anyhow::Result;
use sui_data_ingestion_core::setup_single_workflow;
use sui_sdk::SuiClientBuilder;

use harvestlib::EventExtractWorker;

#[tokio::main]
async fn main() -> Result<()> {
    let sui_mainnet = SuiClientBuilder::default()
        .build("https://fullnode.mainnet.sui.io:443")
        .await?;
    println!("Sui mainnet version: {}", sui_mainnet.api_version());

    // Get and print the latest checkpoint
    let latest_checkpoint = sui_mainnet
        .read_api()
        .get_latest_checkpoint_sequence_number()
        .await?;

    let limit = 1000;
    let initial = (latest_checkpoint - limit).max(0);

    // Get a new Custom Worker
    let (worker, mut receiver) = EventExtractWorker::new(initial);

    let (executor, term_sender) = setup_single_workflow(
        worker,
        "https://checkpoints.mainnet.sui.io".to_string(),
        initial, /* initial checkpoint number */
        5,       /* concurrency */
        None,    /* reader options */
    )
    .await?;

    // spawn a task to process the received data
    tokio::spawn(async move {
        let mut number = 0;

        // Histogram of identifiers
        let mut histogram = HashMap::new();

        while let Some((sequence_number, data)) = receiver.recv().await {
            println!(
                "Received data: {:?} Number of events: {:?}",
                sequence_number,
                data.len()
            );

            // Update the histogram
            data.iter().for_each(|(_index, _id, event)| {
                let entry = histogram.entry(event.type_.clone()).or_insert(0);
                *entry += 1;
            });

            number += 1;
            if number == limit {
                term_sender.send(()).unwrap();
                break;
            }
        }

        // Print all entries in the histogram, sorted in descending order of value
        println!("Histogram:");
        let mut histogram: Vec<_> = histogram.into_iter().collect();
        histogram.sort_by(|a, b| b.1.cmp(&a.1));

        for (type_, value) in histogram.iter() {
            println!("{:5} : {}", value, type_.to_canonical_string(false));
        }
    });

    executor.await?;
    Ok(())
}
