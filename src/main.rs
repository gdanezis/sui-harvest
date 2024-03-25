use std::collections::HashMap;

use anyhow::Result;
use sui_data_ingestion_core::setup_single_workflow;
use sui_sdk::SuiClientBuilder;

use harvestlib::EventExtractWorker;
use move_core_types::language_storage::StructTag;
use sui_types::TypeTag;

use colored::Colorize;

use clap::Parser;

/// A simple event monitor and library to consume events from the Sui blockchain.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {

    /// Number of checkpoints to process
    #[arg(short, long, default_value_t = 10)]
    count: u64,

    /// Whether to follow in real time
    #[arg(short, long, default_value_t = false)]
    follow: bool,

    /// Bottom percentage to suppress
    #[arg(short, long, default_value_t = 0.5)]
    suppress: f64,

}

fn tag_to_short_string(tag_: &TypeTag) -> String {
    match tag_ {
        TypeTag::Struct(struct_tag) => type_to_short_string(struct_tag),
        TypeTag::Vector(type_tag) => format!("Vector<{}>", tag_to_short_string(type_tag)),
        _ => tag_.to_canonical_string(false),
    }
}

fn type_to_short_string(type_: &StructTag) -> String {
    let base = format!(
        "{}::{}",
        type_.module,
        type_.name,
    );

    if type_.type_params.is_empty() {
        base
    } else {
        let type_params = type_
            .type_params
            .iter()
            .map(|type_param| tag_to_short_string(type_param))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{}<{}>", base, type_params)
    }
}

#[tokio::main]
async fn main() -> Result<()> {

    let args = Args::parse();

    let sui_mainnet = SuiClientBuilder::default()
        .build("https://fullnode.mainnet.sui.io:443")
        .await?;
    println!("Sui mainnet version: {}", sui_mainnet.api_version());

    // Get and print the latest checkpoint
    let latest_checkpoint = sui_mainnet
        .read_api()
        .get_latest_checkpoint_sequence_number()
        .await?;

    let limit = args.count;

    let initial = if args.follow {
        println!("Following the latest checkpoint ({}) ...", latest_checkpoint);
        latest_checkpoint
    } else {
        println!("Get events from checkpoints {} ... {}", (latest_checkpoint - limit).max(0), latest_checkpoint);
        (latest_checkpoint - limit).max(0)
    };

    // Get a new Custom Worker
    let (worker, mut receiver)
        = EventExtractWorker::new(initial, |_e| true);

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


        while let Some((_sequence_number, data)) = receiver.recv().await {

            // Update the histogram
            data.iter().for_each(|(_index, _id, event)| {
                let entry = histogram.entry(
                    event.type_.address.clone()).or_insert((0, HashMap::new()));
                entry.0 += 1;
                let entry = entry.1.entry(event.type_.clone()).or_insert(0);
                *entry += 1;
            });

            number += 1;
            if number == limit {
                term_sender.send(()).unwrap();
                break;
            }
        }

        // Print all entries in the histogram, sorted in descending order of value
        let mut histogram: Vec<_> = histogram.into_iter().collect();
        histogram.sort_by(|a, b| b.1.0.cmp(&a.1.0));

        // Sum all events
        let total_events: usize = histogram.iter().map(|(_type_, value)| value.0).sum();
        // Define the cutoff to suppress
        let cutoff = (total_events as f64 * args.suppress / 100.0).round() as usize;
        if cutoff > 0 {
            println!("Suppressing packages with fewer than {} events", cutoff);
        }

        for (type_, value) in histogram.into_iter() {

            if value.0 < cutoff {
                continue;
            }

            println!("\x1b[34m{:<5}\x1b[0m {}", value.0, type_.to_string().red());

            let mut inner_histogram: Vec<_> = value.1.into_iter().collect();
            inner_histogram.sort_by(|a, b| b.1.cmp(&a.1));

            for (type_, value) in inner_histogram.into_iter() {
                println!("       \x1b[34m{:5}\x1b[0m : {}", value, type_to_short_string(&type_).green());
            }
        }
    });

    executor.await?;
    Ok(())
}
