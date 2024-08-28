use std::{collections::HashMap, path::PathBuf};

use anyhow::Result;
use async_trait::async_trait;
use prometheus::Registry;
use sui_data_ingestion_core::{
    DataIngestionMetrics, IndexerExecutor, ProgressStore, ReaderOptions, Worker, WorkerPool,
};
use sui_types::{
    event::{Event, EventID},
    full_checkpoint_content::CheckpointData,
    messages_checkpoint::{CertifiedCheckpointSummary, CheckpointSequenceNumber},
};

use futures::Future;
use serde::{Deserialize, Serialize};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
};

pub struct ShimProgressStore(pub u64);

#[async_trait]
impl ProgressStore for ShimProgressStore {
    async fn load(&mut self, _: String) -> Result<CheckpointSequenceNumber> {
        Ok(self.0)
    }
    async fn save(&mut self, _: String, _: CheckpointSequenceNumber) -> Result<()> {
        Ok(())
    }
}
// derive serialize
#[derive(Debug, Serialize, Deserialize)]
pub struct EventIndex {
    pub checkpoint_sequence_number: u64,
    pub transaction_sequence_number: u64,
    pub timestamp: u64,
}

impl EventIndex {
    pub fn new(
        checkpoint_sequence_number: u64,
        transaction_sequence_number: u64,
        timestamp: u64,
    ) -> Self {
        Self {
            checkpoint_sequence_number,
            transaction_sequence_number,
            timestamp,
        }
    }
}

pub type EventRecord = (EventIndex, EventID, Event);

pub struct EventExtractWorker<F>
where
    F: Fn(&EventRecord) -> bool,
{
    filter: F,
    sender: UnboundedSender<(CertifiedCheckpointSummary, Vec<EventRecord>)>,
}

impl<F> EventExtractWorker<F>
where
    F: Fn(&EventRecord) -> bool + Send + Sync + 'static,
{
    pub async fn new(
        initial: u64,
        length: u64,
        filter: F,
        remote_store_url: String,
        concurrency: usize,
        reader_options: Option<ReaderOptions>,
        cache_folder: Option<PathBuf>,
    ) -> Result<(
        impl Future<Output = Result<HashMap<String, CheckpointSequenceNumber>>>,
        UnboundedReceiver<(CertifiedCheckpointSummary, Vec<EventRecord>)>,
    )> {
        let (sender, mut receiver) =
            unbounded_channel::<(CertifiedCheckpointSummary, Vec<EventRecord>)>();
        let (sender_out, receiver_out) =
            unbounded_channel::<(CertifiedCheckpointSummary, Vec<EventRecord>)>();
        let (exit_sender, exit_receiver) = oneshot::channel();

        tokio::spawn(async move {
            let mut data = HashMap::new();
            let mut next_wait_for = initial;
            loop {
                if let Some((checkpoint_summary, item)) = receiver.recv().await {
                    data.insert(
                        checkpoint_summary.sequence_number,
                        (checkpoint_summary, item),
                    );

                    while data.contains_key(&next_wait_for) {
                        let data_item = data.remove(&next_wait_for).unwrap();
                        let Ok(_) = sender_out.send(data_item) else {
                            return;
                        };
                        next_wait_for += 1;

                        // Exit automatically if we reach the end
                        if next_wait_for == initial + length {
                            exit_sender.send(()).unwrap();
                            return;
                        }
                    }
                }
            }
        });

        let worker = Self { filter, sender };

        // Also make a custom executor
        let metrics = DataIngestionMetrics::new(&Registry::new());
        let progress_store = ShimProgressStore(initial);
        let mut executor = IndexerExecutor::new(progress_store, 1, metrics);
        let worker_pool = WorkerPool::new(worker, "workflow".to_string(), concurrency);
        executor.register(worker_pool).await?;

        let folder = cache_folder.unwrap_or_else(|| tempfile::tempdir().unwrap().into_path());

        let join = executor.run(
            folder,
            Some(remote_store_url),
            vec![],
            reader_options.unwrap_or_default(),
            exit_receiver,
        );

        Ok((join, receiver_out))
    }
}

#[async_trait]
impl<F> Worker for EventExtractWorker<F>
where
    F: Fn(&EventRecord) -> bool + Send + Sync,
{
    async fn process_checkpoint(&self, checkpoint: CheckpointData) -> Result<()> {
        let timestamp = checkpoint.checkpoint_summary.timestamp_ms;

        // Deconstruct checkpoint data
        let CheckpointData {
            checkpoint_summary,
            checkpoint_contents: _, // We don't need this
            transactions,
        } = checkpoint;

        // Extract all events from the checkpoint
        let mut events = vec![];
        transactions
            .into_iter()
            .enumerate()
            .for_each(|(tx_seq, tx)| {
                if tx.events.is_none() {
                    return;
                }
                tx.events
                    .unwrap()
                    .data
                    .into_iter()
                    .enumerate()
                    .for_each(|(event_seq, event)| {
                        // Define the event record
                        let record = (
                            EventIndex::new(
                                checkpoint_summary.sequence_number,
                                tx_seq as u64,
                                timestamp,
                            ),
                            EventID {
                                tx_digest: *tx.transaction.digest(),
                                event_seq: event_seq as u64,
                            },
                            event,
                        );

                        // Filter the events
                        if (self.filter)(&record) {
                            events.push(record);
                        }
                    });
            });

        // Send them to the aggregator
        self.sender.send((checkpoint_summary, events))?;

        Ok(())
    }
}
