use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use sui_data_ingestion_core::Worker;
use sui_types::{
    event::{Event, EventID},
    full_checkpoint_content::CheckpointData,
};

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

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
    where F: Fn(&EventRecord) -> bool
{
    filter: F,
    sender: UnboundedSender<(u64, Vec<EventRecord>)>,
}

impl<F> EventExtractWorker<F>
    where F: Fn(&EventRecord) -> bool + Send + Sync {
    pub fn new(initial: u64, filter : F) -> (Self, UnboundedReceiver<(u64, Vec<EventRecord>)>) {
        let (sender, mut receiver) = unbounded_channel::<(u64, Vec<EventRecord>)>();
        let (sender_out, receiver_out) = unbounded_channel::<(u64, Vec<EventRecord>)>();

        tokio::spawn(async move {
            let mut data = HashMap::new();
            let mut next_wait_for = initial;
            loop {
                if let Some((sequence_number, item)) = receiver.recv().await {
                    data.insert(sequence_number, item);

                    while data.contains_key(&next_wait_for) {
                        let data_item = data.remove(&next_wait_for).unwrap();
                        let Ok(_) = sender_out.send((next_wait_for, data_item)) else {
                            return;
                        };
                        next_wait_for += 1;
                    }
                }
            }
        });

        (Self { filter, sender }, receiver_out)
    }
}

#[async_trait]
impl<F> Worker for EventExtractWorker<F>
    where F: Fn(&EventRecord) -> bool + Send + Sync {
    async fn process_checkpoint(&self, checkpoint: CheckpointData) -> Result<()> {
        let timestamp = checkpoint.checkpoint_summary.timestamp_ms;

        // Deconstruct checkpoint data
        let CheckpointData {
            checkpoint_summary,
            checkpoint_contents: _, // We don't need this
            transactions } = checkpoint;

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
                        let record = (EventIndex::new(
                            checkpoint_summary.sequence_number,
                            tx_seq as u64,
                            timestamp,
                        ), EventID {
                            tx_digest: tx.transaction.digest().clone(),
                            event_seq: event_seq as u64,
                        }, event);

                        // Filter the events
                        if (self.filter)(&record) {
                            events.push(record);
                        }
                    });
            });

        // Send them to the aggregator
        self.sender
            .send((checkpoint_summary.sequence_number, events))?;

        Ok(())
    }
}
