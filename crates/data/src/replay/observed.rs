// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! A contiguous observation stream for opt-in read-only decision replay.

use std::collections::BTreeMap;

use nautilus_common::observation::{ObservationKey, ObservedDataEvent};

/// Merges recorded chunks by consuming sequence, never by source or chain timestamp.
///
/// Independent catalog partitions may be added in any order before consumption. The next
/// consumed row must be present; a missing sequence, duplicate, or clock regression faults the
/// stream. Empty buffers at a streaming boundary are not a claim that the recording is complete.
#[derive(Debug, Default)]
pub struct ObservedDataIterator {
    pending: BTreeMap<u64, ObservedDataEvent>,
    last: Option<ObservationKey>,
    fault: Option<String>,
}

impl ObservedDataIterator {
    /// Creates an empty stream beginning at observation one.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a complete catalog chunk without partially admitting a malformed chunk.
    ///
    /// # Errors
    ///
    /// Returns an error if the chunk is empty, its sequence is duplicated or already consumed, or
    /// the stream has failed. Interleaved chunks must all be present before their next sequence is consumed.
    pub fn add_batch(&mut self, events: Vec<ObservedDataEvent>) -> anyhow::Result<()> {
        self.check_error()?;
        anyhow::ensure!(!events.is_empty(), "Observation chunk was empty");
        let mut chunk = BTreeMap::new();

        for event in events {
            let sequence = event.key.sequence;
            anyhow::ensure!(
                sequence > self.last.map_or(0, |key| key.sequence),
                "Observation {sequence} is zero or already consumed"
            );
            anyhow::ensure!(
                !self.pending.contains_key(&sequence) && !chunk.contains_key(&sequence),
                "Duplicate observation {sequence}"
            );
            chunk.insert(sequence, event);
        }
        self.pending.append(&mut chunk);
        Ok(())
    }

    /// Returns the next input's identity after checking continuity and monotone observed time.
    ///
    /// # Errors
    ///
    /// Returns an error if an observation is missing, the clock regresses, or the sequence is exhausted.
    /// The first failure remains sticky.
    pub fn peek_key(&mut self) -> anyhow::Result<Option<ObservationKey>> {
        self.check_error()?;
        let Some(key) = self.pending.first_key_value().map(|(_, event)| event.key) else {
            return Ok(None);
        };
        let expected = self.last.map_or(0, |key| key.sequence).checked_add(1);
        let error = if expected != Some(key.sequence) {
            Some(format!(
                "Expected observation {expected:?}, received {}",
                key.sequence
            ))
        } else if self
            .last
            .is_some_and(|last| key.ts_observed < last.ts_observed)
        {
            Some(format!(
                "Observed clock regressed at observation {}",
                key.sequence
            ))
        } else {
            None
        };

        if let Some(error) = error {
            self.fault = Some(error);
            self.check_error()?;
        }
        Ok(Some(key))
    }

    /// Removes the next original input in observed order.
    ///
    /// # Errors
    ///
    /// Returns an error if [`Self::peek_key`] rejects the stream.
    pub fn next_event(&mut self) -> anyhow::Result<Option<ObservedDataEvent>> {
        let Some(key) = self.peek_key()? else {
            return Ok(None);
        };
        let (_, event) = self
            .pending
            .pop_first()
            .expect("peeked observation is present");
        self.last = Some(key);
        Ok(Some(event))
    }

    /// Returns the last consumed identity, including across streaming chunk boundaries.
    #[must_use]
    pub const fn last_observation(&self) -> Option<ObservationKey> {
        self.last
    }

    /// Refuses to discard observations which have not been delivered.
    ///
    /// # Errors
    ///
    /// Returns an error if an unconsumed chunk would be lost. The first failure remains sticky.
    pub fn clear_consumed(&mut self) -> anyhow::Result<()> {
        if !self.pending.is_empty() && self.fault.is_none() {
            self.fault = Some("Cannot clear unconsumed observations".to_string());
        }
        self.check_error()
    }

    fn check_error(&self) -> anyhow::Result<()> {
        if let Some(error) = &self.fault {
            anyhow::bail!("{error}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::messages::DataEvent;
    use nautilus_model::data::{Data, QuoteTick};
    use rstest::rstest;

    use super::*;

    fn input(sequence: u64, observed: u64) -> ObservedDataEvent {
        let quote = QuoteTick {
            ts_init: (100 - sequence).into(),
            ..QuoteTick::default()
        };
        ObservedDataEvent {
            key: ObservationKey {
                sequence,
                ts_observed: observed.into(),
            },
            event: DataEvent::Data(Data::Quote(quote)),
        }
    }

    #[rstest]
    fn merges_interleaved_chunks_without_rewriting_payload_time() {
        let mut iterator = ObservedDataIterator::new();
        iterator.add_batch(vec![input(4, 8), input(2, 7)]).unwrap();
        iterator.add_batch(vec![input(3, 7), input(1, 7)]).unwrap();
        let mut observed = Vec::new();

        while let Some(event) = iterator.next_event().unwrap() {
            let DataEvent::Data(Data::Quote(quote)) = event.event else {
                panic!("expected quote");
            };
            observed.push((
                event.key.sequence,
                event.key.ts_observed.as_u64(),
                quote.ts_init.as_u64(),
            ));
        }
        assert_eq!(observed, [(1, 7, 99), (2, 7, 98), (3, 7, 97), (4, 8, 96)]);
    }

    #[rstest]
    fn preserves_continuity_across_streaming_boundaries() {
        let mut iterator = ObservedDataIterator::new();
        iterator.add_batch(vec![input(1, 7)]).unwrap();
        assert_eq!(iterator.next_event().unwrap().unwrap().key.sequence, 1);
        assert!(iterator.next_event().unwrap().is_none());
        iterator.clear_consumed().unwrap();
        iterator.add_batch(vec![input(2, 7)]).unwrap();
        assert_eq!(iterator.next_event().unwrap().unwrap().key.sequence, 2);
        assert!(iterator.add_batch(vec![input(1, 7)]).is_err());
    }

    #[rstest]
    #[case::gap(3, 8)]
    #[case::regressed_time(2, 6)]
    fn invalid_continuity_faults_the_iterator(#[case] sequence: u64, #[case] timestamp: u64) {
        let mut iterator = ObservedDataIterator::new();
        iterator
            .add_batch(vec![input(1, 7), input(sequence, timestamp)])
            .unwrap();
        iterator.next_event().unwrap();
        let first = iterator.next_event().unwrap_err().to_string();
        assert_eq!(iterator.next_event().unwrap_err().to_string(), first);
        assert!(iterator.add_batch(vec![input(4, 9)]).is_err());
    }

    #[rstest]
    fn duplicate_chunk_admission_is_atomic() {
        let mut iterator = ObservedDataIterator::new();
        iterator.add_batch(vec![input(1, 7)]).unwrap();
        assert!(iterator.add_batch(vec![input(2, 7), input(1, 7)]).is_err());
        iterator.next_event().unwrap();
        assert!(iterator.next_event().unwrap().is_none());
    }

    #[rstest]
    fn unconsumed_clear_is_not_successful_exhaustion() {
        let mut iterator = ObservedDataIterator::new();
        iterator.add_batch(vec![input(1, 7)]).unwrap();
        assert!(iterator.clear_consumed().is_err());
        assert!(iterator.next_event().is_err());
    }
}
