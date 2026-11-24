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
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
//  the specific language governing permissions and limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, VecDeque},
};

use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use nautilus_core::UnixNanos;
use nautilus_model::data::{CustomData, HasTsInit};
use nautilus_serialization::arrow::{DecodeTypedFromRecordBatch, custom::CustomDataDecoder};

pub(super) type TypedPages<T> = Box<dyn Iterator<Item = anyhow::Result<Vec<T>>> + Send>;

pub(super) struct MergedPages<T> {
    sources: Vec<Option<TypedPages<T>>>,
    buffers: Vec<VecDeque<T>>,
    heads: BinaryHeap<Reverse<(UnixNanos, usize)>>,
    unopened: VecDeque<(UnixNanos, usize)>,
    lower_bounds: Vec<UnixNanos>,
    last_timestamps: Vec<Option<UnixNanos>>,
    page_size: usize,
    finished: bool,
}

impl<T: HasTsInit> MergedPages<T> {
    pub(super) fn new(sources: Vec<TypedPages<T>>, page_size: usize) -> Self {
        Self::with_lower_bounds(
            sources
                .into_iter()
                .map(|source| (UnixNanos::default(), source))
                .collect(),
            page_size,
        )
    }

    // Native catalog filenames give each source a lower bound. Delay opening later files
    // until they can contain the next row, including equality at a timestamp boundary.
    pub(super) fn with_lower_bounds(
        sources: Vec<(UnixNanos, TypedPages<T>)>,
        page_size: usize,
    ) -> Self {
        let lower_bounds: Vec<_> = sources.iter().map(|(lower, _)| *lower).collect();
        let mut unopened: Vec<_> = lower_bounds
            .iter()
            .enumerate()
            .map(|(index, lower)| (*lower, index))
            .collect();
        unopened.sort_unstable();
        Self {
            buffers: (0..sources.len()).map(|_| VecDeque::new()).collect(),
            last_timestamps: vec![None; sources.len()],
            sources: sources
                .into_iter()
                .map(|(_, source)| Some(source))
                .collect(),
            heads: BinaryHeap::new(),
            unopened: unopened.into(),
            lower_bounds,
            page_size: page_size.max(1),
            finished: false,
        }
    }

    fn read_page(&mut self) -> anyhow::Result<Option<Vec<T>>> {
        let mut page = Vec::with_capacity(self.page_size);
        while page.len() < self.page_size {
            self.open_sources()?;
            let Some(Reverse((_, index))) = self.heads.pop() else {
                break;
            };

            page.push(
                self.buffers[index]
                    .pop_front()
                    .expect("queued source has a row"),
            );
            self.advance(index)?;
        }

        Ok((!page.is_empty()).then_some(page))
    }

    fn open_sources(&mut self) -> anyhow::Result<()> {
        while let Some(&(lower, index)) = self.unopened.front() {
            if self
                .heads
                .peek()
                .is_some_and(|Reverse((ts, _))| *ts < lower)
            {
                break;
            }

            self.unopened.pop_front();
            self.advance(index)?;
        }
        Ok(())
    }

    fn advance(&mut self, index: usize) -> anyhow::Result<()> {
        while self.buffers[index].is_empty() {
            let Some(source) = self.sources[index].as_mut() else {
                return Ok(());
            };

            match source.next() {
                Some(page) => self.buffers[index].extend(page?),
                None => {
                    // Drop exhausted tasks, DataFusion contexts and page allocations promptly.
                    self.sources[index] = None;
                    self.buffers[index] = VecDeque::new();
                    return Ok(());
                }
            }
        }

        let row = self.buffers[index].front().expect("source has a row");
        anyhow::ensure!(
            row.ts_init() >= self.lower_bounds[index],
            "Catalog row timestamp {} precedes source lower bound {}",
            row.ts_init(),
            self.lower_bounds[index],
        );
        let ts_init = row.ts_init();
        anyhow::ensure!(
            self.last_timestamps[index].is_none_or(|previous| ts_init >= previous),
            "Catalog source timestamp {ts_init} regresses from {:?}",
            self.last_timestamps[index],
        );
        self.last_timestamps[index] = Some(ts_init);
        self.heads.push(Reverse((ts_init, index)));
        Ok(())
    }
}

impl<T: HasTsInit> Iterator for MergedPages<T> {
    type Item = anyhow::Result<Vec<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        match self.read_page() {
            Ok(Some(page)) => Some(Ok(page)),
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(e) => {
                self.finished = true;
                Some(Err(e))
            }
        }
    }
}

// The factory runs only when its source can contribute the next merged row. In particular,
// constructing a query must not start every file's bounded background prefetch at once.
pub(super) fn deferred_pages<T, F>(factory: F) -> TypedPages<T>
where
    T: Send + 'static,
    F: FnOnce() -> anyhow::Result<TypedPages<T>> + Send + 'static,
{
    let mut factory = Some(factory);
    let mut pages: Option<TypedPages<T>> = None;
    Box::new(std::iter::from_fn(move || {
        if let Some(factory) = factory.take() {
            match factory() {
                Ok(source) => pages = Some(source),
                Err(e) => return Some(Err(e)),
            }
        }
        pages.as_mut()?.next()
    }))
}

pub(super) fn decode_custom_pages(
    stream: SendableRecordBatchStream,
    metadata: HashMap<String, String>,
) -> impl Stream<Item = anyhow::Result<Vec<CustomData>>> {
    futures::stream::try_unfold((stream, metadata), |(mut stream, metadata)| async move {
        let Some(batch) = stream.next().await else {
            return Ok(None);
        };

        let rows = CustomDataDecoder::decode_custom_batch(&metadata, &batch?)?;
        Ok(Some((rows, (stream, metadata))))
    })
}

pub(super) fn decode_typed_pages<T>(
    stream: SendableRecordBatchStream,
) -> impl Stream<Item = anyhow::Result<Vec<T>>>
where
    T: DecodeTypedFromRecordBatch,
{
    let metadata = stream.schema().metadata().clone();
    futures::stream::try_unfold((stream, metadata), |(mut stream, metadata)| async move {
        let Some(batch) = stream.next().await else {
            return Ok(None);
        };

        let rows = T::decode_typed_batch(&metadata, batch?)?;
        Ok(Some((rows, (stream, metadata))))
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Poll,
    };

    use datafusion::{error::DataFusionError, physical_plan::stream::RecordBatchStreamAdapter};
    use nautilus_model::data::{QuoteTick, stubs::quote_audusd};
    use nautilus_serialization::arrow::{EncodeToRecordBatch, EncodingError};
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::within_page(vec![vec![10_u64, 9]])]
    #[case::across_pages(vec![vec![10_u64], vec![9]])]
    fn merged_pages_reject_source_timestamp_regression(#[case] timestamps: Vec<Vec<u64>>) {
        let source: TypedPages<QuoteTick> = Box::new(timestamps.into_iter().map(|page| {
            Ok(page
                .into_iter()
                .map(|ts_init| QuoteTick {
                    ts_init: ts_init.into(),
                    ..quote_audusd()
                })
                .collect())
        }));
        let mut merged = MergedPages::new(vec![source], 1);
        let error = merged.next().unwrap().unwrap_err();
        assert!(error.to_string().contains("Catalog source timestamp"));
        assert!(error.to_string().contains("regresses"));
        assert!(merged.next().is_none());
    }

    #[rstest]
    #[case::stream_error(false)]
    #[case::decode_error(true)]
    fn typed_pages_stop_polling_after_error(#[case] decode_error: bool) {
        let quote = quote_audusd();
        let batch = QuoteTick::encode_batch(&quote.metadata(), &[quote]).unwrap();
        let schema = batch.schema();

        let failure = if decode_error {
            Ok(batch.project(&[0]).unwrap())
        } else {
            Err(DataFusionError::Execution(
                "injected stream failure".to_string(),
            ))
        };

        let mut batches = vec![Ok(batch.clone()), failure, Ok(batch)].into_iter();
        let polls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&polls);

        let inner = futures::stream::poll_fn(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(batches.next())
        });

        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, inner));
        let decoded = decode_typed_pages::<QuoteTick>(stream);
        let mut items: Vec<_> = futures::executor::block_on_stream(Box::pin(decoded)).collect();

        assert_eq!(items.len(), 2);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(items.remove(0).unwrap(), vec![quote]);
        let error = items.remove(0).unwrap_err();

        if decode_error {
            assert!(matches!(
                error.downcast_ref::<EncodingError>(),
                Some(EncodingError::MissingColumn("ask_price", 1))
            ));
        } else {
            assert!(matches!(error.downcast_ref::<DataFusionError>(),
                Some(DataFusionError::Execution(message)) if message == "injected stream failure"));
        }
    }
}
