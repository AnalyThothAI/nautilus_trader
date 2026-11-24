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

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    custom_data,
    data::{
        CustomData, CustomDataTrait, Data, DataBatch, DataType, NautilusDataType, register_arrow,
    },
};
use nautilus_serialization::{
    arrow::{ArrowSchemaProvider, DecodeDataFromRecordBatch, EncodeToRecordBatch},
    arrow_custom_data, ensure_custom_data_registered,
};
use object_store::{ObjectStoreExt, PutOptions};
use rstest::rstest;
use tempfile::TempDir;

use super::ParquetDataCatalog;
use crate::catalog::{traits::CatalogReader, types::CatalogQuery};

#[arrow_custom_data]
#[custom_data]
struct CatalogSequencePage {
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
}

#[arrow_custom_data]
#[custom_data]
struct CatalogCountedPage {
    sequence: u64,
    reject: bool,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
}

fn sequence_type(source: &str) -> DataType {
    DataType::new(
        CatalogSequencePage::type_name_static(),
        Some(Params::new([("source".to_owned(), source.into())].into())),
        None,
    )
}

fn row(sequence: u64, ts_init: u64, data_type: &DataType) -> CustomData {
    CustomData::new(
        Arc::new(CatalogSequencePage {
            sequence,
            ts_event: (ts_init + 17).into(),
            ts_init: ts_init.into(),
        }),
        data_type.clone(),
    )
}

fn rows(batch: DataBatch) -> Vec<CustomData> {
    let DataBatch::Custom(rows) = batch else {
        panic!("custom catalog returned another family");
    };
    rows.as_ref().to_vec()
}

fn sequence(row: &CustomData) -> u64 {
    row.data
        .as_any()
        .downcast_ref::<CatalogSequencePage>()
        .unwrap()
        .sequence
}

fn sequence_query() -> CatalogQuery {
    CatalogQuery::new(NautilusDataType::Custom {
        type_name: CatalogSequencePage::type_name_static().to_owned(),
    })
}

#[rstest]
fn custom_session_keeps_equal_timestamps_across_files_and_arrow_pages() {
    ensure_custom_data_registered::<CatalogSequencePage>();
    let directory = TempDir::new().unwrap();
    let mut catalog = ParquetDataCatalog::new(directory.path(), None, Some(23), None, Some(17));
    let left_type = sequence_type("left");
    let right_type = sequence_type("right");
    let mut left = vec![row(1, 99, &left_type)];
    left.extend(
        (2..=300)
            .rev()
            .filter(|i| i % 2 == 0)
            .map(|i| row(i, 100, &left_type)),
    );
    let mut right: Vec<_> = (3..300)
        .rev()
        .filter(|i| i % 2 == 1)
        .map(|i| row(i, 100, &right_type))
        .collect();
    right.push(row(301, 101, &right_type));

    let left_path = catalog
        .write_custom_data_batch(left, None, None, Some(true))
        .unwrap();
    let right_path = catalog
        .write_custom_data_batch(right, None, None, Some(true))
        .unwrap();
    assert_ne!(left_path, right_path);
    let mut query = catalog
        .query_batch_session(&sequence_query(), Some(128))
        .unwrap();

    let mut first = rows(query.next_batch().unwrap().unwrap());
    assert_eq!(
        first.len(),
        300,
        "the boundary timestamp must not be split at row 128"
    );
    first.sort_by_key(sequence);
    assert_eq!(
        first.iter().map(sequence).collect::<Vec<_>>(),
        (1..=300).collect::<Vec<_>>()
    );

    for item in &first {
        let expected_type = if sequence(item) == 1 || sequence(item).is_multiple_of(2) {
            &left_type
        } else {
            &right_type
        };
        assert_eq!(
            &item.data_type, expected_type,
            "per-file metadata changed during merge"
        );
    }
    let last = rows(query.next_batch().unwrap().unwrap());
    assert_eq!(last.iter().map(sequence).collect::<Vec<_>>(), vec![301]);
    assert!(query.next_batch().unwrap().is_none());

    let filtered = sequence_query()
        .with_range(Some(100_u64.into()), Some(101_u64.into()))
        .with_where_clause(Some("sequence >= 298".to_owned()));
    let mut query = catalog.query_batch_session(&filtered, Some(1)).unwrap();
    let mut selected = Vec::new();
    while let Some(batch) = query.next_batch().unwrap() {
        selected.extend(rows(batch).iter().map(sequence));
    }
    selected.sort_unstable();
    assert_eq!(selected, [298, 299, 300, 301]);

    let mut collected: Vec<_> = catalog
        .query_custom_data_dynamic(
            CatalogSequencePage::type_name_static(),
            None,
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap()
        .into_iter()
        .map(|item| {
            let Data::Custom(item) = item else {
                panic!("expected custom data");
            };
            sequence(&item)
        })
        .collect();
    collected.sort_unstable();
    assert_eq!(collected, (1..=301).collect::<Vec<_>>());
}

#[rstest]
fn custom_session_defers_later_files_and_propagates_late_decode_failure() {
    const FILES: u64 = 3;
    const PER_FILE: u64 = 65_536;
    const PAGE_ROWS: usize = 32;
    let decoded_rows = Arc::new(AtomicUsize::new(0));
    let highest_sequence = Arc::new(AtomicUsize::new(0));
    let largest_page = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&decoded_rows);
    let highest = Arc::clone(&highest_sequence);
    let largest = Arc::clone(&largest_page);
    register_arrow(
        CatalogCountedPage::type_name_static(),
        Arc::new(CatalogCountedPage::get_schema(None)),
        Box::new(|items| {
            let values: Vec<_> = items
                .iter()
                .map(|item| {
                    item.as_any()
                        .downcast_ref::<CatalogCountedPage>()
                        .unwrap()
                        .clone()
                })
                .collect();
            Ok(CatalogCountedPage::encode_batch(&HashMap::new(), &values)?)
        }),
        Box::new(move |metadata, batch| {
            count.fetch_add(batch.num_rows(), Ordering::SeqCst);
            largest.fetch_max(batch.num_rows(), Ordering::SeqCst);
            let values = CatalogCountedPage::decode_data_batch(metadata, batch)?;
            for value in &values {
                let Data::Custom(value) = value else {
                    panic!("expected custom data");
                };
                let value = value
                    .data
                    .as_any()
                    .downcast_ref::<CatalogCountedPage>()
                    .unwrap();
                highest.fetch_max(usize::try_from(value.sequence).unwrap(), Ordering::SeqCst);
                anyhow::ensure!(!value.reject, "injected late custom decode failure");
            }
            Ok(values)
        }),
    )
    .unwrap();
    let directory = TempDir::new().unwrap();
    let mut catalog =
        ParquetDataCatalog::new(directory.path(), None, Some(PAGE_ROWS), None, Some(1024));
    let data_type = DataType::new(CatalogCountedPage::type_name_static(), None, None);

    for file in 0..FILES {
        let data: Vec<_> = (file * PER_FILE + 1..=(file + 1) * PER_FILE)
            .map(|sequence| {
                CustomData::new(
                    Arc::new(CatalogCountedPage {
                        sequence,
                        reject: sequence == FILES * PER_FILE,
                        ts_event: sequence.into(),
                        ts_init: sequence.into(),
                    }),
                    data_type.clone(),
                )
            })
            .collect();
        catalog
            .write_custom_data_batch(data, None, None, None)
            .unwrap();
    }
    let mut query = catalog
        .query_batch_session(
            &CatalogQuery::new(NautilusDataType::Custom {
                type_name: CatalogCountedPage::type_name_static().to_owned(),
            }),
            Some(128),
        )
        .unwrap();
    assert_eq!(
        decoded_rows.load(Ordering::SeqCst),
        0,
        "query construction eagerly decoded files"
    );
    // The stream owns its query context; its creating catalog can go away immediately.
    drop(catalog);
    let first = rows(query.next_batch().unwrap().unwrap());
    assert_eq!(first.len(), 128);
    assert!(
        largest_page.load(Ordering::SeqCst) <= PAGE_ROWS,
        "the Arrow scanner ignored the configured page size"
    );
    assert!(
        highest_sequence.load(Ordering::SeqCst) < usize::try_from(PER_FILE).unwrap(),
        "the first chunk decoded the entire first file or opened a later file"
    );
    let mut observed = first.len();

    loop {
        match query.next_batch() {
            Ok(Some(batch)) => observed += rows(batch).len(),
            Ok(None) => panic!("late decoder failure disappeared"),
            Err(e) => {
                assert!(
                    e.to_string()
                        .contains("injected late custom decode failure")
                );
                break;
            }
        }
    }
    assert!(observed > usize::try_from(FILES * PER_FILE / 2).unwrap());
    assert!(largest_page.load(Ordering::SeqCst) <= PAGE_ROWS);
    assert!(query.next_batch().unwrap().is_none());
}

#[rstest]
fn custom_session_opens_corrupt_later_file_only_when_its_interval_is_reached() {
    ensure_custom_data_registered::<CatalogSequencePage>();
    let directory = TempDir::new().unwrap();
    let mut catalog = ParquetDataCatalog::new(directory.path(), None, Some(16), None, Some(32));
    let data_type = sequence_type("late-file-error");
    let first: Vec<_> = (1..=512).map(|i| row(i, i, &data_type)).collect();
    let last: Vec<_> = (513..=1024).map(|i| row(i, i, &data_type)).collect();
    catalog
        .write_custom_data_batch(first, None, None, None)
        .unwrap();
    let path = catalog
        .write_custom_data_batch(last, None, None, None)
        .unwrap();
    // The writer returns a catalog-relative object key, not a filesystem path.
    let object_path = catalog.to_object_path(&path.to_string_lossy()).unwrap();
    let object_store = Arc::clone(&catalog.object_store);
    catalog
        .execute_async(|| async {
            object_store.head(&object_path).await?;
            object_store
                .put_opts(
                    &object_path,
                    b"not a parquet footer".to_vec().into(),
                    PutOptions::default(),
                )
                .await?;
            Ok(())
        })
        .unwrap();

    let mut query = catalog
        .query_batch_session(&sequence_query(), Some(128))
        .unwrap();
    let first = rows(query.next_batch().unwrap().unwrap());
    assert_eq!(
        first.iter().map(sequence).collect::<Vec<_>>(),
        (1..=128).collect::<Vec<_>>()
    );
    let mut observed = first.len();

    loop {
        match query.next_batch() {
            Ok(Some(batch)) => observed += rows(batch).len(),
            Ok(None) => panic!("corrupt later file disappeared"),
            Err(_) => break,
        }
    }
    assert!(observed >= 384);
    assert!(query.next_batch().unwrap().is_none());
}

#[rstest]
fn custom_session_rejects_a_file_with_rows_before_its_declared_lower_bound() {
    ensure_custom_data_registered::<CatalogSequencePage>();
    let directory = TempDir::new().unwrap();
    let mut catalog = ParquetDataCatalog::new(directory.path(), None, Some(16), None, None);
    catalog
        .write_custom_data_batch(
            vec![row(1, 50, &sequence_type("bad-interval"))],
            Some(100_u64.into()),
            Some(200_u64.into()),
            None,
        )
        .unwrap();
    let mut query = catalog
        .query_batch_session(&sequence_query(), Some(128))
        .unwrap();
    let error = query.next_batch().unwrap_err();
    assert!(error.to_string().contains("precedes source lower bound"));
    assert!(query.next_batch().unwrap().is_none());
}

#[rstest]
fn collected_custom_query_preserves_unordered_file_compatibility() {
    ensure_custom_data_registered::<CatalogSequencePage>();
    let directory = TempDir::new().unwrap();
    let mut catalog = ParquetDataCatalog::new(directory.path(), None, Some(1), None, Some(1));
    let data_type = sequence_type("unordered-file");
    let file = catalog
        .write_custom_data_batch(
            vec![
                row(2, 20, &data_type),
                row(1, 10, &data_type),
                row(3, 30, &data_type),
            ],
            None,
            None,
            None,
        )
        .unwrap();
    let object_path = catalog.to_object_path(&file.to_string_lossy()).unwrap();
    let object_store = Arc::clone(&catalog.object_store);
    let metadata = catalog
        .execute_async(|| async { Ok(object_store.head(&object_path).await?) })
        .unwrap();
    assert!(metadata.size > 0);

    let collected = catalog
        .query_custom_data_dynamic(
            CatalogSequencePage::type_name_static(),
            None,
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();
    let collected: Vec<_> = collected
        .iter()
        .map(|data| {
            let Data::Custom(data) = data else {
                panic!("expected custom data");
            };
            sequence(data)
        })
        .collect();
    assert_eq!(collected, [1, 2, 3]);

    let mut stream = catalog
        .query_batch_session(&sequence_query(), Some(1))
        .unwrap();
    let error = stream.next_batch().unwrap_err();
    assert!(error.to_string().contains("regresses"));
    assert!(stream.next_batch().unwrap().is_none());
}
