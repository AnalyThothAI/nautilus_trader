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

//! Actual data ingress ordering for opt-in, read-only decision replay.
//!
//! A recording owns one runtime thread. The recorder runs before the `DataEngine` sees each
//! `DataEvent`; source timestamps are not rewritten. Decision timers and external controls must
//! enter this same channel as typed custom data. Recorder failures are sticky and prevent further
//! observed ingress. The run owner checks [`DataObservationRecorder::check_error`] before sealing.

use std::{cell::RefCell, fmt::Debug, marker::PhantomData, rc::Rc};

use nautilus_core::UnixNanos;
use serde::{Deserialize, Serialize};

use crate::{clock::Clock, messages::DataEvent};

/// The actual consuming order and clock timestamp of one data ingress.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationKey {
    /// The contiguous, one-based sequence within one recording.
    pub sequence: u64,
    /// The runtime clock reading immediately before native dispatch.
    pub ts_observed: UnixNanos,
}

/// One original native input and its observed order, without changing payload timestamps.
#[derive(Debug)]
pub struct ObservedDataEvent {
    pub key: ObservationKey,
    pub event: DataEvent,
}

type Observer = Rc<dyn Fn(ObservationKey, &DataEvent) -> anyhow::Result<()>>;

struct Recording {
    clock: Rc<RefCell<dyn Clock>>,
    observer: Observer,
    in_observer: bool,
    last: Option<ObservationKey>,
    error: Option<String>,
}

/// Owns the optional ingress recorder on the current runtime thread.
///
/// Keep the recorder alive through the final queue drain. Its callback writes the input envelope,
/// not derived strategy outputs. Those outputs retain [`current_data_observation`] for comparison.
/// This type is thread-affine and unregisters on drop.
pub struct DataObservationRecorder {
    recording: Rc<RefCell<Recording>>,
}

impl Debug for DataObservationRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let recording = self.recording.borrow();
        f.debug_struct(stringify!(DataObservationRecorder))
            .field("last", &recording.last)
            .field("error", &recording.error)
            .finish_non_exhaustive()
    }
}

impl DataObservationRecorder {
    /// Installs the recorder before the node starts consuming data.
    ///
    /// # Errors
    ///
    /// Returns an error if a recorder is already installed on this runtime thread.
    pub fn install(
        clock: Rc<RefCell<dyn Clock>>,
        observer: impl Fn(ObservationKey, &DataEvent) -> anyhow::Result<()> + 'static,
    ) -> anyhow::Result<Self> {
        let recording = Rc::new(RefCell::new(Recording {
            clock,
            in_observer: false,
            observer: Rc::new(observer),
            last: None,
            error: None,
        }));
        RECORDING.with(|slot| {
            let mut slot = slot.borrow_mut();
            anyhow::ensure!(
                slot.is_none(),
                "A data observation recorder is already installed"
            );
            *slot = Some(recording.clone());
            Ok(Self { recording })
        })
    }

    /// Returns the last input whose recording callback completed successfully.
    #[must_use]
    pub fn last_observation(&self) -> Option<ObservationKey> {
        self.recording.borrow().last
    }

    /// Checks the first recording failure without consuming or clearing it.
    ///
    /// # Errors
    ///
    /// Returns an error if recording, timestamp validation, or sequence validation has failed.
    pub fn check_error(&self) -> anyhow::Result<()> {
        if let Some(e) = &self.recording.borrow().error {
            anyhow::bail!("{e}");
        }
        Ok(())
    }
}

impl Drop for DataObservationRecorder {
    fn drop(&mut self) {
        let _ = RECORDING.try_with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot
                .as_ref()
                .is_some_and(|value| Rc::ptr_eq(value, &self.recording))
            {
                *slot = None;
            }
        });
    }
}

/// Preserves a data observation through native synchronous and deferred callback dispatch.
#[derive(Debug)]
pub struct DataObservationScope {
    previous: Option<ObservationKey>,
    marker: PhantomData<Rc<()>>,
}

impl DataObservationScope {
    /// Enters an already validated replay input's observation.
    ///
    /// # Errors
    ///
    /// Returns an error if the sequence is zero or a live recorder is installed.
    pub fn recorded(key: ObservationKey) -> anyhow::Result<Self> {
        anyhow::ensure!(key.sequence > 0, "Observation sequence must be positive");
        anyhow::ensure!(
            RECORDING.with(|slot| slot.borrow().is_none()),
            "Recorded replay cannot run with a live observation recorder"
        );
        Ok(Self::enter(Some(key)))
    }

    pub(crate) fn enter(key: Option<ObservationKey>) -> Self {
        let previous = CURRENT.with(|current| current.replace(key));
        Self {
            previous,
            marker: PhantomData,
        }
    }
}

impl Drop for DataObservationScope {
    fn drop(&mut self) {
        let _ = CURRENT.try_with(|current| current.replace(self.previous));
    }
}

/// Returns the original ingress identity of the current native callback.
#[must_use]
pub fn current_data_observation() -> Option<ObservationKey> {
    CURRENT.with(|current| *current.borrow())
}

/// Checks the installed recorder at a native runner boundary.
///
/// # Errors
///
/// Returns an error if the recorder has failed, without consuming its first failure.
pub fn check_data_observation_error() -> anyhow::Result<()> {
    RECORDING.with(|slot| {
        if let Some(recording) = slot.borrow().as_ref()
            && let Some(e) = recording.borrow().error.as_ref()
        {
            anyhow::bail!("{e}");
        }
        Ok(())
    })
}

/// Records an original live input before dispatching it to the `DataEngine`.
///
/// Returns no scope when observation recording is disabled. On failure the caller must not
/// dispatch the input, and the recorder keeps the first error for the run owner.
///
/// # Errors
///
/// Returns an error if recording failed, the clock regressed, or the sequence overflowed.
pub fn observe_data_event(event: &DataEvent) -> anyhow::Result<Option<DataObservationScope>> {
    let Some(recording) = RECORDING.with(|slot| slot.borrow().clone()) else {
        return Ok(None);
    };
    let ts_observed = recording.borrow().clock.borrow().timestamp_ns();
    observe_at(&recording, event, ts_observed).map(Some)
}

fn observe_at(
    recording: &Rc<RefCell<Recording>>,
    event: &DataEvent,
    ts_observed: UnixNanos,
) -> anyhow::Result<DataObservationScope> {
    let (key, observer) = {
        let mut recording = recording.borrow_mut();
        if recording.in_observer && recording.error.is_none() {
            recording.error =
                Some("Data observation recorder re-entered native ingress".to_string());
        }

        if let Some(e) = &recording.error {
            anyhow::bail!("{e}");
        }
        let sequence = recording.last.map_or(0, |key| key.sequence).checked_add(1);
        let key = sequence
            .filter(|_| {
                recording
                    .last
                    .is_none_or(|key| key.ts_observed <= ts_observed)
            })
            .map(|sequence| ObservationKey {
                sequence,
                ts_observed,
            });
        (key, recording.observer.clone())
    };

    let Some(key) = key else {
        let message = "Observation clock regressed or sequence exhausted".to_string();
        recording.borrow_mut().error = Some(message.clone());
        anyhow::bail!("{message}");
    };
    let scope = DataObservationScope::enter(Some(key));
    recording.borrow_mut().in_observer = true;
    let result = observer(key, event);
    recording.borrow_mut().in_observer = false;

    if let Err(e) = result {
        let message = format!("Failed to record observation {}: {e:#}", key.sequence);
        let message = recording.borrow_mut().error.get_or_insert(message).clone();
        anyhow::bail!("{message}");
    }

    if let Some(e) = &recording.borrow().error {
        anyhow::bail!("{e}");
    }
    recording.borrow_mut().last = Some(key);
    Ok(scope)
}

thread_local! {
    static RECORDING: RefCell<Option<Rc<RefCell<Recording>>>> = const { RefCell::new(None) };
    static CURRENT: RefCell<Option<ObservationKey>> = const { RefCell::new(None) };
}

#[cfg(test)]
mod tests {
    use nautilus_model::data::{Data, QuoteTick};
    use rstest::rstest;

    use super::*;

    fn clock() -> Rc<RefCell<dyn Clock>> {
        Rc::new(RefCell::new(crate::clock::VirtualClock::new()))
    }

    fn event() -> DataEvent {
        DataEvent::Data(Data::Quote(QuoteTick::default()))
    }

    #[rstest]
    fn records_same_timestamp_inputs_in_consumption_order() {
        let observed = Rc::new(RefCell::new(Vec::new()));
        let sink = observed.clone();
        let recorder = DataObservationRecorder::install(clock(), move |key, _| {
            sink.borrow_mut().push(key);
            assert_eq!(current_data_observation(), Some(key));
            Ok(())
        })
        .unwrap();
        for _ in 0..3 {
            let scope = observe_at(&recorder.recording, &event(), 11.into()).unwrap();
            assert!(current_data_observation().is_some());
            drop(scope);
        }
        assert_eq!(
            *observed.borrow(),
            (1..=3)
                .map(|sequence| ObservationKey {
                    sequence,
                    ts_observed: 11.into(),
                })
                .collect::<Vec<_>>()
        );
        assert_eq!(current_data_observation(), None);
        recorder.check_error().unwrap();
    }

    #[rstest]
    fn first_recording_failure_remains_and_blocks_later_input() {
        let calls = Rc::new(RefCell::new(0));
        let counter = calls.clone();
        let recorder = DataObservationRecorder::install(clock(), move |_, _| {
            *counter.borrow_mut() += 1;
            anyhow::bail!("disk failed")
        })
        .unwrap();
        let first = observe_at(&recorder.recording, &event(), 20.into())
            .unwrap_err()
            .to_string();
        let later = observe_at(&recorder.recording, &event(), 21.into())
            .unwrap_err()
            .to_string();
        assert_eq!(first, later);
        assert_eq!(recorder.check_error().unwrap_err().to_string(), first);
        assert_eq!(*calls.borrow(), 1);
        assert_eq!(recorder.last_observation(), None);
        assert_eq!(current_data_observation(), None);
    }

    #[rstest]
    fn clock_regression_does_not_create_an_observation() {
        let recorder = DataObservationRecorder::install(clock(), |_, _| Ok(())).unwrap();
        drop(observe_at(&recorder.recording, &event(), 20.into()).unwrap());
        assert!(observe_at(&recorder.recording, &event(), 19.into()).is_err());
        assert_eq!(
            recorder.last_observation(),
            Some(ObservationKey {
                sequence: 1,
                ts_observed: 20.into()
            })
        );
        assert!(recorder.check_error().is_err());
    }

    #[rstest]
    fn recursive_recording_is_rejected_even_when_the_observer_ignores_the_error() {
        let recorder = DataObservationRecorder::install(clock(), |_, _| {
            assert!(observe_data_event(&event()).is_err());
            Ok(())
        })
        .unwrap();
        let error = observe_data_event(&event()).unwrap_err().to_string();
        assert!(error.contains("re-entered"));
        assert_eq!(recorder.check_error().unwrap_err().to_string(), error);
        assert_eq!(recorder.last_observation(), None);
        assert_eq!(current_data_observation(), None);
    }

    #[rstest]
    fn recorder_ownership_and_recorded_scopes_are_exclusive() {
        let recorder = DataObservationRecorder::install(clock(), |_, _| Ok(())).unwrap();
        assert!(DataObservationRecorder::install(clock(), |_, _| Ok(())).is_err());
        let key = ObservationKey {
            sequence: 1,
            ts_observed: 3.into(),
        };
        assert!(DataObservationScope::recorded(key).is_err());
        drop(recorder);
        let scope = DataObservationScope::recorded(key).unwrap();
        assert_eq!(current_data_observation(), Some(key));
        drop(scope);
        assert_eq!(current_data_observation(), None);
    }
}
