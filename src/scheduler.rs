use std::collections::HashSet;

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize};
use std::sync::{RwLock, RwLockWriteGuard};

use crate::types;

pub struct Scheduler {
    done_marker: AtomicBool,
    validation_index: AtomicUsize,
    execution_index: AtomicUsize,
    num_active_tasks: AtomicI32,
    decrease_count: AtomicI32,
    all_txn_state: Vec<TxnState>,
    block_size: usize,
}

#[derive(Default)]
struct TxnState(RwLock<TxnStateInner>);

#[derive(Default)]
struct TxnStateInner {
    status: TxnStatus,
    incarnation: usize,
    blockers: Option<HashSet<usize>>, // blocking transactions for current transactoin
    dependencies: Option<HashSet<usize>>, // blocked transactions  on current transaction
}

#[derive(Default, PartialEq, Clone, Copy)]
enum TxnStatus {
    #[default]
    ReadyToExecute,
    Executing,
    Executed,
    Aborting,
}

impl types::Scheduler for Scheduler {
    fn new(block_size: usize) -> Scheduler {
        let all_txn_state = (0..block_size)
            .into_iter()
            .map(|_| TxnState::default())
            .collect();
        Scheduler {
            done_marker: Default::default(),
            validation_index: Default::default(),
            execution_index: Default::default(),
            num_active_tasks: Default::default(),
            decrease_count: Default::default(),
            all_txn_state: all_txn_state,
            block_size: block_size,
        }
    }
    fn done(&self) -> bool {
        return self.done_marker.load(std::sync::atomic::Ordering::Acquire);
    }
    fn next_task(&self) -> Option<types::Task> {
        if self
            .validation_index
            .load(std::sync::atomic::Ordering::Acquire)
            < self
                .execution_index
                .load(std::sync::atomic::Ordering::Acquire)
        {
            let version_to_validate = self.next_version_to_validate();
            if version_to_validate.is_some() {
                return Some(types::Task {
                    kind: types::TaskKind::Validation,
                    version: version_to_validate.unwrap(),
                });
            }
        } else {
            let version_to_execute = self.next_version_to_execute();
            if version_to_execute.is_some() {
                return Some(types::Task {
                    kind: types::TaskKind::Execution,
                    version: version_to_execute.unwrap(),
                });
            }
        }
        return None;
    }
    fn add_dependency(&self, index: usize, blocking_index: usize) -> bool {
        let blocking_txn_state = &self.all_txn_state[blocking_index];
        let blocked_txn_state = &self.all_txn_state[index];

        let mut blocking_guard = blocking_txn_state.0.write().unwrap();
        if blocking_guard.status == TxnStatus::Executed {
            return false;
        }

        let mut blocked_guard = blocked_txn_state.0.write().unwrap();
        blocked_guard.status = TxnStatus::Aborting;

        if blocking_guard.dependencies.is_none() {
            blocking_guard.dependencies = Some(HashSet::<_>::default());
        }
        if blocked_guard.blockers.is_none() {
            blocked_guard.blockers = Some(HashSet::<_>::default());
        }
        blocking_guard
            .dependencies
            .as_mut()
            .expect("must have been some")
            .insert(index);
        blocked_guard
            .blockers
            .as_mut()
            .expect("must have been some")
            .insert(blocking_index);

        drop(blocking_guard);
        drop(blocked_guard);

        self.num_active_tasks
            .fetch_add(-1, std::sync::atomic::Ordering::AcqRel);

        return true;
    }
    fn finish_execution(
        &self,
        version: types::Version,
        wrote_new_location: bool,
    ) -> Option<types::Task> {
        let txn_state = &self.all_txn_state[version.index];
        let mut guard = txn_state.0.write().unwrap();
        if guard.status != TxnStatus::Executing {
            panic!("status must have been EXECUTING")
        }
        guard.status = TxnStatus::Executed;
        let dependencies = guard.dependencies.take();
        self.resume_dependencies(version.index, dependencies);
        drop(guard);

        if self
            .validation_index
            .load(std::sync::atomic::Ordering::Acquire)
            > version.index
        {
            // otherwise index already small enough
            if wrote_new_location {
                // schedule validation for txn_idx and higher txns
                self.decrease_validation_index(version.index);
            } else {
                return Some(types::Task {
                    kind: types::TaskKind::Validation,
                    version: version,
                });
            }
        }

        // done with execution task
        self.num_active_tasks
            .fetch_add(-1, std::sync::atomic::Ordering::AcqRel);
        // no task returned to the caller
        return None;
    }
    fn finish_validation(&self, txn_index: usize, aborted: bool) -> Option<types::Task> {
        if aborted {
            self.set_ready_status(txn_index);
            // schedule validation for higher transactions
            self.decrease_validation_index(txn_index + 1);

            if self
                .execution_index
                .load(std::sync::atomic::Ordering::Acquire)
                > txn_index
            {
                let new_version = self.try_incarnation(txn_index);
                if let Some(version) = new_version.as_ref() {
                    // return re-execution task to the caller
                    return Some(types::Task {
                        kind: types::TaskKind::Execution,
                        version: *version,
                    });
                }
            }
        }

        // done with validation task
        self.num_active_tasks
            .fetch_add(-1, std::sync::atomic::Ordering::AcqRel);
        // no task returned to the caller
        return None;
    }
    fn try_validation_abort(&self, version: types::Version) -> bool {
        let txn_state = &self.all_txn_state[version.index];
        let mut guard = txn_state.0.write().unwrap();
        if guard.incarnation == version.incarnation && guard.status == TxnStatus::Executed {
            guard.status = TxnStatus::Aborting;
            return true;
        }

        return false;
    }
}

impl Scheduler {
    fn resume_dependencies(&self, blocking_index: usize, dependencies: Option<HashSet<usize>>) {
        if dependencies.is_none() {
            return;
        }

        let mut min_dep_txn_index = usize::MAX;
        for dep_txn_index in dependencies.expect("must be some") {
            let txn_state = &self.all_txn_state[dep_txn_index];
            let mut guard = txn_state.0.write().unwrap();
            guard.blockers.as_mut().unwrap().remove(&blocking_index);
            let can_resume = guard.blockers.as_mut().unwrap().is_empty();

            if can_resume {
                self.set_ready_status_locked(&mut guard);
                drop(guard);
                if min_dep_txn_index == usize::MAX || dep_txn_index < min_dep_txn_index {
                    min_dep_txn_index = dep_txn_index;
                }
            } else {
                drop(guard);
            }
        }

        if min_dep_txn_index != usize::MAX {
            // ensure dependent indices get re-executed
            self.decrease_execution_index(min_dep_txn_index);
        }
    }

    fn set_ready_status(&self, txn_index: usize) {
        let txn_state = &self.all_txn_state[txn_index];
        let mut guard = txn_state.0.write().unwrap();
        self.set_ready_status_locked(&mut guard);
    }
    fn set_ready_status_locked(&self, guard: &mut RwLockWriteGuard<'_, TxnStateInner>) {
        guard.incarnation += 1;
        guard.status = TxnStatus::ReadyToExecute;
    }

    fn next_version_to_validate(&self) -> Option<types::Version> {
        if self
            .validation_index
            .load(std::sync::atomic::Ordering::Acquire)
            >= self.block_size
        {
            self.check_done();
            return None;
        }

        self.num_active_tasks
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let validation_index = self
            .validation_index
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        if validation_index < self.block_size {
            let txn_state = &self.all_txn_state[validation_index];
            let guard = txn_state.0.read().unwrap();
            let (status, incarnation) = (guard.status, guard.incarnation);
            drop(guard);
            if status == TxnStatus::Executed {
                return Some(types::Version {
                    index: validation_index,
                    incarnation: incarnation,
                });
            }
        }

        self.num_active_tasks
            .fetch_add(-1, std::sync::atomic::Ordering::AcqRel);
        return None;
    }

    fn next_version_to_execute(&self) -> Option<types::Version> {
        if self
            .execution_index
            .load(std::sync::atomic::Ordering::Acquire)
            >= self.block_size
        {
            self.check_done();
            return None;
        }

        self.num_active_tasks
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let execution_index = self
            .execution_index
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        self.try_incarnation(execution_index)
    }

    fn try_incarnation(&self, txn_index: usize) -> Option<types::Version> {
        if txn_index < self.block_size {
            let txn_state = &self.all_txn_state[txn_index];
            let mut guard = txn_state.0.write().unwrap();
            if guard.status == TxnStatus::ReadyToExecute {
                guard.status = TxnStatus::Executing;
                return Some(types::Version {
                    index: txn_index,
                    incarnation: guard.incarnation,
                });
            }
        }

        self.num_active_tasks
            .fetch_add(-1, std::sync::atomic::Ordering::AcqRel);
        return None;
    }

    fn check_done(&self) {
        let observed_count = self
            .decrease_count
            .load(std::sync::atomic::Ordering::Acquire);
        if self
            .execution_index
            .load(std::sync::atomic::Ordering::Acquire)
            >= self.block_size
            && self
                .validation_index
                .load(std::sync::atomic::Ordering::Acquire)
                >= self.block_size
            && self
                .num_active_tasks
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
            && observed_count
                == self
                    .decrease_count
                    .load(std::sync::atomic::Ordering::Acquire)
        {
            self.done_marker
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    fn decrease_execution_index(&self, txn_index: usize) {
        loop {
            let execution_index = self
                .execution_index
                .load(std::sync::atomic::Ordering::Acquire);
            if execution_index > txn_index {
                if self
                    .execution_index
                    .compare_exchange(
                        execution_index,
                        txn_index,
                        std::sync::atomic::Ordering::AcqRel,
                        std::sync::atomic::Ordering::Acquire,
                    )
                    .is_err()
                {
                    continue;
                }
            }

            break;
        }

        self.decrease_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
    fn decrease_validation_index(&self, txn_index: usize) {
        loop {
            let validation_index = self
                .validation_index
                .load(std::sync::atomic::Ordering::Acquire);
            if validation_index > txn_index {
                if self
                    .execution_index
                    .compare_exchange(
                        validation_index,
                        txn_index,
                        std::sync::atomic::Ordering::AcqRel,
                        std::sync::atomic::Ordering::Acquire,
                    )
                    .is_err()
                {
                    continue;
                }
            }

            break;
        }

        self.decrease_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
}
