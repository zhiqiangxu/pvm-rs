use crate::types::{self, ReadStatus};

use std::collections::{BTreeMap, HashMap, HashSet};

use std::sync::atomic::AtomicPtr;
use std::sync::{Arc, RwLock};

use std::collections::btree_map::Entry::{Occupied as BOccupied, Vacant as BVacant};
use std::collections::hash_map::Entry::{Occupied, Vacant};

pub struct MVMemory<L, V> {
    data: RwLock<HashMap<L, Arc<DataCells<V>>>>,
    last_writeset: Vec<AtomicPtr<Vec<L>>>,
    last_readset: Vec<AtomicPtr<types::ReadSet<L>>>,
}

type DataCells<V> = RwLock<BTreeMap<usize, DataCell<V>>>;

struct DataCell<V> {
    flag: Flag,
    incarnation: usize,
    value: V,
}

enum Flag {
    Done,
    Estimate,
}

unsafe impl<L, V> Sync for MVMemory<L, V> {}

impl<L, V> Drop for MVMemory<L, V> {
    fn drop(&mut self) {
        for atomic_ptr in &self.last_readset {
            let raw_ptr = atomic_ptr.load(std::sync::atomic::Ordering::Acquire);
            if !raw_ptr.is_null() {
                unsafe { drop(Box::from_raw(raw_ptr)) };
            }
        }
        for atomic_ptr in &self.last_writeset {
            let raw_ptr = atomic_ptr.load(std::sync::atomic::Ordering::Acquire);
            if !raw_ptr.is_null() {
                unsafe { drop(Box::from_raw(raw_ptr)) };
            }
        }
    }
}

impl<L, V> types::MVMemory<L, V> for MVMemory<L, V>
where
    L: Eq + std::hash::Hash + Copy + std::fmt::Debug,
    V: Clone,
{
    fn new(block_size: usize) -> Self {
        MVMemory::<L, V> {
            data: Default::default(),
            last_writeset: (0..block_size)
                .into_iter()
                .map(|_| AtomicPtr::default())
                .collect(),
            last_readset: (0..block_size)
                .into_iter()
                .map(|_| AtomicPtr::default())
                .collect(),
        }
    }
    fn record(
        &self,
        version: types::Version,
        rs: types::ReadSet<L>,
        ws: types::WriteSet<L, V>,
    ) -> bool {
        let old = self.last_readset[version.index].swap(
            Box::into_raw(Box::new(rs)),
            std::sync::atomic::Ordering::Release,
        );

        if !old.is_null() {
            unsafe {
                drop(Box::from_raw(old));
            }
        }

        self.apply_write_set(version, ws)
    }
    fn read(&self, location: &L, txn_index: usize) -> types::ReadResult<V> {
        let mut result: types::ReadResult<V> = Default::default();
        let cells = self.get_location_cells(location, || None);

        if cells.is_none() {
            result.status = types::ReadStatus::NotFound;
            return result;
        }

        let cells = cells.expect("must not be none");
        let cells = cells.read().unwrap();
        let cell = cells.range(..txn_index).next_back();

        match cell {
            Some(cell) => match cell.1.flag {
                Flag::Estimate => {
                    result.status = types::ReadStatus::Error;
                    result.blocking_index = *cell.0;
                }
                Flag::Done => {
                    result.status = types::ReadStatus::OK;
                    result.version = types::Version {
                        index: *cell.0,
                        incarnation: cell.1.incarnation,
                    };
                    result.value = Some(cell.1.value.clone())
                }
            },
            None => {
                result.status = types::ReadStatus::NotFound;
            }
        }

        result
    }
    fn snapshot(&self) -> Vec<types::LocationValue<L, V>> {
        let mut ret = Vec::with_capacity(self.data.read().unwrap().len());
        for (location, _) in &*self.data.read().unwrap() {
            let result = self.read(location, self.last_readset.len());
            if result.status == ReadStatus::OK {
                ret.push(types::LocationValue::<L, V> {
                    location: *location,
                    value: result.value.expect("must be some since status is ok"),
                })
            }
        }
        ret
    }
    fn validate_readset(&self, txn_index: usize) -> bool {
        let prev_reads = self.last_readset[txn_index].load(std::sync::atomic::Ordering::Acquire);
        if !prev_reads.is_null() {
            let prev_reads = unsafe { &*prev_reads };
            for read in prev_reads {
                let cur_read = self.read(&read.location, txn_index);

                if cur_read.status == ReadStatus::Error {
                    return false;
                }

                if cur_read.status == ReadStatus::NotFound && read.version.is_some() {
                    return false;
                }

                if cur_read.status == ReadStatus::OK
                    && (read.version.is_none() || cur_read.version != read.version.unwrap())
                {
                    return false;
                }
            }
        }

        true
    }
    fn convert_writes_to_estimates(&self, txn_index: usize) {
        let prev_writes = self.last_writeset[txn_index].load(std::sync::atomic::Ordering::Acquire);
        if !prev_writes.is_null() {
            let prev_writes = unsafe { &*prev_writes };
            for location in prev_writes {
                let cells = self.get_location_cells(location, || None);
                let cells = cells.expect("cells must be some since location has been writen");

                let mut guard = cells.write().unwrap();
                match guard.entry(txn_index) {
                    BOccupied(o) => o.into_mut().flag = Flag::Estimate,
                    _ => {
                        unreachable!("must be occupied since location has been writen");
                    }
                }
            }
        }
    }
}

impl<L, V> MVMemory<L, V>
where
    L: Eq + std::hash::Hash + Copy + std::fmt::Debug,
{
    fn get_location_cells(
        &self,
        location: &L,
        f_gen: impl Fn() -> Option<Arc<DataCells<V>>>,
    ) -> Option<Arc<DataCells<V>>> {
        let data_cells = (*self.data.read().unwrap()).get(location).cloned();
        if data_cells.is_none() {
            f_gen()
        } else {
            data_cells
        }
    }

    fn apply_write_set(&self, version: types::Version, ws: types::WriteSet<L, V>) -> bool {
        let mut new_locations_set = HashSet::with_capacity(ws.len());
        for w in ws {
            self.write_data(w.location, version, w.value);
            new_locations_set.insert(w.location);
        }

        let prev_locations =
            self.last_writeset[version.index].load(std::sync::atomic::Ordering::Acquire);
        let wrote_new_location = if !prev_locations.is_null() {
            let prev_locations_list = unsafe { Box::from_raw(prev_locations) };
            let mut prev_locations_set = HashSet::with_capacity(prev_locations_list.len());
            for location in *prev_locations_list {
                prev_locations_set.insert(location);
            }

            let mut wrote_new_location = false;
            for location in &new_locations_set {
                if !prev_locations_set.contains(location) {
                    wrote_new_location = true;
                    break;
                }
            }

            for location in prev_locations_set {
                if !new_locations_set.contains(&location) {
                    self.remove_data(location, version.index);
                }
            }

            wrote_new_location
        } else {
            !new_locations_set.is_empty()
        };

        let mut new_location_list = Vec::with_capacity(new_locations_set.len());
        for location in new_locations_set {
            new_location_list.push(location);
        }

        self.last_writeset[version.index].store(
            Box::into_raw(Box::new(new_location_list)),
            std::sync::atomic::Ordering::Relaxed,
        );

        wrote_new_location
    }

    fn write_data(&self, location: L, version: types::Version, value: V) {
        let cells = self.get_location_cells(&location, || {
            let mut guard = self.data.write().unwrap();
            let c = match guard.entry(location) {
                Vacant(vacant) => vacant.insert(Arc::new(DataCells::default())),
                Occupied(o) => o.into_mut(),
            };

            Some(c.clone())
        });

        let cells = cells.expect("must be some");
        let mut guard = cells.write().unwrap();

        match (*guard).entry(version.index) {
            BOccupied(o) => {
                let o = o.into_mut();
                assert!(
                    o.incarnation <= version.incarnation,
                    "existing transaction value does not have lower incarnation: {:?}, {:?}",
                    location,
                    version.index
                );
                o.flag = Flag::Done;
                o.incarnation = version.incarnation;
                o.value = value;
            }
            BVacant(vacant) => {
                vacant.insert(DataCell {
                    flag: Flag::Done,
                    value,
                    incarnation: version.incarnation,
                });
            }
        }
    }

    fn remove_data(&self, location: L, txn_index: usize) {
        let cells = self.get_location_cells(&location, || None);

        if cells.is_none() {
            return;
        }

        let cells = cells.expect("must be some");
        cells.write().unwrap().remove(&txn_index);
    }
}
