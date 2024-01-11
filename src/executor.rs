use crate::types::{self, Version};
use rayon::prelude::*;
use rich_phantoms::PhantomCovariantAlwaysSendSync;

pub struct Executor<L, V, VM, Scheduler, MVMemory>
where
    VM: types::VM<L, V>,
    Scheduler: types::Scheduler,
    MVMemory: types::MVMemory<L, V>,
{
    concurrency: usize,
    vm: VM,
    scheduler: Scheduler,
    mvmemory: MVMemory,
    _l: PhantomCovariantAlwaysSendSync<L>,
    _v: PhantomCovariantAlwaysSendSync<V>,
}

impl<L, V, VM, Scheduler, MVMemory> types::Executor for Executor<L, V, VM, Scheduler, MVMemory>
where
    VM: types::VM<L, V>,
    Scheduler: types::Scheduler,
    MVMemory: types::MVMemory<L, V>,
{
    fn run(&self) {
        (0..self.concurrency)
            .into_par_iter()
            .for_each(|_| self.run_inner())
    }
}

impl<L, V, VM, Scheduler, MVMemory> Executor<L, V, VM, Scheduler, MVMemory>
where
    VM: types::VM<L, V>,
    Scheduler: types::Scheduler,
    MVMemory: types::MVMemory<L, V>,
{
    pub fn new(
        concurrency: usize,
        vm: VM,
        block_size: usize,
    ) -> Executor<L, V, VM, Scheduler, MVMemory> {
        Executor::<L, V, VM, Scheduler, MVMemory> {
            concurrency,
            vm,
            scheduler: Scheduler::new(block_size),
            mvmemory: MVMemory::new(block_size),
            _l: Default::default(),
            _v: Default::default(),
        }
    }

    pub fn new_with_deps(
        concurrency: usize,
        vm: VM,
        block_size: usize,
        all_deps: Vec<Vec<usize>>,
    ) -> Executor<L, V, VM, Scheduler, MVMemory> {
        let scheduler = Scheduler::new(block_size);
        for (index, deps) in all_deps.iter().enumerate() {
            for dep_index in deps {
                scheduler.add_dependency(index, *dep_index);
            }
        }

        Executor::<L, V, VM, Scheduler, MVMemory> {
            concurrency,
            vm,
            scheduler,
            mvmemory: MVMemory::new(block_size),
            _l: Default::default(),
            _v: Default::default(),
        }
    }

    fn run_inner(&self) {
        let mut task: Option<types::Task> = None;
        loop {
            if let Some(t) = task {
                match t.kind {
                    types::TaskKind::Execution => task = self.try_execute(t.version),
                    types::TaskKind::Validation => task = self.try_validate(t.version),
                }
            }
            if task.is_none() {
                task = self.scheduler.next_task();
            }

            if task.is_none() && self.scheduler.done() {
                break;
            }
        }
    }

    fn try_execute(&self, version: Version) -> Option<types::Task> {
        let mut task: Option<types::Task> = None;
        let result = self.vm.execute(version.index);
        match result.status {
            types::VMStatus::ReadError => {
                if !self
                    .scheduler
                    .add_dependency(version.index, result.blocking_index)
                {
                    task = self.try_execute(version);
                }
            }
            types::VMStatus::OK => {
                let wrote_new_location =
                    self.mvmemory
                        .record(version, result.read_set, result.write_set);
                task = self.scheduler.finish_execution(version, wrote_new_location);
            }
        }
        task
    }

    fn try_validate(&self, version: Version) -> Option<types::Task> {
        let readset_valid = self.mvmemory.validate_readset(version.index);
        let aborted = !readset_valid && self.scheduler.try_validation_abort(version);
        if aborted {
            self.mvmemory.convert_writes_to_estimates(version.index);
        }

        self.scheduler.finish_validation(version.index, aborted)
    }
}
