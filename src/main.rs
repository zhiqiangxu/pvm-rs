use pvm_rs::types::{self, Executor as ExecutorTrait};
use pvm_rs::{executor::Executor, mvmemory::MVMemory, scheduler::Scheduler};
use std::time::Instant;

struct VM {}

impl<L, V> types::VM<L, V> for VM
where
    L: Default,
    V: Default,
{
    fn execute(&self, _: usize) -> types::VMResult<L, V> {
        types::VMResult::<L, V>::default()
    }
}

fn main() {
    let start = Instant::now();

    Executor::<i32, i32, VM, Scheduler, MVMemory<i32, i32>>::new(5, VM {}, 100).run();
    println!("execution took {:?}", start.elapsed());
}
