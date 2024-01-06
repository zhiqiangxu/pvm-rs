#[derive(Copy, Clone, Default, PartialEq)]
pub struct Version {
    pub index: usize,
    pub incarnation: usize,
}

pub type ReadSet<L> = Vec<ReadDescriptor<L>>;
pub struct ReadDescriptor<L> {
    pub location: L,
    pub version: Option<Version>,
}

pub struct WriteDescriptor<L, V> {
    pub location: L,
    pub value: V,
}

pub type WriteSet<L, V> = Vec<WriteDescriptor<L, V>>;

pub trait Executor {
    fn run(&self);
}

pub struct LocationValue<L, V> {
    pub location: L,
    pub value: V,
}

pub trait MVMemory<L, V>: Sync {
    fn new(block_size: usize) -> Self;
    fn record(&self, version: Version, rs: ReadSet<L>, ws: WriteSet<L, V>) -> bool;
    fn read(&self, location: &L, txn_index: usize) -> ReadResult<V>;
    fn snapshot(&self) -> Vec<LocationValue<L, V>>;
    fn validate_readset(&self, txn_index: usize) -> bool;
    fn convert_writes_to_estimates(&self, txn_index: usize);
}

pub struct ReadResult<V> {
    pub status: ReadStatus,
    pub version: Version,
    pub value: Option<V>,
    // blocking transaction index
    pub blocking_index: usize,
}

impl<V> Default for ReadResult<V> {
    fn default() -> Self {
        ReadResult::<V> {
            value: None,
            status: Default::default(),
            version: Default::default(),
            blocking_index: Default::default(),
        }
    }
}

#[derive(Default, PartialEq)]
pub enum ReadStatus {
    #[default]
    OK,
    NotFound,
    Error,
}

pub trait VM<L, V>: Sync {
    fn execute(&self, txn_index: usize) -> VMResult<L, V>;
}

#[derive(Default)]
pub struct VMResult<L, V> {
    pub read_set: ReadSet<L>,
    pub write_set: WriteSet<L, V>,
    pub status: VMStatus,
    // blocking transaction index
    pub blocking_index: usize,
}

#[derive(Default)]
pub enum VMStatus {
    #[default]
    OK,
    ReadError,
}

pub enum TaskKind {
    Execution,
    Validation,
}

pub struct Task {
    pub kind: TaskKind,
    pub version: Version,
}

pub trait Scheduler: Sync {
    fn new(block_size: usize) -> Self;
    fn done(&self) -> bool;
    fn next_task(&self) -> Option<Task>;
    fn add_dependency(&self, index: usize, blocking_index: usize) -> bool;
    fn finish_execution(&self, version: Version, wrote_new_location: bool) -> Option<Task>;
    fn finish_validation(&self, txn_index: usize, aborted: bool) -> Option<Task>;
    fn try_validation_abort(&self, version: Version) -> bool;
}
