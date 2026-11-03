//! A database: its control file, log, page store, buffer pool and checkpointer, created new or
//! recovered from what a crash left.

use super::buffer::{BufferConfig, BufferPool, LocalSource};
use super::checkpoint::Checkpointer;
use super::control::{Control, Superblock};
use super::log::{framed_len, table_len, Log, LogConfig, Lsn, Writers};
use super::page::CLASS_COUNT;
use super::recovery::Recovery;
use super::store::{PageStore, StoreConfig};
use super::txn::{Clock, Transactions, TxnConfig};
use crate::io::IO;
use crate::sync::Arc;
use crate::{LimboError, Result};

#[derive(Debug, Clone)]
pub struct AtollConfig {
    pub log: LogConfig,
    pub store: StoreConfig,
    pub buffer: BufferConfig,
    /// Bytes of log a recovery may have to replay. `None` for 0.4 times the pool's budget, the
    /// ratio of Haubenschild et al.'s example (SIGMOD 2020, §3.4: a 20 GB log for a 50 GB pool):
    /// the checkpoint work per byte of log then stays the same whatever the pool's size.
    pub log_budget: Option<u64>,
    pub txn: TxnConfig,
}

impl AtollConfig {
    /// The default configuration around a pool of `budget` bytes.
    pub fn with_budget(budget: usize) -> Self {
        Self {
            log: LogConfig::default(),
            store: StoreConfig::default(),
            buffer: BufferConfig::with_budget(budget),
            log_budget: None,
            txn: TxnConfig::default(),
        }
    }

    pub(super) fn log_budget(&self) -> u64 {
        self.log_budget
            .unwrap_or(self.buffer.budget as u64 * 2 / 5)
            .max(1)
    }
}

pub struct Atoll {
    database_id: [u8; 16],
    recovered_end: Option<Lsn>,
    log: Arc<Log>,
    store: Arc<PageStore>,
    pool: Arc<BufferPool>,
    checkpointer: Checkpointer,
    txns: Transactions,
}

impl Atoll {
    /// Opens the database at `base`, creating it if it does not exist and recovering it on this
    /// thread if it does. To recover on several threads, use [`Opening`].
    pub fn open(io: Arc<dyn IO>, base: &str, config: AtollConfig) -> Result<Self> {
        let opening = Opening::new(io, base, config)?;
        opening.work()?;
        opening.finish()
    }

    pub fn database_id(&self) -> [u8; 16] {
        self.database_id
    }

    /// Where recovery found the log's valid records end, if this open recovered: the state it
    /// recovered is the log's up to there.
    pub fn recovered_end(&self) -> Option<Lsn> {
        self.recovered_end
    }

    pub fn log(&self) -> &Arc<Log> {
        &self.log
    }

    pub fn store(&self) -> &Arc<PageStore> {
        &self.store
    }

    pub fn pool(&self) -> &Arc<BufferPool> {
        &self.pool
    }

    /// Continuous checkpoints, stepped like any background work ([`Checkpointer::step`]).
    pub fn transactions(&self) -> &Transactions {
        &self.txns
    }

    pub fn checkpointer(&self) -> &Checkpointer {
        &self.checkpointer
    }
}

/// Opening a database, which recovery may make long. Any number of threads may call
/// [`work`](Self::work) at once; once every call has returned, [`finish`](Self::finish) gives the
/// database.
pub struct Opening {
    store: Arc<PageStore>,
    superblock: Superblock,
    txn: TxnConfig,
    /// The writer registry the transactions enter and checkpoints read.
    writers: Arc<Writers>,
    how: How,
}

/// A new database, ready to open.
struct Created {
    log: Arc<Log>,
    pool: Arc<BufferPool>,
    checkpointer: Checkpointer,
    clock: Arc<Clock>,
}

enum How {
    Created(Box<Created>),
    Recovering(Box<Recovery>),
}

impl Opening {
    pub fn new(io: Arc<dyn IO>, base: &str, mut config: AtollConfig) -> Result<Self> {
        // A checkpoint's writer table lists up to one writer per slot, in one record.
        let table = framed_len(table_len(config.txn.slots));
        if table > config.log.buffer_bytes {
            return Err(LimboError::InternalError(format!(
                "{} transaction slots need a writer table of up to {table} bytes of log, more \
                 than the {} byte log buffer",
                config.txn.slots, config.log.buffer_bytes
            )));
        }
        let (control, superblock) = Control::open(io.clone(), base)?;
        let Some(superblock) = superblock else {
            return Self::create(io, base, config, control);
        };
        // What the database was created with holds, whatever this configuration says.
        config.log.segment_bytes = superblock.segment_bytes;
        config.store.file_bytes = superblock.file_bytes;
        let store =
            PageStore::recovering(io.clone(), base, config.store.clone(), superblock.heights);
        let writers = Arc::new(Writers::new(config.txn.slots));
        let recovery = Recovery::new(
            io.clone(),
            base,
            &config,
            store.clone(),
            control,
            superblock.clone(),
            writers.clone(),
        );
        Ok(Self {
            store,
            superblock,
            writers,
            txn: config.txn,
            how: How::Recovering(Box::new(recovery)),
        })
    }

    fn create(io: Arc<dyn IO>, base: &str, config: AtollConfig, control: Control) -> Result<Self> {
        // From the IO, so a simulation that seeds it creates the same database every time.
        let mut database_id = [0u8; 16];
        for half in database_id.chunks_exact_mut(8) {
            half.copy_from_slice(&io.generate_random_number().to_le_bytes());
        }
        let superblock = Superblock {
            seq: 1,
            redo: 0,
            segment_bytes: config.log.segment_bytes,
            file_bytes: config.store.file_bytes,
            database_id,
            heights: [[1; CLASS_COUNT]; 2],
            clock: 0,
            writers: None,
            continuations: Vec::new(),
        };
        let log = Log::open(io.clone(), base, config.log.clone(), 0)?;
        let store = PageStore::new(
            io.clone(),
            base,
            config.store.clone(),
            log.clone(),
            superblock.heights,
        );
        // Before anything is appended: a log with no superblock is not a database yet, and is
        // created again from nothing.
        control.write_and_wait(&superblock)?;
        let pool = BufferPool::new(
            config.buffer.clone(),
            Arc::new(LocalSource::new(store.clone())),
            store.clone(),
            0,
        )?;
        let clock = Arc::new(Clock::after(0));
        let writers = Arc::new(Writers::new(config.txn.slots));
        let checkpointer = Checkpointer::new(
            pool.clone(),
            store.clone(),
            control,
            superblock.clone(),
            config.log_budget(),
            clock.clone(),
            writers.clone(),
        );
        Ok(Self {
            store,
            superblock,
            writers,
            txn: config.txn,
            how: How::Created(Box::new(Created {
                log,
                pool,
                checkpointer,
                clock,
            })),
        })
    }

    /// Recovers alongside every other thread calling this; returns once recovery is done.
    pub fn work(&self) -> Result<()> {
        match &self.how {
            How::Created(..) => Ok(()),
            How::Recovering(r) => r.work(),
        }
    }

    pub fn finish(self) -> Result<Atoll> {
        let (recovered_end, log, pool, checkpointer, clock) = match self.how {
            How::Created(created) => {
                let Created {
                    log,
                    pool,
                    checkpointer,
                    clock,
                } = *created;
                (None, log, pool, checkpointer, clock)
            }
            How::Recovering(r) => {
                let done = r.take_recovered().ok_or_else(|| {
                    LimboError::InternalError("finishing an open before recovery is done".into())
                })?;
                (
                    Some(done.end),
                    done.log,
                    done.pool,
                    done.checkpointer,
                    done.clock,
                )
            }
        };
        Ok(Atoll {
            database_id: self.superblock.database_id,
            recovered_end,
            txns: Transactions::new(&self.txn, clock, log.clone(), self.writers),
            log,
            store: self.store,
            pool,
            checkpointer,
        })
    }
}
