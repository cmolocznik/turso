//! A database: its control file, log, page store, buffer pool and checkpointer, created new or
//! recovered from what a crash left.

use super::buffer::{BufferConfig, BufferPool, LocalSource};
use super::checkpoint::Checkpointer;
use super::control::{Control, Superblock};
use super::log::{Log, LogConfig, Lsn};
use super::meta::TREES;
use super::page::CLASS_COUNT;
use super::recovery::{self, Recovered};
use super::store::{PageStore, StoreConfig};
use super::txn::{Clock, Transactions, TxnConfig};
use crate::io::IO;
use crate::sync::{Arc, Mutex};
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

    /// A small database's configuration, for suites and tools that open many: the sizes every
    /// test of Atoll's SQL layer runs on, 1 MiB log segments, the smallest pool Atoll takes and a
    /// 512 KiB log budget, with the default transaction slots, so connections the suites hold at
    /// once never wait for one.
    pub fn small() -> Self {
        Self {
            log: LogConfig {
                segment_bytes: 1 << 20,
                buffer_bytes: 256 << 10,
                chunk_bytes: 16 << 10,
                writes_in_flight: 3,
                ..LogConfig::default()
            },
            store: StoreConfig {
                file_bytes: 1 << 30,
                meta_pages: 1 << 10,
            },
            buffer: BufferConfig::with_budget(super::buffer::MIN_BUDGET),
            log_budget: Some(512 << 10),
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

    pub fn transactions(&self) -> &Transactions {
        &self.txns
    }

    pub fn checkpointer(&self) -> &Checkpointer {
        &self.checkpointer
    }
}

/// Opening a database, which recovery may make long. Any number of threads may call
/// [`work`](Self::work) at once: one recovers while the rest wait for it; once every call has
/// returned, [`finish`](Self::finish) gives the database.
pub struct Opening {
    store: Arc<PageStore>,
    superblock: Superblock,
    how: Mutex<How>,
}

enum How {
    /// A new database, ready to open.
    Created(Box<Recovered>),
    /// A database to recover from its superblock, and what recovery needs.
    Recovering {
        io: Arc<dyn IO>,
        base: String,
        config: Box<AtollConfig>,
        control: Option<Control>,
    },
    Recovered(Box<Recovered>),
    Taken,
}

impl Opening {
    pub fn new(io: Arc<dyn IO>, base: &str, mut config: AtollConfig) -> Result<Self> {
        let (control, superblock) = Control::open(io.clone(), base)?;
        let Some(superblock) = superblock else {
            return Self::create(io, base, config, control);
        };
        // What the database was created with holds, whatever this configuration says.
        config.log.segment_bytes = superblock.segment_bytes;
        config.store.file_bytes = superblock.file_bytes;
        let store = PageStore::open(
            io.clone(),
            base,
            config.store.clone(),
            superblock.heights,
            superblock.checkpoint,
        );
        Ok(Self {
            store,
            superblock,
            how: Mutex::new(How::Recovering {
                io,
                base: base.to_string(),
                config: Box::new(config),
                control: Some(control),
            }),
        })
    }

    /// A database made from nothing: its first superblock names checkpoint 0, no page and no log,
    /// written before anything is appended, so a log with no superblock is no database yet.
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
            heights: [[1; CLASS_COUNT]; TREES],
            clock: 0,
            snapshot: 0,
            checkpoint: 0,
            continuations: Vec::new(),
        };
        let log = Log::open(io.clone(), base, config.log.clone(), 0)?;
        let store = PageStore::open(
            io.clone(),
            base,
            config.store.clone(),
            superblock.heights,
            0,
        );
        control.write_and_wait(&superblock)?;
        let pool = BufferPool::new(
            config.buffer.clone(),
            Arc::new(LocalSource::new(store.clone())),
            store.clone(),
        )?;
        let txns = Transactions::new(&config.txn, Arc::new(Clock::after(0)), log.clone());
        let checkpointer = Checkpointer::new(
            pool.clone(),
            store.clone(),
            log.clone(),
            control,
            superblock.clone(),
            config.log_budget(),
        );
        Ok(Self {
            store,
            superblock,
            how: Mutex::new(How::Created(Box::new(Recovered {
                log,
                pool,
                checkpointer,
                txns,
                end: 0,
            }))),
        })
    }

    /// Recovers, if this open recovers and no one has yet; returns once recovery is done.
    pub fn work(&self) -> Result<()> {
        let mut how = self.how.lock();
        let How::Recovering {
            io,
            base,
            config,
            control,
        } = &mut *how
        else {
            return Ok(());
        };
        let control = control.take().ok_or_else(|| {
            LimboError::InternalError("a recovery that failed is not tried again".into())
        })?;
        let recovered = recovery::recover(
            io.clone(),
            base,
            config,
            self.store.clone(),
            control,
            self.superblock.clone(),
        )?;
        *how = How::Recovered(Box::new(recovered));
        Ok(())
    }

    pub fn finish(self) -> Result<Atoll> {
        let how = std::mem::replace(&mut *self.how.lock(), How::Taken);
        let (recovered_end, r) = match how {
            How::Created(r) => (None, r),
            How::Recovered(r) => (Some(r.end), r),
            How::Recovering { .. } | How::Taken => {
                return Err(LimboError::InternalError(
                    "finishing an open before recovery is done".into(),
                ))
            }
        };
        let Recovered {
            log,
            pool,
            checkpointer,
            txns,
            ..
        } = *r;
        Ok(Atoll {
            database_id: self.superblock.database_id,
            recovered_end,
            txns,
            log,
            store: self.store,
            pool,
            checkpointer,
        })
    }
}
