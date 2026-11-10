//! A database: its control file, log, page store, buffer pool and checkpointer, created new or
//! recovered from what a crash left.

use super::buffer::{BufferConfig, BufferPool, LocalSource};
use super::checkpoint::{CheckpointRelief, Checkpointer, Checkpointing, WRITE_BYTES};
use super::control::{Control, Superblock};
use super::log::{Log, LogConfig};
use super::meta::TREES;
use super::page::CLASS_COUNT;
use super::recovery::{self, Recovered};
use super::requests::{Counted, Requests};
use super::store::{PageStore, StoreConfig};
use super::txn::{Clock, Transactions, TxnConfig};
use crate::io::IO;
use crate::storage::atoll::locks::Mutex;
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
    /// Bytes of a checkpoint's image writes in flight, beside its 32: `None` for
    /// [`WRITE_BYTES`], one page of the largest class. Less is taken as that.
    pub checkpoint_write_bytes: Option<usize>,
    /// Who steps the checkpoints: a thread of the database's own, or its writers.
    pub checkpointing: Checkpointing,
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
            checkpoint_write_bytes: None,
            checkpointing: Checkpointing::default_for_platform(),
            txn: TxnConfig::default(),
        }
    }

    /// A small database's configuration, for suites and tools that open many: the sizes every
    /// test of Atoll's SQL layer runs on, 1 MiB log segments, the smallest pool Atoll takes with
    /// its log's ring and a 512 KiB log budget, with the default transaction slots, so
    /// connections the suites hold at once never wait for one.
    /// The configuration every test of the SQL layer's trees runs with.
    #[cfg(test)]
    pub fn sql_tests() -> Self {
        super::sql::catalog::tests::config()
    }

    pub fn small() -> Self {
        let log = LogConfig {
            segment_bytes: 1 << 20,
            buffer_bytes: 256 << 10,
            chunk_bytes: 16 << 10,
            writes_in_flight: 3,
            ..LogConfig::default()
        };
        let budget = super::buffer::min_budget_beside(
            super::log::ring_bytes(&log),
            super::buffer::bank_frames(WRITE_BYTES),
        );
        Self {
            log,
            store: StoreConfig {
                file_bytes: 1 << 30,
                meta_pages: 1 << 10,
            },
            buffer: BufferConfig::with_budget(budget),
            log_budget: Some(512 << 10),
            checkpoint_write_bytes: None,
            checkpointing: Checkpointing::Writers,
            txn: TxnConfig::default(),
        }
    }

    pub(super) fn checkpoint_write_bytes(&self) -> usize {
        self.checkpoint_write_bytes.unwrap_or(WRITE_BYTES)
    }

    pub(super) fn log_budget(&self) -> u64 {
        self.log_budget
            .unwrap_or(self.buffer.budget as u64 * 2 / 5)
            .max(1)
    }
}

pub(in crate::storage::atoll) struct Atoll {
    #[cfg(test)]
    recovered: bool,
    log: Arc<Log>,
    store: Arc<PageStore>,
    pool: Arc<BufferPool>,
    checkpointer: Arc<Checkpointer>,
    txns: Arc<Transactions>,
    /// The checkpointing thread ([`Checkpointing::Thread`]), ended and joined when the database
    /// closes.
    driver: Option<crate::thread::JoinHandle<()>>,
    /// The IO every file of the database was opened through, counting its requests.
    io: Arc<dyn IO>,
    requests: Arc<Requests>,
}

impl Drop for Atoll {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        if let Some(driver) = self.driver.take() {
            self.checkpointer.stop_driver();
            if driver.join().is_err() {
                tracing::error!("the checkpointing thread panicked");
            }
        }
        // No request on the database's files outlives it (DESIGN.md 22.12, step 4).
        self.requests.drain(&*self.io);
    }
}

impl Atoll {
    /// Opens the database at `base`, creating it if it does not exist and recovering it on this
    /// thread if it does. To recover on several threads, use [`Opening`].
    pub fn open(io: Arc<dyn IO>, base: &str, config: AtollConfig) -> Result<Self> {
        let opening = Opening::new(io, base, config)?;
        opening.work()?;
        opening.finish()
    }

    /// Whether this open recovered a database a run before it left.
    #[cfg(test)]
    pub fn recovered(&self) -> bool {
        self.recovered
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

    /// The checkpointer as the checkpointing thread holds it, for a test to tell it let go.
    #[cfg(test)]
    pub(crate) fn checkpointer_arc(&self) -> &Arc<Checkpointer> {
        &self.checkpointer
    }
}

/// Opening a database, which recovery may make long. Any number of threads may call
/// [`work`](Self::work) at once: one recovers while the rest wait for it; once every call has
/// returned, [`finish`](Self::finish) gives the database.
pub(in crate::storage::atoll) struct Opening {
    store: Arc<PageStore>,
    superblock: Superblock,
    how: Mutex<How>,
    /// The embedder's IO wrapped to count the requests on the database's files: every file is
    /// opened through it, from the first.
    io: Arc<dyn IO>,
    requests: Arc<Requests>,
    checkpointing: Checkpointing,
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
        let requests = Arc::new(Requests::default());
        let io: Arc<dyn IO> = Arc::new(Counted::new(io, requests.clone()));
        // The log's ring and the image bank are lent from the pool for as long as it is open,
        // taken as it opens; the smallest pool is the budget beside them and the copies' worst
        // layout (DESIGN.md 9.1.1).
        let ring = super::log::ring_bytes(&config.log);
        let bank = super::buffer::bank_frames(config.checkpoint_write_bytes());
        let need = super::buffer::min_budget_beside(ring, bank);
        if config.buffer.budget < need {
            return Err(LimboError::InternalError(format!(
                "a budget of {} bytes holds the log's ring of {ring} bytes, the image bank of \
                 {bank} frames and copies of three pages, and not the smallest pool beside them: \
                 {need} bytes at least",
                config.buffer.budget
            )));
        }
        config.buffer.lent_at_open = super::buffer::LentAtOpen {
            ring: super::log::ring_chunks(&config.log),
            bank,
        };
        let (control, superblock) = Control::open(io.clone(), base)?;
        let Some(superblock) = superblock else {
            return Self::create(io, requests, base, config, control);
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
            io: io.clone(),
            requests,
            checkpointing: config.checkpointing,
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
    fn create(
        io: Arc<dyn IO>,
        requests: Arc<Requests>,
        base: &str,
        config: AtollConfig,
        control: Control,
    ) -> Result<Self> {
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
        // Its ring in frames the pool lends: the pool first.
        let log = Log::open_in(io.clone(), base, config.log.clone(), 0, &pool)?;
        let txns = Transactions::new(&config.txn, Arc::new(Clock::after(0)), log.clone());
        let checkpointer = Checkpointer::new(
            pool.clone(),
            store.clone(),
            log.clone(),
            control,
            superblock.clone(),
            config.log_budget(),
            config.checkpoint_write_bytes(),
        );
        Ok(Self {
            store,
            superblock,
            io,
            requests,
            checkpointing: config.checkpointing,
            how: Mutex::new(How::Created(Box::new(Recovered {
                log,
                pool,
                checkpointer,
                txns,
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
        #[cfg(test)]
        let recovered = matches!(how, How::Recovered(_));
        let r = match how {
            How::Created(r) | How::Recovered(r) => r,
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
        let (checkpointer, txns) = (Arc::new(checkpointer), Arc::new(txns));
        // A writer refused room for kept copies steps the checkpoint itself, or with a thread
        // stepping checkpoints waits for the room it frees.
        pool.set_relief(Arc::new(CheckpointRelief::new(&checkpointer, &txns)));
        let driver = match self.checkpointing {
            Checkpointing::Writers => None,
            Checkpointing::Thread => {
                let (cp, txns, io) = (checkpointer.clone(), txns.clone(), self.io.clone());
                let handle = crate::thread::Builder::new()
                    .name("atoll-checkpoint".into())
                    .spawn(move || cp.drive(&io, &txns))
                    .map_err(|e| {
                        LimboError::InternalError(format!(
                            "the checkpointing thread did not start: {e}"
                        ))
                    })?;
                // Named before the database is handed out: from then on no writer steps a
                // checkpoint.
                checkpointer.attach_driver(handle.thread().clone());
                Some(handle)
            }
        };
        Ok(Atoll {
            #[cfg(test)]
            recovered,
            txns,
            log,
            store: self.store,
            pool,
            checkpointer,
            driver,
            io: self.io,
            requests: self.requests,
        })
    }
}
