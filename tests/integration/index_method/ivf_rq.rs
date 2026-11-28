//! The `ivf_rq` index method's tests, moved from turso_core's own unit tests, which can no longer
//! parse SQL: they run SQL over its answers, its maintenance and its check, and read the index's
//! backing tables to compare them with what the index says.
#![cfg(feature = "test_helper")]

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use turso_core::index_method::ivf_rq::{
    block_code_bytes, block_count, centroid_key, check, dead_blocks, dense, directory_key, dot, is_mark, kind_of, l2, mark_key, part_key, partition_of, split_bound, trained_partitions, unpack, Directory,
    Layout, Location, Part, State, BLOCK_READS, BLOCK_ROW, COMPACT_AT, DEAD_ROW, EXACT_SUMS, KILLS, LEASE, PACK_AT, PART_ROW, POINT_READS, PROBES, QUERY_REFINES, QUERY_ROUNDS, REBUILD_LEASE, REBUILT, REDO_BITS,
    ROWID_BITS, SETTLE, STATEMENT_BUDGET, STATEMENT_UPKEEP, SURVEYED, TABLE_READS, TAIL_ROW, VERSION_BITS, WALK_SEEKS,
};
use turso_core::{LimboError, Value};

fn random(state: &mut u64, dims: usize) -> Vec<f32> {
    (0..dims)
        .map(|_| {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            (*state >> 11) as f32 / (1u64 << 53) as f32 - 0.5
        })
        .collect()
}

/// A query scores entries' 7-bit codes from the blocks and tail entries it read: with the model
/// held from an earlier query it makes no point read, however many entries it refines, in a
/// packed index with a tail and stale copies, and its answer is the exact top ten.
#[test]
fn a_query_refines_its_candidates_without_a_point_read() {
    use turso_core::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags};
    use turso_sqlite::SqliteDialect;
    const DIMS: usize = 64;
    for mvcc in [false, true] {
        let db = Database::open_file_with_flags(
            Arc::new(MemoryIO::new()),
            &format!("ivf-rq-no-point-reads-{mvcc}.db"),
            OpenFlags::default(),
            DatabaseOpts::new().with_index_method(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)").unwrap();
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 64, refine = 4)").unwrap();
        let mut state = 41;
        let mut live: HashMap<i64, Vec<f32>> = HashMap::new();
        for id in 1..=600i64 {
            let v = random(&mut state, DIMS);
            conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v))).unwrap();
            live.insert(id, v);
        }
        for _ in 0..30 {
            optimize(&conn, "v_ivf");
        }
        // Stale copies in packed blocks, and fresh entries in the tails.
        for id in (3..=600i64).step_by(3) {
            let v = random(&mut state, DIMS);
            conn.execute(format!("UPDATE v SET e = {} WHERE id = {id}", literal(&v))).unwrap();
            live.insert(id, v);
        }
        for id in (10..=600i64).step_by(10) {
            conn.execute(format!("DELETE FROM v WHERE id = {id}")).unwrap();
            live.remove(&id);
        }
        // The first query after the writes reads the directory rows of the partitions they split.
        ids(&conn, &format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10", literal(&random(&mut state, DIMS))));
        for _ in 0..3 {
            let q = random(&mut state, DIMS);
            let sql = format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10", literal(&q));
            let (reads, refines) = (POINT_READS.with(|c| c.get()), QUERY_REFINES.with(|c| c.get()));
            let got = ids(&conn, &sql);
            let reads = POINT_READS.with(|c| c.get()) - reads;
            let refines = QUERY_REFINES.with(|c| c.get()) - refines;
            assert!(refines > 0, "mvcc={mvcc}: the query refined no candidate, so its reads measure nothing");
            assert_eq!(reads, 0, "mvcc={mvcc}: refining {refines} candidates made {reads} point reads");
            let mut exact: Vec<(f64, i64)> = live.iter().map(|(id, v)| (dot(&q, v), *id)).collect();
            exact.sort_by(|a, b| b.0.total_cmp(&a.0));
            assert_eq!(got, exact[..10].iter().map(|(_, id)| *id).collect::<Vec<_>>(), "mvcc={mvcc}");
        }
    }
}

/// An index created with `rerank = 0` answers from its best 7-bit estimates and reads no table
/// row: at a refine of one, the same ten rows a reranking index returns, each at an estimated
/// distance near its exact one.
#[test]
fn a_query_without_the_rerank_answers_from_its_estimates_and_reads_no_table_row() {
    const DIMS: usize = 64;
    let mut answers = Vec::new();
    for rerank in [1, 0] {
        let (_db, conn) = database(&format!("ivf-rq-rerank-{rerank}.db"), false);
        let mut state = 139;
        let vectors: Vec<(i64, Vec<f32>)> = (1..=2000i64).map(|id| (id, random(&mut state, DIMS))).collect();
        insert_all(&conn, &vectors);
        conn.execute(format!("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 1, rerank = {rerank})")).unwrap();
        let q = random(&mut state, DIMS);
        top_ten(&conn, &q);
        let reads = TABLE_READS.with(|c| c.get());
        let answer: Vec<(i64, f64)> = rows(&conn, &format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10", literal(&q)))
            .iter()
            .map(|row| (row[0].as_int().unwrap(), row[1].as_float()))
            .collect();
        let reads = TABLE_READS.with(|c| c.get()) - reads;
        let live: HashMap<i64, Vec<f32>> = vectors.into_iter().collect();
        answers.push((answer, reads, live, q));
    }
    let ((reranked, reranked_reads, _, _), (estimated, estimated_reads, live, q)) = (&answers[0], &answers[1]);
    assert!(*reranked_reads >= 10 && *estimated_reads == 0, "table rows read: {reranked_reads} reranking, {estimated_reads} without");
    let ids = |answer: &[(i64, f64)]| {
        let mut ids: Vec<i64> = answer.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        ids
    };
    assert_eq!(ids(reranked), ids(estimated));
    for (id, distance) in estimated {
        let exact = -dot(q, &live[id]);
        assert!((distance - exact).abs() < 0.1, "row {id}: estimated distance {distance}, exact {exact}");
    }
}

fn rows(conn: &Arc<turso_core::Connection>, sql: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect());
        Ok(())
    })
    .unwrap();
    rows
}

fn literal(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| format!("{x}")).collect();
    format!("vector32('[{}]')", parts.join(","))
}

/// An ivf_rq index written through inserts, updates, deletes and maintenance answers each
/// top-10 query with the rows an exact scan of the live vectors ranks first, in WAL and MVCC
/// mode.
#[test]
fn the_index_answers_the_exact_top_ten_in_wal_and_mvcc() {
    use turso_core::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags};
    use turso_sqlite::SqliteDialect;
    const DIMS: usize = 32;
    for mvcc in [false, true] {
        let db = Database::open_file_with_flags(
            Arc::new(MemoryIO::new()),
            &format!("ivf-rq-top-ten-{mvcc}.db"),
            OpenFlags::default(),
            DatabaseOpts::new().with_index_method(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)")
            .unwrap();
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 64, refine = 4)")
            .unwrap();
        let mut state = 17;
        let mut live: HashMap<i64, Vec<f32>> = HashMap::new();
        for id in 1..=300i64 {
            let v = random(&mut state, DIMS);
            conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v)))
                .unwrap();
            live.insert(id, v);
        }
        for id in (5..=300i64).step_by(5) {
            let v = random(&mut state, DIMS);
            conn.execute(format!("UPDATE v SET e = {} WHERE id = {id}", literal(&v)))
                .unwrap();
            live.insert(id, v);
        }
        for id in (7..=300i64).step_by(7) {
            conn.execute(format!("DELETE FROM v WHERE id = {id}")).unwrap();
            live.remove(&id);
        }
        for _ in 0..20 {
            optimize(&conn, "v_ivf");
        }
        for _ in 0..5 {
            let q = random(&mut state, DIMS);
            let sql = format!(
                "SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10",
                literal(&q)
            );
            // The plan must query the index; a scan of the table would match the exact
            // ranking trivially.
            let plan = rows(&conn, &format!("EXPLAIN {sql}"));
            assert!(
                plan.iter().any(|row| row[1].to_string() == "IndexMethodQuery"),
                "mvcc={mvcc}: the query does not go through the index"
            );
            let got: Vec<i64> = rows(&conn, &sql)
            .iter()
            .map(|row| row[0].as_int().unwrap())
            .collect();
            let mut exact: Vec<(f64, i64)> = live.iter().map(|(id, v)| (dot(&q, v), *id)).collect();
            exact.sort_by(|a, b| b.0.total_cmp(&a.0));
            let expected: Vec<i64> = exact[..10].iter().map(|(_, id)| *id).collect();
            assert_eq!(got, expected, "mvcc={mvcc}");
        }
    }
}

/// A query near vectors that were packed into blocks and then updated away returns the live
/// top ten: the stale entries their blocks keep must not raise the bar the live entries meet.
#[test]
fn a_query_near_vectors_updated_away_returns_the_live_top_ten() {
    use turso_core::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags};
    use turso_sqlite::SqliteDialect;
    const DIMS: usize = 128;
    for mvcc in [false, true] {
        let db = Database::open_file_with_flags(
            Arc::new(MemoryIO::new()),
            &format!("ivf-rq-stale-bar-{mvcc}.db"),
            OpenFlags::default(),
            DatabaseOpts::new().with_index_method(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)").unwrap();
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 64, refine = 4)").unwrap();
        let mut state = 23;
        let q = random(&mut state, DIMS);
        let mut live: HashMap<i64, Vec<f32>> = HashMap::new();
        for id in 1..=216i64 {
            let v = if id > 200 { q.iter().zip(random(&mut state, DIMS)).map(|(a, n)| a + 0.002 * n).collect() } else { random(&mut state, DIMS) };
            conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v))).unwrap();
            live.insert(id, v);
        }
        for _ in 0..50 {
            optimize(&conn, "v_ivf");
        }
        for id in 201..=216i64 {
            let v = random(&mut state, DIMS);
            conn.execute(format!("UPDATE v SET e = {} WHERE id = {id}", literal(&v))).unwrap();
            live.insert(id, v);
        }
        let sql = format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10", literal(&q));
        let plan = rows(&conn, &format!("EXPLAIN {sql}"));
        assert!(plan.iter().any(|row| row[1].to_string() == "IndexMethodQuery"), "mvcc={mvcc}: the query does not go through the index");
        let got: Vec<i64> = rows(&conn, &sql).iter().map(|row| row[0].as_int().unwrap()).collect();
        let mut exact: Vec<(f64, i64)> = live.iter().map(|(id, v)| (dot(&q, v), *id)).collect();
        exact.sort_by(|a, b| b.0.total_cmp(&a.0));
        let expected: Vec<i64> = exact[..10].iter().map(|(_, id)| *id).collect();
        assert_eq!(got, expected, "mvcc={mvcc}");
    }
}

/// Revising vectors near a query round after round, with maintenance between the rounds, keeps
/// each top ten exact; once maintenance has run out of work no block holds [`COMPACT_AT`] stale
/// copies, and every dead row names blocks that still hold a copy of its vector that the
/// vector's row does not point to.
#[test]
fn revisions_keep_the_top_ten_exact_and_every_dead_row_true() {
    use turso_core::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags};
    use turso_sqlite::SqliteDialect;
    const DIMS: usize = 128;
    for mvcc in [false, true] {
        let db = Database::open_file_with_flags(
            Arc::new(MemoryIO::new()),
            &format!("ivf-rq-revisions-{mvcc}.db"),
            OpenFlags::default(),
            DatabaseOpts::new().with_index_method(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)").unwrap();
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 64, refine = 4)").unwrap();
        let mut state = 29;
        let q = random(&mut state, DIMS);
        let near = |state: &mut u64| -> Vec<f32> { q.iter().zip(random(state, DIMS)).map(|(a, n)| a + 0.002 * n).collect() };
        let exact_top = |live: &HashMap<i64, Vec<f32>>| -> Vec<i64> {
            let mut exact: Vec<(f64, i64)> = live.iter().map(|(id, v)| (dot(&q, v), *id)).collect();
            exact.sort_by(|a, b| b.0.total_cmp(&a.0));
            exact[..10].iter().map(|(_, id)| *id).collect()
        };
        let sql = format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10", literal(&q));
        let mut live: HashMap<i64, Vec<f32>> = HashMap::new();
        for id in 1..=300i64 {
            let v = if id > 280 { near(&mut state) } else { random(&mut state, DIMS) };
            conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v))).unwrap();
            live.insert(id, v);
        }
        for round in 0..4 {
            for _ in 0..30 {
                optimize(&conn, "v_ivf");
            }
            // Three rounds revise the vectors near the query again; the last moves them away.
            for id in 281..=300i64 {
                let v = if round < 3 { near(&mut state) } else { random(&mut state, DIMS) };
                conn.execute(format!("UPDATE v SET e = {} WHERE id = {id}", literal(&v))).unwrap();
                live.insert(id, v);
            }
            let got: Vec<i64> = rows(&conn, &sql).iter().map(|row| row[0].as_int().unwrap()).collect();
            assert_eq!(got, exact_top(&live), "mvcc={mvcc} round {round}");
        }
        for _ in 0..100 {
            optimize(&conn, "v_ivf");
        }
        let got: Vec<i64> = rows(&conn, &sql).iter().map(|row| row[0].as_int().unwrap()).collect();
        assert_eq!(got, exact_top(&live), "mvcc={mvcc} after maintenance");
        let mut stale: HashMap<i64, usize> = HashMap::new();
        for row in partition_rows(&conn, DEAD_ROW) {
            let rowid = row[0].as_int().unwrap() & ((1 << ROWID_BITS) - 1);
            let blocks = dead_blocks(row[1].to_blob().unwrap()).unwrap();
            assert!(!blocks.is_empty(), "mvcc={mvcc}: the dead row of {rowid} names no block");
            let location = rows(&conn, &format!("SELECT location FROM __turso_internal_v_ivf_vectors WHERE rowid = {rowid}"));
            for block in blocks {
                assert!(
                    location.iter().all(|r| r[0].as_int() != Some(Location::Block(block).encode())),
                    "mvcc={mvcc}: the dead row of {rowid} names block {block}, where its vector lives"
                );
                let payload = rows(&conn, &format!("SELECT c1 FROM __turso_internal_v_ivf_partitions WHERE rowid = {block}"));
                let payload = payload.first().unwrap_or_else(|| panic!("mvcc={mvcc}: the dead row of {rowid} names missing block {block}"));
                let entries = unpack(payload[0].to_blob().unwrap()).unwrap();
                assert!(entries.iter().any(|e| e.rowid == rowid), "mvcc={mvcc}: block {block} holds no copy of {rowid}");
                *stale.entry(block).or_default() += 1;
            }
        }
        assert!(stale.values().all(|count| *count < COMPACT_AT), "mvcc={mvcc}: blocks left with stale copies {stale:?}");
    }
}

fn database(name: &str, mvcc: bool) -> (Arc<turso_core::Database>, Arc<turso_core::Connection>) {
    use turso_core::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags};
    use turso_sqlite::SqliteDialect;
    let db = Database::open_file_with_flags(Arc::new(MemoryIO::new()), name, OpenFlags::default(), DatabaseOpts::new().with_index_method(true), None, Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect(ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
    if mvcc {
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    }
    conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)").unwrap();
    (db, conn)
}

/// A vector near one of `centres`, the centre chosen by the generator.
fn near(state: &mut u64, centres: &[Vec<f32>], spread: f32) -> Vec<f32> {
    let pick = ((random(state, 1)[0] + 0.5) * centres.len() as f32) as usize;
    let centre = &centres[pick.min(centres.len() - 1)];
    centre.iter().zip(random(state, centre.len())).map(|(c, n)| c + spread * n).collect()
}

/// One statement inserting every vector of `rows`.
fn insert_all(conn: &Arc<turso_core::Connection>, rows: &[(i64, Vec<f32>)]) {
    let values: Vec<String> = rows.iter().map(|(id, v)| format!("({id}, {})", literal(v))).collect();
    conn.execute(format!("INSERT INTO v VALUES {}", values.join(","))).unwrap();
}

fn top_ten(conn: &Arc<turso_core::Connection>, q: &[f32]) -> Vec<i64> {
    rows(conn, &format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10", literal(q))).iter().map(|row| row[0].as_int().unwrap()).collect()
}

fn exact_top_ten(live: &HashMap<i64, Vec<f32>>, q: &[f32]) -> Vec<i64> {
    let mut exact: Vec<(f64, i64)> = live.iter().map(|(id, v)| (dot(q, v), *id)).collect();
    exact.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    exact.iter().take(10).map(|(_, id)| *id).collect()
}

/// The rows of one of index `v_ivf`'s backing tables: the rowid, then `columns`.
fn backing(conn: &Arc<turso_core::Connection>, store: &str, columns: &str) -> Vec<Vec<Value>> {
    rows(conn, &format!("SELECT rowid, {columns} FROM __turso_internal_v_ivf_{store}"))
}

/// The index as its backing tables hold it: each partition's bookkeeping, tail length, marks and
/// current centroid version, and the stale copies per block.
struct Stored {
    parts: HashMap<i64, Part>,
    tails: HashMap<i64, usize>,
    marks: HashMap<i64, usize>,
    versions: HashMap<i64, i64>,
    stale: HashMap<i64, usize>,
    redo: usize,
}

/// The partitions table's rows of `kind`: the key, then the row's columns.
fn partition_rows(conn: &Arc<turso_core::Connection>, kind: i64) -> Vec<Vec<Value>> {
    backing(conn, "partitions", "c0, c1, c2, c3, c4, c5, c6, c7").into_iter().filter(|r| kind_of(r[0].as_int().unwrap()) == kind).collect()
}

fn stored(conn: &Arc<turso_core::Connection>) -> Stored {
    let parts = partition_rows(conn, PART_ROW)
        .iter()
        .filter(|r| !is_mark(r[0].as_int().unwrap()))
        .map(|r| {
            let f = |i: usize| r[i].as_int().unwrap();
            let part = Part { next: f(1), size: f(2), open: f(3), move_next: f(4), move_end: f(5), state: State::decode(f(6)).unwrap(), retry: f(7), version: f(8) };
            (partition_of(f(0)), part)
        })
        .collect();
    let mut tails = HashMap::new();
    for r in partition_rows(conn, TAIL_ROW) {
        *tails.entry(partition_of(r[0].as_int().unwrap())).or_default() += 1;
    }
    let mut marks = HashMap::new();
    for r in partition_rows(conn, PART_ROW).iter().filter(|r| is_mark(r[0].as_int().unwrap())) {
        *marks.entry(partition_of(r[0].as_int().unwrap())).or_default() += 1;
    }
    let mut versions: HashMap<i64, i64> = HashMap::new();
    for r in backing(conn, "centroids", "value") {
        let key = r[0].as_int().unwrap();
        if key >= 0 {
            let version = versions.entry(key >> VERSION_BITS).or_default();
            *version = (*version).max(key & ((1 << VERSION_BITS) - 1));
        }
    }
    let mut stale = HashMap::new();
    for r in partition_rows(conn, DEAD_ROW) {
        for block in dead_blocks(r[1].to_blob().unwrap()).unwrap() {
            *stale.entry(block).or_default() += 1;
        }
    }
    let redo = backing(conn, "redo", "writer").len();
    Stored { parts, tails, marks, versions, stale, redo }
}

impl Stored {
    fn part(&self, p: i64) -> Part {
        self.parts.get(&p).copied().unwrap_or_default()
    }

    fn tail(&self, p: i64) -> usize {
        self.tails.get(&p).copied().unwrap_or(0)
    }

    fn partitions(&self) -> Vec<i64> {
        let last = self.versions.keys().chain(self.parts.keys()).chain(self.tails.keys()).chain(self.marks.keys()).copied().max().unwrap_or(0);
        (0..=last).collect()
    }

    fn live(&self) -> Vec<i64> {
        self.partitions().into_iter().filter(|p| self.part(*p).state == State::Live).collect()
    }

    fn live_entries(&self, p: i64) -> i64 {
        let stale: usize = self.stale.iter().filter(|(block, _)| partition_of(**block) == p).map(|(_, count)| *count).sum();
        self.part(p).size + self.tail(p) as i64 - stale as i64
    }

    /// The jobs left, by the rules maintenance chooses them by.
    fn pending(&self) -> Vec<String> {
        let live = self.live();
        let split_bound = split_bound(live.len().max(1));
        let mut left = Vec::new();
        for p in self.partitions() {
            let part = self.part(p);
            if part.moving() {
                left.push(format!("partition {p} is moving"));
            }
            if let Some(n) = self.marks.get(&p) {
                left.push(format!("partition {p} has {n} marks to take up"));
            }
            if part.state != State::Live {
                if self.tail(p) > 0 {
                    left.push(format!("{:?} partition {p} has a tail", part.state));
                }
                if part.state == State::Retiring {
                    left.push(format!("partition {p} is retiring"));
                }
                continue;
            }
            if self.tail(p) >= PACK_AT {
                left.push(format!("partition {p} has a tail of {}", self.tail(p)));
            }
            if live.len() > 1 && self.live_entries(p) < split_bound / 16 {
                left.push(format!("partition {p} holds {} live entries", self.live_entries(p)));
            }
            let size = part.size + self.tail(p) as i64;
            if size >= split_bound.max(part.retry) {
                left.push(format!("partition {p} holds {size}"));
            }
        }
        for (block, count) in &self.stale {
            if *count >= COMPACT_AT {
                left.push(format!("block {block} holds {count} stale copies"));
            }
        }
        if self.redo > 0 {
            left.push(format!("{} redo rows", self.redo));
        }
        left
    }
}

/// Plain inserts, with no OPTIMIZE INDEX, keep the index maintained: their statements pack the
/// tails they grow and split the partitions they fill.
#[test]
fn plain_writes_keep_the_index_maintained() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-writes-{mvcc}.db"), mvcc);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let mut state = 31;
        let mut live = HashMap::new();
        for id in 1..=1500i64 {
            let v = random(&mut state, DIMS);
            conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v))).unwrap();
            live.insert(id, v);
        }
        let index = stored(&conn);
        let tails: usize = index.tails.values().sum();
        assert!(index.live().len() >= 8, "mvcc={mvcc}: {} live partitions after 1500 inserts", index.live().len());
        assert!(tails < 150, "mvcc={mvcc}: {tails} entries left in tails");
        for _ in 0..3 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
    }
}

/// Statements inserting a hundred rows each, with no OPTIMIZE INDEX, keep the index maintained:
/// each pays for the maintenance its rows make due, so no partition grows to twice the split
/// bound and work a statement leaves, the moves of a split late in it, is done by the next.
#[test]
fn statements_of_many_rows_keep_the_index_maintained() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-statements-{mvcc}.db"), mvcc);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let mut state = 61;
        let mut live = HashMap::new();
        let mut before: Vec<String> = Vec::new();
        for statement in 0..40i64 {
            let rows: Vec<(i64, Vec<f32>)> = (1..=100).map(|i| (statement * 100 + i, random(&mut state, DIMS))).collect();
            insert_all(&conn, &rows);
            live.extend(rows);
            let index = stored(&conn);
            let bound = split_bound(index.live().len().max(1));
            let biggest = index.live().iter().map(|p| index.part(*p).size + index.tail(*p) as i64).max().unwrap_or(0);
            assert!(biggest < 2 * bound, "mvcc={mvcc}: a partition holds {biggest} after statement {statement}, the split bound being {bound}");
            let left = index.pending();
            assert!(before.is_empty() || left.is_empty(), "mvcc={mvcc}: statements {} and {statement} both left work: {before:?}, then {left:?}", statement - 1);
            before = left;
        }
        let index = stored(&conn);
        assert!(index.live().len() >= trained_partitions(4000) / 2, "mvcc={mvcc}: {} live partitions for 4000 rows", index.live().len());
        for _ in 0..3 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
    }
}

/// Work a statement's maintenance found due but had no job left for is done by the next
/// statement's, though that one writes none of the partitions the work lies in.
#[test]
fn work_a_statement_left_due_is_done_by_the_next() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-left-{mvcc}.db"), mvcc);
        let mut state = 127;
        let centres: Vec<Vec<f32>> = (0..12).map(|_| random(&mut state, DIMS).iter().map(|x| 3.0 * x).collect()).collect();
        let rows: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, near(&mut state, &centres, 0.4))).collect();
        insert_all(&conn, &rows);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        assert!(stored(&conn).pending().is_empty(), "mvcc={mvcc}: {:?}", stored(&conn).pending());
        let mut next = 3000i64;
        let mut around = |state: &mut u64, c: usize, n: usize| -> Vec<(i64, Vec<f32>)> {
            (0..n)
                .map(|_| {
                    next += 1;
                    (next, near(state, &centres[c..=c], 0.01))
                })
                .collect()
        };
        // One statement grows two tails past the pack length with one job to spend: it packs one.
        let mut two = around(&mut state, 0, 20);
        two.extend(around(&mut state, 1, 20));
        STATEMENT_BUDGET.with(|budget| budget.set(1));
        insert_all(&conn, &two);
        STATEMENT_BUDGET.with(|budget| budget.set(usize::MAX));
        let left = stored(&conn).pending();
        assert_eq!(left.len(), 1, "mvcc={mvcc}: the statement left {left:?}");
        // The next writes one row, into a third partition.
        insert_all(&conn, &around(&mut state, 2, 1));
        let left = stored(&conn).pending();
        assert!(left.is_empty(), "mvcc={mvcc}: the next statement left {left:?}");
    }
}

/// One OPTIMIZE INDEX does every job a backlog holds: over a trained index, statements that do
/// not maintain it leave packs, splits and moves due, and after one call none is.
#[test]
fn one_optimize_finishes_every_job() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-backlog-{mvcc}.db"), mvcc);
        let mut state = 37;
        let live = backlog(&conn, &mut state, DIMS);
        let backlog = stored(&conn).pending();
        assert!(backlog.len() > 1, "mvcc={mvcc}: the writes left {backlog:?} to finish");
        optimize(&conn, "v_ivf");
        let left = stored(&conn).pending();
        assert!(left.is_empty(), "mvcc={mvcc}: work left after one OPTIMIZE: {left:?}");
        for _ in 0..3 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
    }
}

/// A trained index with a backlog: 3,000 rows inserted with maintenance, then 1,500 more by
/// statements of a hundred that do none, leaving packs, splits and moves due. The rows by id.
fn backlog(conn: &Arc<turso_core::Connection>, state: &mut u64, dims: usize) -> HashMap<i64, Vec<f32>> {
    conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
    let mut rows: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, random(state, dims))).collect();
    insert_all(conn, &rows);
    STATEMENT_UPKEEP.with(|on| on.set(false));
    for statement in 0..15i64 {
        let more: Vec<(i64, Vec<f32>)> = (1..=100).map(|i| (3000 + statement * 100 + i, random(state, dims))).collect();
        insert_all(conn, &more);
        rows.extend(more);
    }
    STATEMENT_UPKEEP.with(|on| on.set(true));
    rows.into_iter().collect()
}

/// Where a statement stepped on to: it yielded between steps of its work, slept out a wait it
/// asked for, or ended.
#[derive(Debug, PartialEq)]
enum Paused {
    Yielded,
    Slept,
    Ended,
}

fn step_to_pause(db: &turso_core::Database, statement: &mut turso_core::Statement) -> Paused {
    loop {
        match statement.step().unwrap() {
            turso_core::StepResult::Yield => return Paused::Yielded,
            turso_core::StepResult::Done => return Paused::Ended,
            turso_core::StepResult::IO => db.io.step().unwrap(),
            turso_core::StepResult::Sleep { duration } => {
                std::thread::sleep(duration);
                return Paused::Slept;
            }
            other => panic!("the statement stepped to {other:?}"),
        }
    }
}

/// Step `statement` until it yields between steps of its work, or sleeps out a wait it asks
/// for, true, or ends, false.
fn step_to_yield(db: &turso_core::Database, statement: &mut turso_core::Statement) -> bool {
    step_to_pause(db, statement) != Paused::Ended
}

/// OPTIMIZE INDEX of `index` run to its end on `conn`, failing past ten thousand steps instead of
/// running on: on a model held stale it did not end.
fn optimize(conn: &Arc<turso_core::Connection>, index: &str) {
    let mut optimize = conn.prepare(format!("OPTIMIZE INDEX {index}")).unwrap();
    let mut yields = 0usize;
    loop {
        match optimize.step().unwrap() {
            turso_core::StepResult::Done => return,
            turso_core::StepResult::Yield => {
                yields += 1;
                assert!(yields < 10_000, "OPTIMIZE INDEX yielded ten thousand times, leaving {:?}", stored(conn).pending());
            }
            turso_core::StepResult::IO => optimize._io().step().unwrap(),
            turso_core::StepResult::Sleep { duration } => std::thread::sleep(duration),
            other => panic!("OPTIMIZE INDEX stepped to {other:?}"),
        }
    }
}

/// A transaction a host steps a little at a time beside another statement, as a host that runs
/// every connection on one thread does: each turn runs its current statement until it waits.
struct Interleaved {
    connection: Arc<turso_core::Connection>,
    sql: VecDeque<String>,
    current: Option<turso_core::Statement>,
}

impl Interleaved {
    fn new(connection: &Arc<turso_core::Connection>, sql: &[String]) -> Self {
        Self { connection: connection.clone(), sql: sql.iter().cloned().collect(), current: None }
    }

    /// Run the current statement until it ends or waits, busy or on I/O; true once every
    /// statement has ended.
    fn turn(&mut self, db: &turso_core::Database) -> bool {
        if self.current.is_none() {
            let Some(sql) = self.sql.pop_front() else {
                return true;
            };
            self.current = Some(self.connection.prepare(sql).unwrap());
        }
        let statement = self.current.as_mut().expect("prepared above");
        loop {
            match statement.step().unwrap() {
                turso_core::StepResult::Done => {
                    self.current = None;
                    return self.sql.is_empty();
                }
                turso_core::StepResult::Row => {}
                turso_core::StepResult::IO => {
                    db.io.step().unwrap();
                    return false;
                }
                turso_core::StepResult::Yield | turso_core::StepResult::Busy | turso_core::StepResult::Sleep { .. } => return false,
                other => panic!("the statement stepped to {other:?}"),
            }
        }
    }
}

/// OPTIMIZE INDEX commits its work a step at a time and yields between steps, so a writer
/// stepped beside it commits while it still has work to do: under WAL its steps take the write
/// lock one at a time, and under MVCC each is a concurrent transaction. Once it ends no work is
/// due and the queries are exact, the rows written meanwhile among them.
#[test]
fn optimize_gives_way_to_writers_between_its_steps() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (db, conn) = database(&format!("ivf-rq-optimize-steps-{mvcc}.db"), mvcc);
        let mut state = 223;
        let mut live = backlog(&conn, &mut state, DIMS);
        let writer = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
        let mut optimize = conn.prepare("OPTIMIZE INDEX v_ivf").unwrap();
        let (mut yields, mut committed) = (0usize, 0usize);
        let mut writing: Option<((i64, Vec<f32>), Interleaved)> = None;
        let mut finished = false;
        while !finished || writing.is_some() {
            if !finished {
                finished = !step_to_yield(&db, &mut optimize);
                yields += 1;
                assert!(yields < 10_000, "mvcc={mvcc}: OPTIMIZE INDEX yielded ten thousand times, leaving {:?}", stored(&conn).pending());
            }
            let (row, transaction) = writing.get_or_insert_with(|| {
                let row = (100_000 + committed as i64, random(&mut state, DIMS));
                let insert = format!("INSERT INTO v VALUES ({}, {})", row.0, literal(&row.1));
                let sql = match mvcc {
                    true => vec!["BEGIN CONCURRENT".to_string(), insert, "COMMIT".to_string()],
                    false => vec![insert],
                };
                (row, Interleaved::new(&writer, &sql))
            });
            if transaction.turn(&db) {
                live.insert(row.0, row.1.clone());
                writing = None;
                if !finished {
                    committed += 1;
                }
            }
        }
        assert!(committed > 0, "mvcc={mvcc}: OPTIMIZE INDEX ended before a writer beside it committed");
        let left = stored(&conn).pending();
        assert!(left.is_empty(), "mvcc={mvcc}: work left after OPTIMIZE INDEX: {left:?}");
        for _ in 0..3 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
    }
}

/// The index after inserts, updates, deletes, splits, packs, moves, compactions and OPTIMIZE
/// INDEX, under WAL and MVCC: every row with a vector has its live code where the index says,
/// coded from its vector, and the rest is accounted for.
#[test]
fn the_check_finds_the_index_consistent_after_every_kind_of_write() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-check-{mvcc}.db"), mvcc);
        let mut state = 311;
        let centres: Vec<Vec<f32>> = (0..6).map(|_| random(&mut state, DIMS).iter().map(|x| 3.0 * x).collect()).collect();
        let table: Vec<(i64, Vec<f32>)> = (1..=4000i64).map(|id| (id, near(&mut state, &centres, 0.5))).collect();
        insert_all(&conn, &table);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        for id in 4001..=5500i64 {
            conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&near(&mut state, &centres, 0.5)))).unwrap();
        }
        for id in (1..=5500i64).step_by(7) {
            conn.execute(format!("UPDATE v SET e = {} WHERE id = {id}", literal(&near(&mut state, &centres, 0.5)))).unwrap();
        }
        for id in (1..=5500i64).step_by(5) {
            conn.execute(format!("DELETE FROM v WHERE id = {id}")).unwrap();
        }
        let before = check(&conn, "v_ivf").unwrap();
        assert!(before.consistent(), "mvcc={mvcc}: {before:?}");
        optimize(&conn, "v_ivf");
        let after = check(&conn, "v_ivf").unwrap();
        assert!(after.consistent(), "mvcc={mvcc}: {after:?}");
        let integrity = rows(&conn, "PRAGMA integrity_check");
        assert!(integrity.len() == 1 && integrity[0][0].to_string() == "ok", "mvcc={mvcc}: integrity_check: {:?}", &integrity[..integrity.len().min(4)]);
        assert!(before.stale_copies > 0 && after.blocks > 0 && after.codes == after.rows, "mvcc={mvcc}: before {before:?}, after {after:?}");
        assert_eq!(after.rows, 5500 - 1100, "mvcc={mvcc}");
    }
}

/// One connection's inserts, updates and deletes, alone and in transactions it commits or rolls
/// back, as an application's, under WAL and MVCC. A split rolled back with its transaction
/// leaves the partition it opened among those later statements survey, and the model holds no
/// centroid for it: such a partition is none to retire or work on.
#[test]
fn transactions_rolled_back_over_maintenance_leave_the_index_consistent() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-rolled-back-{mvcc}.db"), mvcc);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let begin = if mvcc { "BEGIN CONCURRENT" } else { "BEGIN" };
        let mut state = 337u64;
        let below = |state: &mut u64, n: u64| {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state % n
        };
        let centres: Vec<Vec<f32>> = (0..8).map(|_| random(&mut state, DIMS).iter().map(|x| 6.0 * x).collect()).collect();
        let mut next = 1i64;
        let statement = |state: &mut u64, next: &mut i64| match below(state, 10) {
            0..=4 => {
                let n = 1 + below(state, 20) as i64;
                let rows: Vec<(i64, Vec<f32>)> = (0..n).map(|i| (*next + i, near(state, &centres, 0.5))).collect();
                *next += n;
                insert_all(&conn, &rows);
            }
            5..=7 => conn.execute(format!("UPDATE v SET e = {} WHERE id = {}", literal(&near(state, &centres, 0.5)), 1 + below(state, *next as u64))).unwrap(),
            _ => conn.execute(format!("DELETE FROM v WHERE id = {}", 1 + below(state, *next as u64))).unwrap(),
        };
        let mut rolled_back = 0;
        for _ in 0..800 {
            if below(&mut state, 100) < 80 {
                statement(&mut state, &mut next);
                continue;
            }
            conn.execute(begin).unwrap();
            for _ in 0..2 + below(&mut state, 5) {
                statement(&mut state, &mut next);
            }
            if below(&mut state, 6) == 0 {
                conn.execute("ROLLBACK").unwrap();
                rolled_back += 1;
            } else {
                conn.execute("COMMIT").unwrap();
            }
        }
        assert!(rolled_back > 10, "mvcc={mvcc}: {rolled_back} transactions rolled back");
        let found = check(&conn, "v_ivf").unwrap();
        assert!(found.consistent() && found.partitions > 8, "mvcc={mvcc}: {found:?}");
    }
}

/// Each way an index can disagree with its table or with itself is one the check reports: a
/// vector's row in the index gone, a tail entry gone, a code changed, bookkeeping miscounting
/// its blocks, a dead row gone, a centroid version gone, and a row the table does not have.
#[test]
fn the_check_reports_each_way_the_index_can_disagree_with_its_table() {
    const DIMS: usize = 16;
    let setup = |name: &str| {
        let (db, conn) = database(name, false);
        let mut state = 317;
        let rows: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
        insert_all(&conn, &rows);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        for id in 3001..=3020i64 {
            conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&random(&mut state, DIMS)))).unwrap();
        }
        for id in (1..=3000i64).step_by(9) {
            conn.execute(format!("UPDATE v SET e = {} WHERE id = {id}", literal(&random(&mut state, DIMS)))).unwrap();
        }
        let fine = check(&conn, "v_ivf").unwrap();
        assert!(fine.consistent() && fine.tail_entries > 0 && fine.stale_copies > 0, "{fine:?}");
        (db, conn)
    };
    let one = |conn: &Arc<turso_core::Connection>, sql: &str| rows(conn, sql).first().cloned().unwrap_or_else(|| panic!("{sql} found nothing"));
    // A nested statement may write the index's tables; it commits with the write transaction
    // around it.
    let corrupt = |conn: &Arc<turso_core::Connection>, sql: String| {
        conn.execute("BEGIN IMMEDIATE").unwrap();
        conn.start_nested();
        let done = conn.execute(&sql);
        conn.end_nested();
        done.unwrap();
        conn.execute("COMMIT").unwrap();
    };
    let findings = |conn: &Arc<turso_core::Connection>| {
        let found = check(conn, "v_ivf").unwrap();
        assert!(!found.consistent(), "the check found nothing wrong: {found:?}");
        found.findings.join("\n")
    };
    let (vectors, partitions, centroids) = ("__turso_internal_v_ivf_vectors", "__turso_internal_v_ivf_partitions", "__turso_internal_v_ivf_centroids");

    let (_db, conn) = setup("ivf-rq-check-vector-row.db");
    let rowid = one(&conn, &format!("SELECT rowid FROM {vectors} WHERE location >= 0 LIMIT 1"))[0].as_int().unwrap();
    corrupt(&conn, format!("DELETE FROM {vectors} WHERE rowid = {rowid}"));
    assert!(findings(&conn).contains(&format!("row {rowid} has a vector and no row in the index")));

    let (_db, conn) = setup("ivf-rq-check-tail.db");
    let found = one(&conn, &format!("SELECT rowid, location FROM {vectors} WHERE location < 0 LIMIT 1"));
    let (rowid, key) = (found[0].as_int().unwrap(), -1 - found[1].as_int().unwrap());
    corrupt(&conn, format!("DELETE FROM {partitions} WHERE rowid = {key}"));
    assert!(findings(&conn).contains(&format!("row {rowid}'s code is not at Tail({key})")));

    let (_db, conn) = setup("ivf-rq-check-code.db");
    let found = one(&conn, &format!("SELECT rowid, c1 FROM {partitions} WHERE (rowid >> {ROWID_BITS}) & 3 = {BLOCK_ROW} LIMIT 1"));
    let (key, mut bytes) = (found[0].as_int().unwrap(), found[1].to_blob().unwrap().to_vec());
    let gains = Layout::new(block_count(&bytes).unwrap(), block_code_bytes(&bytes).unwrap()).gains();
    bytes[gains + 3] ^= 0x40;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    corrupt(&conn, format!("UPDATE {partitions} SET c1 = X'{hex}' WHERE rowid = {key}"));
    assert!(findings(&conn).contains(&format!("code at Block({key}) is not its vector's")));

    let (_db, conn) = setup("ivf-rq-check-bookkeeping.db");
    let partition = one(&conn, &format!("SELECT rowid >> 42 FROM {partitions} WHERE (rowid >> {ROWID_BITS}) & 3 = {BLOCK_ROW} LIMIT 1"))[0].as_int().unwrap();
    corrupt(&conn, format!("UPDATE {partitions} SET c1 = c1 + 1 WHERE rowid = {}", part_key(partition).unwrap()));
    assert!(findings(&conn).contains(&format!("partition {partition}'s bookkeeping counts")));

    let (_db, conn) = setup("ivf-rq-check-dead-row.db");
    let found = one(&conn, &format!("SELECT rowid FROM {partitions} WHERE (rowid >> {ROWID_BITS}) & 3 = {DEAD_ROW} LIMIT 1"));
    let rowid = found[0].as_int().unwrap() & ((1 << ROWID_BITS) - 1);
    corrupt(&conn, format!("DELETE FROM {partitions} WHERE rowid = {}", found[0].as_int().unwrap()));
    assert!(findings(&conn).contains(&format!("holds a copy of row {rowid} that is neither")));

    let (_db, conn) = setup("ivf-rq-check-centroid.db");
    let found = one(&conn, &format!("SELECT rowid >> 42, c0 FROM {partitions} WHERE (rowid >> {ROWID_BITS}) & 3 = {BLOCK_ROW} LIMIT 1"));
    let (partition, version) = (found[0].as_int().unwrap(), found[1].as_int().unwrap());
    corrupt(&conn, format!("DELETE FROM {centroids} WHERE rowid = {}", centroid_key(partition, version).unwrap()));
    assert!(findings(&conn).contains(&format!("names centroid version {version}, which the index does not hold")));

    let (_db, conn) = setup("ivf-rq-check-orphan.db");
    let location = one(&conn, &format!("SELECT location FROM {vectors} WHERE location >= 0 LIMIT 1"))[0].as_int().unwrap();
    corrupt(&conn, format!("INSERT INTO {vectors} (rowid, location) VALUES (999999, {location})"));
    assert!(findings(&conn).contains("the index holds row 999999, which the table does not"));

    let (_db, conn) = setup("ivf-rq-check-directory.db");
    let partition = one(&conn, &format!("SELECT rowid >> 42 FROM {partitions} WHERE (rowid >> {ROWID_BITS}) & 3 = {BLOCK_ROW} LIMIT 1"))[0].as_int().unwrap();
    corrupt(&conn, format!("DELETE FROM {centroids} WHERE rowid = {}", directory_key(partition).unwrap()));
    assert!(findings(&conn).contains(&format!("partition {partition} has no directory row")));

    let (_db, conn) = setup("ivf-rq-check-directory-state.db");
    let partition = one(&conn, &format!("SELECT rowid >> 42 FROM {partitions} WHERE (rowid >> {ROWID_BITS}) & 3 = {BLOCK_ROW} LIMIT 1"))[0].as_int().unwrap();
    let held = directory(&conn)[&partition];
    let hex: String = Directory { state: State::Retiring, ..held }.record().unwrap().get_value(0).unwrap().to_blob().unwrap().iter().map(|b| format!("{b:02x}")).collect();
    corrupt(&conn, format!("UPDATE {centroids} SET value = X'{hex}' WHERE rowid = {}", directory_key(partition).unwrap()));
    assert!(findings(&conn).contains(&format!("partition {partition}'s directory row says it is Retiring and its bookkeeping Live")));
}

/// An index built in the earlier format, whose model's one generation row stands where no
/// directory row does, is refused with a hint, and REINDEX makes it afresh.
#[test]
fn an_index_of_the_earlier_format_is_refused_until_reindexed() {
    let (_db, conn) = database("ivf-rq-earlier-format.db", false);
    conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e)").unwrap();
    let mut state = 3u64;
    insert_all(&conn, &(1..=50).map(|id| (id, random(&mut state, 8))).collect::<Vec<_>>());
    conn.execute("BEGIN IMMEDIATE").unwrap();
    conn.start_nested();
    let done = conn.execute("INSERT INTO __turso_internal_v_ivf_centroids (rowid, value) VALUES (-1, X'01')");
    conn.end_nested();
    done.unwrap();
    conn.execute("COMMIT").unwrap();
    let q = random(&mut state, 8);
    let sql = format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10", literal(&q));
    let refused = conn.prepare(&sql).and_then(|mut statement| statement.run_collect_rows()).unwrap_err();
    assert!(refused.to_string().contains("REINDEX v_ivf"), "{refused}");
    conn.execute("REINDEX v_ivf").unwrap();
    assert_eq!(rows(&conn, &sql).len(), 10);
    assert!(check(&conn, "v_ivf").unwrap().consistent());
}

/// Step `statement` until it yields between steps of its work, true, or ends, false, sleeping
/// out the waits its busy handler asks for.
fn step_waiting(db: &turso_core::Database, statement: &mut turso_core::Statement) -> bool {
    loop {
        match statement.step().unwrap() {
            turso_core::StepResult::Yield => return true,
            turso_core::StepResult::Done => return false,
            turso_core::StepResult::IO => db.io.step().unwrap(),
            turso_core::StepResult::Sleep { duration } => std::thread::sleep(duration),
            other => panic!("the statement stepped to {other:?}"),
        }
    }
}

/// Under WAL a stepped OPTIMIZE INDEX that finds the write lock held at many of its steps
/// waits for each holder within its connection's busy timeout: the timeout bounds each wait,
/// not their sum over the run.
#[test]
fn a_stepped_optimize_waits_for_each_writer_within_its_busy_timeout() {
    use std::time::{Duration, Instant};
    const DIMS: usize = 16;
    let (db, conn) = database("ivf-rq-optimize-busy-each.db", false);
    let mut state = 229;
    backlog(&conn, &mut state, DIMS);
    STATEMENT_UPKEEP.with(|on| on.set(false));
    for statement in 0..20i64 {
        let more: Vec<(i64, Vec<f32>)> = (1..=100).map(|i| (10_000 + statement * 100 + i, random(&mut state, DIMS))).collect();
        insert_all(&conn, &more);
    }
    STATEMENT_UPKEEP.with(|on| on.set(true));
    conn.set_busy_timeout(Duration::from_millis(100));
    let writer = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
    let mut optimize = conn.prepare("OPTIMIZE INDEX v_ivf").unwrap();
    // The first step only finds the work.
    assert!(step_waiting(&db, &mut optimize));
    let mut holds = 0;
    while holds < 8 {
        // A writer holds the write lock for 30 ms while OPTIMIZE INDEX waits for it.
        writer.execute("BEGIN IMMEDIATE").unwrap();
        let until = Instant::now() + Duration::from_millis(30);
        while let Some(left) = until.checked_duration_since(Instant::now()) {
            match optimize.step().unwrap() {
                turso_core::StepResult::IO => db.io.step().unwrap(),
                turso_core::StepResult::Sleep { duration } => std::thread::sleep(duration.min(left)),
                turso_core::StepResult::Busy => {
                    panic!("OPTIMIZE INDEX gave up on the write lock after {holds} waits of 30 ms, each within its busy timeout of 100 ms")
                }
                other => panic!("OPTIMIZE INDEX stepped to {other:?} while a writer held the write lock"),
            }
        }
        writer.execute("COMMIT").unwrap();
        holds += 1;
        assert!(step_waiting(&db, &mut optimize), "OPTIMIZE INDEX ended after waiting for a writer {holds} times");
    }
    let mut yields = 1 + holds;
    while step_waiting(&db, &mut optimize) {
        yields += 1;
        assert!(yields < 10_000, "OPTIMIZE INDEX yielded ten thousand times, leaving {:?}", stored(&conn).pending());
    }
    let left = stored(&conn).pending();
    assert!(left.is_empty(), "work left after OPTIMIZE INDEX: {left:?}");
}

/// Under WAL a writer refused the write lock while a step of OPTIMIZE INDEX held it takes the
/// lock before later steps do: the step that refused it waits for it, asleep, as long as a
/// busy handler sleeps at most, before taking the lock again. A writer on its own thread
/// inserts beside OPTIMIZE INDEX on another and counts the steps that took the lock while it
/// waited. A wait stepped through at once instead of slept yielded 760,514 times here, past
/// the bound of [`optimize`].
#[test]
fn a_writer_refused_during_a_step_of_optimize_takes_the_lock_before_the_next_steps() {
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    const DIMS: usize = 16;
    let (db, conn) = database("ivf-rq-optimize-turns.db", false);
    let mut state = 239;
    backlog(&conn, &mut state, DIMS);
    // Work for many steps, and inserts on this thread that leave theirs due too.
    STATEMENT_UPKEEP.with(|on| on.set(false));
    for statement in 0..30i64 {
        let more: Vec<(i64, Vec<f32>)> = (1..=100).map(|i| (10_000 + statement * 100 + i, random(&mut state, DIMS))).collect();
        insert_all(&conn, &more);
    }
    conn.set_busy_timeout(Duration::from_secs(60));
    let writer = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
    writer.set_busy_timeout(Duration::from_secs(60));
    let grants = |connection: &Arc<turso_core::Connection>| connection.get_pager().wal.as_ref().unwrap().write_lock_turns().0;
    let rows: Vec<Vec<f32>> = (0..20_000).map(|_| random(&mut state, DIMS)).collect();
    let done = AtomicBool::new(false);
    let ahead: Vec<u64> = std::thread::scope(|scope| {
        let optimizing = scope.spawn(|| {
            optimize(&conn, "v_ivf");
            done.store(true, Ordering::Release);
        });
        let mut ahead = Vec::new();
        for (i, row) in rows.iter().enumerate() {
            if done.load(Ordering::Acquire) {
                break;
            }
            let mut insert = writer.prepare(format!("INSERT INTO v VALUES ({}, {})", 300_000 + i, literal(row))).unwrap();
            let mut refused_at = None;
            loop {
                match insert.step().unwrap() {
                    turso_core::StepResult::Done => break,
                    turso_core::StepResult::IO => db.io.step().unwrap(),
                    turso_core::StepResult::Sleep { duration } => {
                        refused_at.get_or_insert_with(|| grants(&writer));
                        std::thread::sleep(duration);
                    }
                    other => panic!("the writer's insert stepped to {other:?}"),
                }
            }
            // Its own is the last grant counted, give or take one step taken right after it.
            if let Some(from) = refused_at {
                ahead.push(grants(&writer) - from - 1);
            }
            // A writer that leaves the lock free between its transactions, as one at a steady
            // rate does.
            std::thread::sleep(Duration::from_millis(2));
        }
        optimizing.join().unwrap();
        ahead
    });
    STATEMENT_UPKEEP.with(|on| on.set(true));
    let most = ahead.iter().copied().max().unwrap_or(0);
    assert!(most <= 2, "a refused writer waited while {most} steps took the write lock: {ahead:?}");
    assert!(ahead.len() >= 5, "the writer was refused the write lock {} times", ahead.len());
}

/// Under MVCC a step of OPTIMIZE INDEX takes work no other transaction holds: with a writer's
/// transaction open over the sixteen lowest-numbered partitions with work due, OPTIMIZE INDEX
/// packs every tail outside them, and after the writer commits the next does the rest. Steps
/// that looked only at those sixteen found no job they could do and ended the run.
#[test]
fn optimize_works_around_the_partitions_an_open_transaction_holds() {
    const DIMS: usize = 16;
    let (db, conn) = database("ivf-rq-optimize-around.db", true);
    let mut state = 251;
    let mut live = backlog(&conn, &mut state, DIMS);
    let index = stored(&conn);
    let mut due: Vec<i64> = index
        .pending()
        .iter()
        .filter_map(|work| {
            let rest = work.strip_prefix("partition ")?;
            rest.split(' ').next()?.parse().ok()
        })
        .collect();
    due.sort_unstable();
    due.dedup();
    let held: Vec<i64> = due.into_iter().take(16).collect();
    assert!(held.len() == 16 && held.iter().all(|p| index.part(*p).state == State::Live), "the backlog left {held:?} due");
    // Each held partition's current centroid, which a row equal to it is coded beside.
    let mut centroids: HashMap<i64, (i64, Vec<f32>)> = HashMap::new();
    for row in backing(&conn, "centroids", "value") {
        let key = row[0].as_int().unwrap();
        if key < 0 {
            continue;
        }
        let (p, version) = (key >> VERSION_BITS, key & ((1 << VERSION_BITS) - 1));
        let v: Vec<f32> = row[1].to_blob().unwrap().chunks_exact(4).map(|c| f32::from_le_bytes(c.try_into().unwrap())).collect();
        if centroids.get(&p).is_none_or(|(at, _)| *at < version) {
            centroids.insert(p, (version, v));
        }
    }
    let rows: Vec<(i64, Vec<f32>)> = held.iter().enumerate().map(|(i, p)| (200_000 + i as i64, centroids[p].1.clone())).collect();
    let writer = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
    writer.execute("BEGIN CONCURRENT").unwrap();
    STATEMENT_UPKEEP.with(|on| on.set(false));
    insert_all(&writer, &rows);
    STATEMENT_UPKEEP.with(|on| on.set(true));
    optimize(&conn, "v_ivf");
    let index = stored(&conn);
    let unpacked: Vec<(i64, usize)> = index.live().into_iter().filter(|p| !held.contains(p) && index.tail(*p) >= PACK_AT).map(|p| (p, index.tail(p))).collect();
    assert!(unpacked.is_empty(), "tails left unpacked outside the held partitions: {unpacked:?}");
    writer.execute("COMMIT").unwrap();
    live.extend(rows);
    optimize(&conn, "v_ivf");
    let left = stored(&conn).pending();
    assert!(left.is_empty(), "work left after the writer committed: {left:?}");
    for _ in 0..3 {
        let q = random(&mut state, DIMS);
        assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q));
    }
}

/// Inside an explicit transaction OPTIMIZE INDEX does all its work in that transaction, whether
/// it was prepared there or before it began: another connection sees the backlog until the
/// transaction commits, and none of it after.
#[test]
fn optimize_inside_a_transaction_does_its_work_there() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        for prepared_before in [false, true] {
            let (db, conn) = database(&format!("ivf-rq-optimize-in-tx-{mvcc}-{prepared_before}.db"), mvcc);
            let mut state = 227;
            let live = backlog(&conn, &mut state, DIMS);
            let other = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
            let early = prepared_before.then(|| conn.prepare("OPTIMIZE INDEX v_ivf").unwrap());
            conn.execute(if mvcc { "BEGIN CONCURRENT" } else { "BEGIN" }).unwrap();
            match early {
                Some(mut statement) => statement.run_ignore_rows().unwrap(),
                None => optimize(&conn, "v_ivf"),
            }
            let case = format!("mvcc={mvcc}, prepared before the transaction={prepared_before}");
            assert!(!conn.get_auto_commit(), "{case}: OPTIMIZE INDEX ended the transaction");
            let left = stored(&conn).pending();
            assert!(left.is_empty(), "{case}: work left inside the transaction: {left:?}");
            assert!(!stored(&other).pending().is_empty(), "{case}: another connection saw the work before the commit");
            conn.execute("COMMIT").unwrap();
            let left = stored(&other).pending();
            assert!(left.is_empty(), "{case}: work left after the commit: {left:?}");
            for _ in 0..3 {
                let q = random(&mut state, DIMS);
                assert_eq!(top_ten(&other, &q), exact_top_ten(&live, &q), "{case}");
            }
        }
    }
}

/// An OPTIMIZE INDEX dropped between its steps leaves its connection outside any transaction,
/// another connection free to write and the steps it committed in place, and the next one
/// finishes the work.
#[test]
fn an_optimize_dropped_between_steps_leaves_the_rest_to_the_next() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (db, conn) = database(&format!("ivf-rq-optimize-dropped-{mvcc}.db"), mvcc);
        let mut state = 229;
        let mut live = backlog(&conn, &mut state, DIMS);
        let mut first = conn.prepare("OPTIMIZE INDEX v_ivf").unwrap();
        assert!(step_to_yield(&db, &mut first), "mvcc={mvcc}: OPTIMIZE INDEX did all its work in one step");
        drop(first);
        assert!(conn.get_auto_commit(), "mvcc={mvcc}: the dropped OPTIMIZE INDEX left its connection in a transaction");
        let writer = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
        let row = (100_001, random(&mut state, DIMS));
        insert_all(&writer, std::slice::from_ref(&row));
        live.insert(row.0, row.1);
        optimize(&conn, "v_ivf");
        let left = stored(&conn).pending();
        assert!(left.is_empty(), "mvcc={mvcc}: work left after the next OPTIMIZE INDEX: {left:?}");
        for _ in 0..3 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
    }
}

/// Under MVCC concurrent writers whose statements maintain the index all commit, each
/// inserting rows near two centres and deleting older ones, their statements interleaved in a
/// random order: a maintenance job leaves for later a partition another transaction holds or
/// has committed past its snapshot, where its own write there would lose at commit. With the
/// jobs claiming every partition, a writer's COMMIT failed.
#[test]
fn concurrent_writers_maintaining_the_index_all_commit() {
    const DIMS: usize = 16;
    const WRITERS: usize = 3;
    let (db, conn) = database("ivf-rq-concurrent-writers.db", true);
    let mut state = 233;
    let centres: Vec<Vec<f32>> = (0..4).map(|_| random(&mut state, DIMS).iter().map(|x| 3.0 * x).collect()).collect();
    let mut live: HashMap<i64, Vec<f32>> = (1..=3000i64).map(|id| (id, near(&mut state, &centres, 0.4))).collect();
    let mut vectors: Vec<(i64, Vec<f32>)> = live.iter().map(|(id, v)| (*id, v.clone())).collect();
    vectors.sort_by_key(|(id, _)| *id);
    insert_all(&conn, &vectors);
    conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
    let writers: Vec<Arc<turso_core::Connection>> = (0..WRITERS).map(|_| db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap()).collect();
    let insert = |rows: &[(i64, Vec<f32>)]| format!("INSERT INTO v VALUES {}", rows.iter().map(|(id, v)| format!("({id}, {})", literal(v))).collect::<Vec<_>>().join(","));
    // Each writer's open transaction: the rows it inserted, the rows it deleted, its statements.
    type Open = Option<(Vec<(i64, Vec<f32>)>, Vec<i64>, Interleaved)>;
    let mut open: Vec<Open> = (0..WRITERS).map(|_| None).collect();
    let (mut next, mut committed) = (3000i64, 0usize);
    // Each transaction also deletes rows of the first 3,000, each once, so blocks gather stale
    // copies and compactions come due beside the packs.
    let mut doomed: Vec<i64> = (1..=3000).collect();
    while committed < 120 || open.iter().any(Option::is_some) {
        let w = ((random(&mut state, 1)[0] + 0.5) * WRITERS as f32) as usize % WRITERS;
        if open[w].is_none() {
            if committed >= 120 {
                continue;
            }
            // Rows near two of the centres, so the writers keep landing in the same partitions.
            let rows: Vec<(i64, Vec<f32>)> = (0..8)
                .map(|_| {
                    next += 1;
                    (next, near(&mut state, &centres[..2], 0.05))
                })
                .collect();
            let gone: Vec<i64> = (0..8).map(|_| doomed.swap_remove(((random(&mut state, 1)[0] + 0.5) * doomed.len() as f32) as usize % doomed.len())).collect();
            let delete = |ids: &[i64]| format!("DELETE FROM v WHERE id IN ({})", ids.iter().map(i64::to_string).collect::<Vec<_>>().join(","));
            let sql = ["BEGIN CONCURRENT".to_string(), insert(&rows[..4]), delete(&gone[..4]), insert(&rows[4..]), delete(&gone[4..]), "COMMIT".to_string()];
            open[w] = Some((rows, gone, Interleaved::new(&writers[w], &sql)));
        }
        let (rows, gone, transaction) = open[w].as_mut().expect("opened above");
        if transaction.turn(&db) {
            live.extend(rows.drain(..));
            for id in gone.drain(..) {
                live.remove(&id);
            }
            open[w] = None;
            committed += 1;
        }
    }
    let found = check(&conn, "v_ivf").unwrap();
    assert!(found.consistent(), "{found:?}");
    optimize(&conn, "v_ivf");
    let left = stored(&conn).pending();
    assert!(left.is_empty(), "work left after OPTIMIZE INDEX: {left:?}");
    for _ in 0..3 {
        let q = near(&mut state, &centres[..2], 0.05);
        assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q));
    }
}

/// Each vector's location as its vectors row records it.
fn locations(conn: &Arc<turso_core::Connection>) -> HashMap<i64, i64> {
    backing(conn, "vectors", "location").iter().map(|r| (r[0].as_int().unwrap(), r[1].as_int().unwrap())).collect()
}

/// Under MVCC, a transaction deletes and updates vectors whose codes maintenance moved after its
/// snapshot: OPTIMIZE INDEX packs, splits and moves the backlog while a `BEGIN CONCURRENT`
/// transaction holds an earlier snapshot, and the transaction's writes to the moved vectors then
/// commit, as writes to rows no other transaction wrote do; a query's held redo rows, read before
/// them, serve no snapshot after their commit. The maintenance each of its
/// statements does passes over the redo rows the transaction wrote, so each one-row update reads
/// a bounded number of rows by key, however many its transaction left to maintenance.
#[test]
fn deletes_and_updates_of_vectors_maintenance_moved_after_the_snapshot_commit() {
    const DIMS: usize = 16;
    let (db, conn) = database("ivf-rq-moved-after-snapshot.db", true);
    let mut state = 239;
    let mut live = backlog(&conn, &mut state, DIMS);
    let writer = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
    writer.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(rows(&writer, "SELECT count(*) FROM v")[0][0].as_int(), Some(live.len() as i64));
    let before = locations(&conn);
    optimize(&conn, "v_ivf");
    // A query holds the redo rows, none yet, under a snapshot past OPTIMIZE INDEX's: the writes
    // below must keep them from serving any later snapshot.
    let q = random(&mut state, DIMS);
    assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q));
    let after = locations(&conn);
    let mut moved: Vec<i64> = before.iter().filter(|(id, at)| after.get(id) != Some(at)).map(|(id, _)| *id).collect();
    moved.sort_unstable();
    assert!(moved.len() >= 200, "OPTIMIZE INDEX moved only {} vectors", moved.len());
    let (deleted, updated) = moved.split_at(moved.len() / 2);
    let ids = |ids: &[i64]| ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",");
    writer.execute(format!("DELETE FROM v WHERE id IN ({})", ids(deleted))).unwrap();
    for id in updated {
        let v = random(&mut state, DIMS);
        let before = POINT_READS.with(|count| count.get());
        writer.execute(format!("UPDATE v SET e = {} WHERE id = {id}", literal(&v))).unwrap();
        let reads = POINT_READS.with(|count| count.get()) - before;
        assert!(reads < 400, "a one-row update of a transaction with {} rows left to maintenance read {reads} rows by key", deleted.len());
        live.insert(*id, v);
    }
    writer.execute("COMMIT").unwrap();
    for id in deleted {
        live.remove(id);
    }
    let found = check(&conn, "v_ivf").unwrap();
    assert!(found.consistent(), "{found:?}");
    for _ in 0..5 {
        let q = random(&mut state, DIMS);
        assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q));
    }
    optimize(&conn, "v_ivf");
    let found = check(&conn, "v_ivf").unwrap();
    assert!(found.consistent(), "{found:?}");
    let left = stored(&conn).pending();
    assert!(left.is_empty(), "work left after OPTIMIZE INDEX: {left:?}");
    for _ in 0..5 {
        let q = random(&mut state, DIMS);
        assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q));
    }
}

/// Write the first redo row of each of `rowids` in a nested statement of its own transaction,
/// as a write that meets maintenance writes it, without changing the rows.
fn leave_to_maintenance(conn: &Arc<turso_core::Connection>, mvcc: bool, index: &str, rowids: &[i64]) {
    conn.execute(if mvcc { "BEGIN CONCURRENT" } else { "BEGIN IMMEDIATE" }).unwrap();
    conn.start_nested();
    let values: Vec<String> = rowids.iter().map(|r| format!("({}, 0)", r << REDO_BITS)).collect();
    let done = conn.execute(format!("INSERT INTO __turso_internal_{index}_redo(rowid, writer) VALUES {}", values.join(",")));
    conn.end_nested();
    done.unwrap();
    conn.execute("COMMIT").unwrap();
}

/// Rows with redo rows are maintenance's to code afresh. Until a settle does, a query passes over
/// any code they have and reads their vectors from the table, and deletes and updates of them add
/// redo rows rather than writing codes; a settle then codes each from its table row. Under WAL
/// and MVCC, with settles held while the rows are written and through one OPTIMIZE INDEX, so the
/// other jobs meet them.
#[test]
fn rows_left_to_maintenance_are_answered_from_their_table_rows_until_settled() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-left-to-maintenance-{mvcc}.db"), mvcc);
        let mut state = 241;
        let mut live = backlog(&conn, &mut state, DIMS);
        // Rows of the tails the backlog left and of the blocks before it.
        let chosen: Vec<i64> = (3001..=3060).chain((1..=600).step_by(10)).collect();
        leave_to_maintenance(&conn, mvcc, "v_ivf", &chosen);
        SETTLE.with(|on| on.set(false));
        let (deleted, updated) = chosen.split_at(chosen.len() / 2);
        let ids = |ids: &[i64]| ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",");
        // The first deleted row's old vector, at eight times its length nearer it than any other.
        let (gone, moved) = (deleted[0], updated[0]);
        let at_gone: Vec<f32> = live[&gone].iter().map(|x| 8.0 * x).collect();
        conn.execute(format!("DELETE FROM v WHERE id IN ({})", ids(deleted))).unwrap();
        for id in deleted {
            live.remove(id);
        }
        for id in updated {
            // The first updated row moves far from every other.
            let scale = if *id == moved { 8.0 } else { 1.0 };
            let v: Vec<f32> = random(&mut state, DIMS).iter().map(|x| scale * x).collect();
            conn.execute(format!("UPDATE v SET e = {} WHERE id = {id}", literal(&v))).unwrap();
            live.insert(*id, v);
        }
        let found = check(&conn, "v_ivf").unwrap();
        assert!(found.consistent() && found.pending == chosen.len() as u64, "mvcc={mvcc}: {found:?}");
        // A delete adds one, an update one for its delete and one for its insert.
        assert_eq!(stored(&conn).redo, chosen.len() + deleted.len() + 2 * updated.len(), "mvcc={mvcc}: the redo rows the writes added");
        for _ in 0..5 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
        // A query at an updated row's new vector finds it first, and one at a deleted row's old
        // vector does not find it.
        let q = live[&moved].clone();
        assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        assert_eq!(top_ten(&conn, &q)[0], moved, "mvcc={mvcc}");
        assert_eq!(top_ten(&conn, &at_gone), exact_top_ten(&live, &at_gone), "mvcc={mvcc}");
        assert!(!top_ten(&conn, &at_gone).contains(&gone), "mvcc={mvcc}");
        optimize(&conn, "v_ivf");
        let found = check(&conn, "v_ivf").unwrap();
        assert!(found.consistent() && found.pending == chosen.len() as u64, "mvcc={mvcc}: {found:?}");
        for _ in 0..5 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
        SETTLE.with(|on| on.set(true));
        optimize(&conn, "v_ivf");
        let found = check(&conn, "v_ivf").unwrap();
        assert!(found.consistent() && found.pending == 0 && found.codes as usize == live.len(), "mvcc={mvcc}: {found:?}");
        let left = stored(&conn).pending();
        assert!(left.is_empty(), "mvcc={mvcc}: work left after OPTIMIZE INDEX: {left:?}");
        for _ in 0..5 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
    }
}

/// A job that codes a vector afresh and finds its row deleted since, the row left to maintenance,
/// kills its code: the rows of one partition, forty of them deleted while left to maintenance,
/// grow past the split bound, and the split's moves recode its entries.
#[test]
fn a_job_finding_a_row_left_to_maintenance_gone_kills_its_code() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-orphan-{mvcc}.db"), mvcc);
        let mut state = 257;
        let rows: Vec<(i64, Vec<f32>)> = (1..=300i64).map(|id| (id, random(&mut state, DIMS))).collect();
        insert_all(&conn, &rows);
        STATEMENT_UPKEEP.with(|on| on.set(false));
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let mut live: HashMap<i64, Vec<f32>> = rows.into_iter().collect();
        let chosen: Vec<i64> = (1..=40).collect();
        leave_to_maintenance(&conn, mvcc, "v_ivf", &chosen);
        conn.execute("DELETE FROM v WHERE id <= 40").unwrap();
        for id in &chosen {
            live.remove(id);
        }
        STATEMENT_UPKEEP.with(|on| on.set(true));
        SETTLE.with(|on| on.set(false));
        KILLS.with(|count| count.set(0));
        optimize(&conn, "v_ivf");
        let index = stored(&conn);
        assert!(index.live().len() > 1, "mvcc={mvcc}: the partition did not split");
        assert_eq!(KILLS.with(|count| count.get()), chosen.len(), "mvcc={mvcc}: the codes killed");
        let found = check(&conn, "v_ivf").unwrap();
        assert!(found.consistent() && found.pending == chosen.len() as u64, "mvcc={mvcc}: {found:?}");
        for _ in 0..3 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
        SETTLE.with(|on| on.set(true));
        optimize(&conn, "v_ivf");
        let found = check(&conn, "v_ivf").unwrap();
        assert!(found.consistent() && found.pending == 0 && found.codes as usize == live.len(), "mvcc={mvcc}: {found:?}");
    }
}

/// OPTIMIZE INDEX settles every row left to maintenance before it ends, however many its steps
/// take: 1,100 rowids, more than one step settles.
#[test]
fn optimize_settles_every_row_left_to_maintenance() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-settle-all-{mvcc}.db"), mvcc);
        let mut state = 263;
        let rows: Vec<(i64, Vec<f32>)> = (1..=1500i64).map(|id| (id, random(&mut state, DIMS))).collect();
        insert_all(&conn, &rows);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        optimize(&conn, "v_ivf");
        let live: HashMap<i64, Vec<f32>> = rows.into_iter().collect();
        let chosen: Vec<i64> = (1..=1100).collect();
        leave_to_maintenance(&conn, mvcc, "v_ivf", &chosen);
        assert_eq!(check(&conn, "v_ivf").unwrap().pending, chosen.len() as u64, "mvcc={mvcc}");
        optimize(&conn, "v_ivf");
        let found = check(&conn, "v_ivf").unwrap();
        assert!(found.consistent() && found.pending == 0 && found.codes as usize == live.len(), "mvcc={mvcc}: {found:?}");
        for _ in 0..3 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
    }
}

/// OPTIMIZE INDEX with nothing due and no redo row ends at its first step, which only reads: under
/// WAL beside a transaction holding the write lock it ends without waiting for it.
#[test]
fn optimize_with_nothing_due_ends_at_its_first_step() {
    const DIMS: usize = 16;
    let (db, conn) = database("ivf-rq-nothing-due.db", false);
    let mut state = 269;
    let rows: Vec<(i64, Vec<f32>)> = (1..=300i64).map(|id| (id, random(&mut state, DIMS))).collect();
    insert_all(&conn, &rows);
    conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
    optimize(&conn, "v_ivf");
    let writer = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
    writer.execute("BEGIN IMMEDIATE").unwrap();
    let mut optimize = conn.prepare("OPTIMIZE INDEX v_ivf").unwrap();
    assert!(!step_to_yield(&db, &mut optimize), "OPTIMIZE INDEX with nothing due went on past its first step");
    writer.execute("ROLLBACK").unwrap();
}

/// A rebuild codes every vector but those of rows with redo rows, whose codes and vectors' rows it
/// removes for their settle to code afresh: a statement inserting enough rows into an index of one
/// partition rebuilds it while forty rows, twenty since deleted, are left to maintenance.
#[test]
fn a_rebuild_leaves_rows_with_redo_rows_to_their_settle() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-rebuild-left-{mvcc}.db"), mvcc);
        let mut state = 251;
        let rows: Vec<(i64, Vec<f32>)> = (1..=300i64).map(|id| (id, random(&mut state, DIMS))).collect();
        insert_all(&conn, &rows);
        // Unmaintained, the index holds every row in one partition's tail.
        STATEMENT_UPKEEP.with(|on| on.set(false));
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        STATEMENT_UPKEEP.with(|on| on.set(true));
        let mut live: HashMap<i64, Vec<f32>> = rows.into_iter().collect();
        let chosen: Vec<i64> = (1..=40).collect();
        leave_to_maintenance(&conn, mvcc, "v_ivf", &chosen);
        SETTLE.with(|on| on.set(false));
        KILLS.with(|count| count.set(0));
        conn.execute("DELETE FROM v WHERE id <= 20").unwrap();
        for id in 1..=20 {
            live.remove(&id);
        }
        let partitions = stored(&conn).live().len();
        let more: Vec<(i64, Vec<f32>)> = (301..=1500i64).map(|id| (id, random(&mut state, DIMS))).collect();
        insert_all(&conn, &more);
        live.extend(more);
        let rebuilt = stored(&conn).live().len();
        assert!(partitions == 1 && rebuilt >= trained_partitions(live.len()) / 2, "mvcc={mvcc}: {partitions} partitions became {rebuilt}");
        assert_eq!(KILLS.with(|count| count.get()), chosen.len(), "mvcc={mvcc}: the codes killed");
        let found = check(&conn, "v_ivf").unwrap();
        assert!(found.consistent() && found.pending == chosen.len() as u64, "mvcc={mvcc}: {found:?}");
        for _ in 0..3 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
        SETTLE.with(|on| on.set(true));
        optimize(&conn, "v_ivf");
        let found = check(&conn, "v_ivf").unwrap();
        assert!(found.consistent() && found.pending == 0 && found.codes as usize == live.len(), "mvcc={mvcc}: {found:?}");
        for _ in 0..3 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
    }
}

/// A filtered query gives out rows left to maintenance as it gives out any other, each round
/// reading their vectors from the table: rows of the kept tag left to maintenance and moved near
/// the query lead its answer, and those deleted are not in it.
#[test]
fn a_filtered_query_reads_rows_left_to_maintenance_from_the_table() {
    for mvcc in [false, true] {
        let (_db, conn, mut rows) = tagged(&format!("ivf-rq-filtered-left-{mvcc}.db"), mvcc);
        let mut state = 83;
        let q = random(&mut state, 16);
        let chosen: Vec<i64> = (3..=600).step_by(10).collect();
        leave_to_maintenance(&conn, mvcc, "f_ivf", &chosen);
        SETTLE.with(|on| on.set(false));
        let (deleted, moved) = chosen.split_at(chosen.len() / 2);
        for id in deleted {
            conn.execute(format!("DELETE FROM f WHERE id = {id}")).unwrap();
        }
        let near: Vec<f32> = q.iter().map(|x| 2.0 * x).collect();
        for id in moved {
            conn.execute(format!("UPDATE f SET e = {} WHERE id = {id}", literal(&near))).unwrap();
        }
        rows.retain(|(id, _)| !deleted.contains(id));
        for (id, v) in rows.iter_mut() {
            if moved.contains(id) {
                *v = near.clone();
            }
        }
        let sql = format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM f WHERE tag = 3 ORDER BY distance LIMIT 10", literal(&q));
        let got = ids(&conn, &sql);
        assert_eq!(got, exact_filtered(&rows, &q, |id| id % 10 == 3), "mvcc={mvcc}");
        assert!(got.iter().all(|id| moved.contains(id)), "mvcc={mvcc}: {got:?}");
        SETTLE.with(|on| on.set(true));
        optimize(&conn, "f_ivf");
        assert_eq!(ids(&conn, &sql), exact_filtered(&rows, &q, |id| id % 10 == 3), "mvcc={mvcc}");
        let found = check(&conn, "f_ivf").unwrap();
        assert!(found.consistent() && found.pending == 0, "mvcc={mvcc}: {found:?}");
    }
}

/// Under MVCC a writer's open transaction keeps no maintenance off the partitions it wrote: beside
/// one that has inserted a thousand rows across the backlog's partitions, OPTIMIZE INDEX does all
/// the work due in them, and the writer then commits.
#[test]
fn maintenance_works_the_partitions_an_open_writer_wrote() {
    const DIMS: usize = 16;
    let (db, conn) = database("ivf-rq-beside-an-open-writer.db", true);
    let mut state = 271;
    let mut live = backlog(&conn, &mut state, DIMS);
    let rows: Vec<(i64, Vec<f32>)> = (1..=1000i64).map(|i| (300_000 + i, random(&mut state, DIMS))).collect();
    let writer = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
    writer.execute("BEGIN CONCURRENT").unwrap();
    STATEMENT_UPKEEP.with(|on| on.set(false));
    insert_all(&writer, &rows);
    STATEMENT_UPKEEP.with(|on| on.set(true));
    assert!(!stored(&conn).pending().is_empty(), "the backlog left no work due");
    optimize(&conn, "v_ivf");
    let left = stored(&conn).pending();
    assert!(left.is_empty(), "work left beside the open writer: {left:?}");
    writer.execute("COMMIT").unwrap();
    live.extend(rows);
    let found = check(&conn, "v_ivf").unwrap();
    assert!(found.consistent() && found.rows as usize == live.len(), "{found:?}");
    optimize(&conn, "v_ivf");
    for _ in 0..3 {
        let q = random(&mut state, DIMS);
        assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q));
    }
}

/// OPTIMIZE INDEX in steps rebuilds an index holding four times the vectors its partitions were
/// trained for: its first step's survey of every partition finds the rebuild due, and the next
/// step makes it, under WAL and MVCC, removing a mark on a slot the rebuild leaves vacant.
#[test]
fn optimize_in_steps_rebuilds_an_index_its_rows_outgrew() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-steps-rebuild-{mvcc}.db"), mvcc);
        let mut state = 277;
        let rows: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
        // Unmaintained, the index holds every row in one partition's tail.
        STATEMENT_UPKEEP.with(|on| on.set(false));
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        insert_all(&conn, &rows);
        STATEMENT_UPKEEP.with(|on| on.set(true));
        assert_eq!(stored(&conn).live().len(), 1, "mvcc={mvcc}");
        conn.execute(if mvcc { "BEGIN CONCURRENT" } else { "BEGIN IMMEDIATE" }).unwrap();
        conn.start_nested();
        let planted = conn.execute(format!("INSERT INTO __turso_internal_v_ivf_partitions (rowid, c0, c1, c2, c3, c4, c5, c6, c7) VALUES ({}, 0, 0, 0, 0, 0, 0, 0, 0)", mark_key(900, 901).unwrap()));
        conn.end_nested();
        planted.unwrap();
        conn.execute("COMMIT").unwrap();
        assert_eq!(check(&conn, "v_ivf").unwrap().marks, 1, "mvcc={mvcc}");
        REBUILT.with(|count| count.set(0));
        optimize(&conn, "v_ivf");
        assert_eq!(REBUILT.with(|count| count.get()), 1, "mvcc={mvcc}: the rebuilds OPTIMIZE INDEX made");
        let index = stored(&conn);
        assert!(index.live().len() >= trained_partitions(3000) / 2 && index.pending().is_empty(), "mvcc={mvcc}: {} partitions, {:?}", index.live().len(), index.pending());
        let live: HashMap<i64, Vec<f32>> = rows.into_iter().collect();
        let found = check(&conn, "v_ivf").unwrap();
        assert!(found.consistent() && found.codes as usize == live.len() && found.marks == 0, "mvcc={mvcc}: {found:?}");
        for _ in 0..3 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
    }
}

/// One connection of [`a_random_workload_keeps_every_answer_exact`]: whether it has a
/// transaction open, the committed rows that transaction's first statement found, and its own
/// writes since, each row's vector or none for a row it deleted.
struct Session {
    connection: Arc<turso_core::Connection>,
    open: bool,
    snapshot: Option<Arc<HashMap<i64, Vec<f32>>>>,
    pending: HashMap<i64, Option<Vec<f32>>>,
    /// Whether its transaction has done anything but insert.
    changed: bool,
}

/// A row a statement writes: its vector, or none for a row it deletes.
type Write = (i64, Option<Vec<f32>>);

impl Session {
    /// The rows this connection's next statement sees: its transaction's snapshot, taken now
    /// if this is its first statement, with its own writes over it.
    fn view(&mut self, committed: &Arc<HashMap<i64, Vec<f32>>>) -> HashMap<i64, Vec<f32>> {
        let snapshot = self.snapshot.get_or_insert_with(|| committed.clone());
        let mut view = (**snapshot).clone();
        for (id, row) in &self.pending {
            match row {
                Some(v) => view.insert(*id, v.clone()),
                None => view.remove(id),
            };
        }
        view
    }

    fn end(&mut self) {
        self.open = false;
        self.snapshot = None;
        self.pending.clear();
        self.changed = false;
    }
}

/// Random workloads over three connections and a fourth running OPTIMIZE INDEX in steps keep
/// every answer exact, under WAL and under MVCC: transactions and autocommit statements insert,
/// delete and update rows, commit or roll back, and query, their statements interleaved in a
/// random order with OPTIMIZE INDEX's steps. Each query's answer is the exact top ten of the rows
/// its snapshot holds and its transaction's own writes; a transaction that only inserts commits;
/// and once every transaction has ended, one more OPTIMIZE INDEX leaves no work due.
/// IVF_RQ_WORKLOAD_SEEDS (default 2) sets how many seeds each mode runs.
#[test]
fn a_random_workload_keeps_every_answer_exact() {
    const DIMS: usize = 8;
    let seeds: u64 = std::env::var("IVF_RQ_WORKLOAD_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
    for mvcc in [false, true] {
        for seed in 0..seeds {
            let case = format!("mvcc={mvcc}, seed={seed}");
            let (db, conn) = database(&format!("ivf-rq-random-{mvcc}-{seed}.db"), mvcc);
            if let Some(store) = db.get_mv_store().as_ref() {
                // A commit then never waits on another's in flight, so each ends in its turn.
                store.set_group_commit_enabled(false);
            }
            let mut state = 241 + seed;
            let centres: Vec<Vec<f32>> = (0..6).map(|_| random(&mut state, DIMS).iter().map(|x| 3.0 * x).collect()).collect();
            let rows: Vec<(i64, Vec<f32>)> = (1..=1500i64).map(|id| (id, near(&mut state, &centres, 0.3))).collect();
            insert_all(&conn, &rows);
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
            let mut committed: Arc<HashMap<i64, Vec<f32>>> = Arc::new(rows.into_iter().collect());
            let mut sessions: Vec<Session> = (0..3)
                .map(|_| Session { connection: db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap(), open: false, snapshot: None, pending: HashMap::new(), changed: false })
                .collect();
            let maintainer = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
            let mut optimizing: Option<Interleaved> = None;
            let (mut next, mut queries) = (100_000i64, 0usize);
            let pick = |state: &mut u64, n: usize| ((random(state, 1)[0] + 0.5) * n as f32) as usize % n;
            for _ in 0..600 {
                let roll = pick(&mut state, 100);
                if roll < 15 {
                    let running = optimizing.get_or_insert_with(|| Interleaved::new(&maintainer, &["OPTIMIZE INDEX v_ivf".to_string()]));
                    if running.turn(&db) {
                        optimizing = None;
                    }
                    continue;
                }
                let s = pick(&mut state, sessions.len());
                let session = &mut sessions[s];
                let in_transaction = session.open;
                let begin = if mvcc { "BEGIN CONCURRENT" } else { "BEGIN" };
                let first = session.snapshot.is_none();
                let view = session.view(&committed);
                let ids: Vec<i64> = view.keys().copied().collect();
                let (sql, writes): (String, Vec<Write>) = match roll {
                    15..30 if !in_transaction => (begin.to_string(), Vec::new()),
                    30..45 if in_transaction => ("COMMIT".to_string(), Vec::new()),
                    45..50 if in_transaction => ("ROLLBACK".to_string(), Vec::new()),
                    50..70 => {
                        let rows: Vec<(i64, Vec<f32>)> = (0..1 + pick(&mut state, 6))
                            .map(|_| {
                                next += 1;
                                (next, near(&mut state, &centres, 0.3))
                            })
                            .collect();
                        let values: Vec<String> = rows.iter().map(|(id, v)| format!("({id}, {})", literal(v))).collect();
                        (format!("INSERT INTO v VALUES {}", values.join(",")), rows.into_iter().map(|(id, v)| (id, Some(v))).collect())
                    }
                    70..80 if !ids.is_empty() => {
                        let id = ids[pick(&mut state, ids.len())];
                        (format!("DELETE FROM v WHERE id = {id}"), vec![(id, None)])
                    }
                    80..88 if !ids.is_empty() => {
                        let (id, v) = (ids[pick(&mut state, ids.len())], near(&mut state, &centres, 0.3));
                        (format!("UPDATE v SET e = {} WHERE id = {id}", literal(&v)), vec![(id, Some(v))])
                    }
                    _ => {
                        let q = near(&mut state, &centres, 0.3);
                        let got = top_ten(&session.connection, &q);
                        assert_eq!(got, exact_top_ten(&view, &q), "{case}: a query's answer");
                        queries += 1;
                        if !in_transaction {
                            session.end();
                        }
                        continue;
                    }
                };
                let only_inserts = sql.starts_with("COMMIT") && !session.changed;
                match session.connection.execute(&sql) {
                    Ok(()) => {
                        if sql == begin {
                            session.open = true;
                            session.snapshot = None;
                            continue;
                        }
                        if !sql.starts_with("INSERT") && !writes.is_empty() {
                            session.changed = true;
                        }
                        session.pending.extend(writes);
                        if sql == "ROLLBACK" {
                            session.end();
                        } else if sql == "COMMIT" || !in_transaction {
                            let after = Arc::make_mut(&mut committed);
                            for (id, row) in session.pending.drain() {
                                match row {
                                    Some(v) => after.insert(id, v),
                                    None => after.remove(&id),
                                };
                            }
                            session.end();
                        }
                    }
                    // Under WAL another transaction holds the write lock: nothing happened, and a
                    // transaction's first statement took no snapshot.
                    Err(LimboError::Busy) if session.connection.get_auto_commit() != session.open => {
                        if !in_transaction {
                            session.end();
                        } else if first {
                            session.snapshot = None;
                        }
                    }
                    // A write lost to another transaction's, or under WAL found its snapshot
                    // stale: the transaction is over, rolled back by the engine or here.
                    Err(e) => {
                        assert!(matches!(e, LimboError::WriteWriteConflict | LimboError::BusySnapshot | LimboError::Busy), "{case}: {sql}: {e}");
                        assert!(!only_inserts, "{case}: a transaction that only inserted failed to commit: {e}");
                        if !session.connection.get_auto_commit() {
                            session.connection.execute("ROLLBACK").unwrap();
                        }
                        session.end();
                    }
                }
            }
            while let Some(optimize) = optimizing.as_mut() {
                if optimize.turn(&db) {
                    optimizing = None;
                }
            }
            for session in &mut sessions {
                if session.open {
                    session.connection.execute("ROLLBACK").unwrap();
                    session.end();
                }
            }
            assert!(queries > 50, "{case}: only {queries} queries");
            let found = check(&conn, "v_ivf").unwrap();
            assert!(found.consistent() && found.rows as usize == committed.len(), "{case}: before OPTIMIZE INDEX: {found:?}");
            optimize(&conn, "v_ivf");
            let found = check(&conn, "v_ivf").unwrap();
            assert!(found.consistent() && found.codes == found.rows, "{case}: after OPTIMIZE INDEX: {found:?}");
            let left = stored(&conn).pending();
            assert!(left.is_empty(), "{case}: work left after OPTIMIZE INDEX: {left:?}");
            for _ in 0..5 {
                let q = near(&mut state, &centres, 0.3);
                assert_eq!(top_ten(&conn, &q), exact_top_ten(&committed, &q), "{case}: after the workload");
            }
        }
    }
}

/// The rows maintenance surveys while it works off a backlog grow with the backlog, not with
/// its square: doubling the rows one statement inserts less than triples them.
#[test]
fn maintenance_reads_grow_linearly_with_the_backlog() {
    const DIMS: usize = 16;
    let mut reads = Vec::new();
    for count in [1500i64, 3000] {
        let (_db, conn) = database(&format!("ivf-rq-linear-{count}.db"), false);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let mut state = 41;
        let rows: Vec<(i64, Vec<f32>)> = (1..=count).map(|id| (id, random(&mut state, DIMS))).collect();
        let before = SURVEYED.with(|c| c.get());
        insert_all(&conn, &rows);
        optimize(&conn, "v_ivf");
        reads.push(SURVEYED.with(|c| c.get()) - before);
        assert!(stored(&conn).pending().is_empty());
    }
    let ratio = reads[1] as f64 / reads[0] as f64;
    assert!(ratio < 3.0, "surveys read {reads:?} rows: doubling the backlog multiplied them by {ratio:.2}");
}

/// Once maintenance has run out of work, every block and tail entry is coded against its
/// partition's current centroid: a move recodes the entries that stay, not only those it moves.
#[test]
fn every_entry_is_coded_against_its_partitions_current_centroid() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-recode-{mvcc}.db"), mvcc);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let mut state = 43;
        let centres: Vec<Vec<f32>> = (0..8).map(|_| random(&mut state, DIMS).iter().map(|x| 4.0 * x).collect()).collect();
        for id in 1..=2000i64 {
            let v = near(&mut state, &centres, 0.3);
            conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v))).unwrap();
        }
        optimize(&conn, "v_ivf");
        let index = stored(&conn);
        assert!(index.pending().is_empty(), "mvcc={mvcc}: {:?}", index.pending());
        assert!(index.live().len() > 4, "mvcc={mvcc}: the index split into {} partitions", index.live().len());
        for kind in [BLOCK_ROW, TAIL_ROW] {
            for r in partition_rows(&conn, kind) {
                let (key, version) = (r[0].as_int().unwrap(), r[1].as_int().unwrap());
                let current = index.versions.get(&partition_of(key)).copied().unwrap_or(0);
                assert_eq!(version, current, "mvcc={mvcc}: row {key} of kind {kind} is coded against version {version} of partition {}", partition_of(key));
            }
        }
    }
}

/// Deleting every vector of half the clusters retires the partitions that held them: none is
/// left live with fewer entries than the retirement bound, and the queries stay exact. A query
/// probes the retired partitions only under MVCC, where a concurrent writer coding against an
/// older model can leave a tail entry in one.
#[test]
fn emptied_partitions_retire() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-retire-{mvcc}.db"), mvcc);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let mut state = 47;
        let centres: Vec<Vec<f32>> = (0..8).map(|_| random(&mut state, DIMS).iter().map(|x| 4.0 * x).collect()).collect();
        let mut live = HashMap::new();
        for (c, centre) in centres.iter().enumerate() {
            let rows: Vec<(i64, Vec<f32>)> = (0..300).map(|i| ((c * 300 + i + 1) as i64, near(&mut state, std::slice::from_ref(centre), 0.2))).collect();
            insert_all(&conn, &rows);
            live.extend(rows);
        }
        optimize(&conn, "v_ivf");
        let before = stored(&conn).live().len();
        conn.execute("DELETE FROM v WHERE id <= 1200").unwrap();
        live.retain(|id, _| *id > 1200);
        optimize(&conn, "v_ivf");
        let index = stored(&conn);
        assert!(index.pending().is_empty(), "mvcc={mvcc}: {:?}", index.pending());
        let retired = index.parts.values().filter(|part| part.state == State::Retired).count();
        assert!(retired > 0, "mvcc={mvcc}: no partition of {before} retired");
        for centre in &centres {
            let q = near(&mut state, std::slice::from_ref(centre), 0.2);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
        let probes = PROBES.with(|c| c.get());
        top_ten(&conn, &near(&mut state, &centres, 0.2));
        let probed = PROBES.with(|c| c.get()) - probes;
        let wanted = index.live().len() + if mvcc { retired } else { 0 };
        assert_eq!(probed, wanted, "mvcc={mvcc}: {probed} partitions probed, {} live and {retired} retired", index.live().len());
    }
}

/// CREATE INDEX over a table that already holds 3000 rows trains its partitions by k-means over a
/// sample of them, in the statement itself: afterwards about the square root of the rows are
/// live partitions, every entry is packed, no job is due, and the queries are exact.
#[test]
fn create_index_over_rows_trains_its_partitions() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-build-{mvcc}.db"), mvcc);
        let mut state = 59;
        let centres: Vec<Vec<f32>> = (0..12).map(|_| random(&mut state, DIMS).iter().map(|x| 3.0 * x).collect()).collect();
        let rows: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, near(&mut state, &centres, 0.4))).collect();
        insert_all(&conn, &rows);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let index = stored(&conn);
        let tails: usize = index.tails.values().sum();
        assert!(index.live().len() >= trained_partitions(3000) / 2, "mvcc={mvcc}: {} live partitions for 3000 rows", index.live().len());
        assert_eq!(tails, 0, "mvcc={mvcc}: entries left in tails");
        assert!(index.pending().is_empty(), "mvcc={mvcc}: {:?}", index.pending());
        let live: HashMap<i64, Vec<f32>> = rows.into_iter().collect();
        for _ in 0..3 {
            let q = near(&mut state, &centres, 0.4);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
    }
}

/// The estimates rank well enough that a query reranking only the rows it returns, at a refine
/// of one, finds nearly all of the exact top ten: probing every partition, at least 95% of them
/// over fifty queries of random vectors, whose best products lie closer together than the
/// estimates' error, of 64 dimensions and of 200, which the rotation pads to 224.
#[test]
fn a_query_reranking_only_its_answer_finds_the_exact_top_ten() {
    for dims in [64, 200] {
        let (_db, conn) = database(&format!("ivf-rq-refine-one-{dims}.db"), false);
        let mut state = 97;
        let vectors: Vec<(i64, Vec<f32>)> = (1..=2000i64).map(|id| (id, random(&mut state, dims))).collect();
        insert_all(&conn, &vectors);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 1)").unwrap();
        let live: HashMap<i64, Vec<f32>> = vectors.into_iter().collect();
        let (mut found, queries) = (0, 50);
        for _ in 0..queries {
            let q = random(&mut state, dims);
            let exact = exact_top_ten(&live, &q);
            found += top_ten(&conn, &q).iter().filter(|id| exact.contains(id)).count();
        }
        let recall = found as f64 / (10 * queries) as f64;
        assert!(recall >= 0.95, "dims {dims}: recall@10 {recall} at a refine of one");
    }
}

/// A fast-scanned member is summed exactly only near the bar, where the exact sum can drop it:
/// a query over a trained index refines more entries than it sums exactly, and still finds the
/// exact top ten.
#[test]
fn a_query_sums_exactly_only_the_members_near_the_bar() {
    const DIMS: usize = 64;
    let (_db, conn) = database("ivf-rq-exact-sums.db", false);
    let mut state = 131;
    let vectors: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
    insert_all(&conn, &vectors);
    conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
    let live: HashMap<i64, Vec<f32>> = vectors.into_iter().collect();
    let q = random(&mut state, DIMS);
    top_ten(&conn, &q);
    let (sums, refines) = (EXACT_SUMS.with(|c| c.get()), QUERY_REFINES.with(|c| c.get()));
    assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q));
    let (sums, refines) = (EXACT_SUMS.with(|c| c.get()) - sums, QUERY_REFINES.with(|c| c.get()) - refines);
    assert!(refines > 0 && sums < refines, "{sums} exact sums for {refines} refined entries");
}

/// The blocks a query read are held for the next: a second query over the same partitions reads
/// no block from pages, until a pack rewrites them under a new version, when the next query reads
/// those again, and every answer is the exact top ten.
#[test]
fn a_query_scores_held_partitions_from_memory_until_a_write_rewrites_them() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-held-{mvcc}.db"), mvcc);
        let mut state = 109;
        let mut live: HashMap<i64, Vec<f32>> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
        let mut vectors: Vec<(i64, Vec<f32>)> = live.iter().map(|(id, v)| (*id, v.clone())).collect();
        vectors.sort_by_key(|(id, _)| *id);
        insert_all(&conn, &vectors);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let q = random(&mut state, DIMS);
        let reads = |conn: &Arc<turso_core::Connection>, live: &HashMap<i64, Vec<f32>>| {
            let before = BLOCK_READS.with(|c| c.get());
            assert_eq!(top_ten(conn, &q), exact_top_ten(live, &q), "mvcc={mvcc}");
            BLOCK_READS.with(|c| c.get()) - before
        };
        assert!(reads(&conn, &live) > 0, "mvcc={mvcc}: the first query read no block");
        assert_eq!(reads(&conn, &live), 0, "mvcc={mvcc}: the second query read blocks its first had read");
        // Rows near the query land in the partitions it probes; their packs rewrite blocks there.
        let near: Vec<(i64, Vec<f32>)> = (3001..=3100i64).map(|id| (id, q.iter().zip(random(&mut state, DIMS)).map(|(a, n)| a + 0.05 * n).collect())).collect();
        insert_all(&conn, &near);
        optimize(&conn, "v_ivf");
        live.extend(near);
        assert!(reads(&conn, &live) > 0, "mvcc={mvcc}: the query after the packs read no block");
        assert_eq!(reads(&conn, &live), 0, "mvcc={mvcc}: the query after that read blocks again");
    }
}

/// A transaction that read the index keeps its snapshot's answers while another connection
/// rewrites the partitions it probes, the blocks it holds being those of its own snapshot's
/// version, and sees the rewrite once it ends.
#[test]
fn a_snapshot_keeps_its_answers_while_another_connection_rewrites_the_partitions() {
    const DIMS: usize = 16;
    let (db, writer) = database("ivf-rq-held-snapshot.db", false);
    let mut state = 113;
    let mut live: HashMap<i64, Vec<f32>> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
    let mut vectors: Vec<(i64, Vec<f32>)> = live.iter().map(|(id, v)| (*id, v.clone())).collect();
    vectors.sort_by_key(|(id, _)| *id);
    insert_all(&writer, &vectors);
    writer.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
    let reader = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
    let q = random(&mut state, DIMS);
    let before = exact_top_ten(&live, &q);
    reader.execute("BEGIN").unwrap();
    assert_eq!(top_ten(&reader, &q), before);
    let near: Vec<(i64, Vec<f32>)> = (3001..=3100i64).map(|id| (id, q.iter().zip(random(&mut state, DIMS)).map(|(a, n)| a + 0.05 * n).collect())).collect();
    insert_all(&writer, &near);
    optimize(&writer, "v_ivf");
    live.extend(near);
    let after = exact_top_ten(&live, &q);
    assert_ne!(before, after, "the rows near the query do not change its top ten");
    assert_eq!(top_ten(&writer, &q), after, "the writer's answer");
    assert_eq!(top_ten(&reader, &q), before, "the reader's snapshot answer");
    reader.execute("COMMIT").unwrap();
    assert_eq!(top_ten(&reader, &q), after, "the reader's answer after its snapshot");
}

/// A partition's rows held under one snapshot serve the next query, which reads no page, until a
/// commit writes the partition, when the next query reads it again and finds the row: committed
/// on its own, in a transaction whose own query found it first, or after a checkpoint restarted
/// the WAL, under WAL and under MVCC. While the transaction is open, probes read pages, and a
/// rolled-back row is never found.
#[test]
fn held_rows_serve_later_snapshots_until_a_commit_writes_their_partition() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (db, writer) = database(&format!("ivf-rq-held-rows-{mvcc}.db"), mvcc);
        let mut state = 211;
        let mut live: HashMap<i64, Vec<f32>> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
        let mut vectors: Vec<(i64, Vec<f32>)> = live.iter().map(|(id, v)| (*id, v.clone())).collect();
        vectors.sort_by_key(|(id, _)| *id);
        insert_all(&writer, &vectors);
        writer.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let reader = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
        let q = random(&mut state, DIMS);
        // The reader's answer, the exact top ten, and the seeks its probes made.
        let seeks = |live: &HashMap<i64, Vec<f32>>| {
            let before = WALK_SEEKS.with(|c| c.get());
            assert_eq!(top_ten(&reader, &q), exact_top_ten(live, &q), "mvcc={mvcc}");
            WALK_SEEKS.with(|c| c.get()) - before
        };
        // A row nearer the query than any other.
        let nearest = |id: i64| format!("INSERT INTO v VALUES ({id}, {})", literal(&q.iter().map(|a| 2.0 * a).collect::<Vec<f32>>()));
        let add = |live: &mut HashMap<i64, Vec<f32>>, id: i64| {
            live.insert(id, q.iter().map(|a| 2.0 * a).collect());
        };
        // The first query holds the rows; under MVCC its snapshot predates the index's record
        // of writes, begun by its own statement, so the second holds them again.
        seeks(&live);
        seeks(&live);
        assert_eq!(seeks(&live), 0, "mvcc={mvcc}: a query over held partitions read pages");

        writer.execute(nearest(3001)).unwrap();
        add(&mut live, 3001);
        assert!(seeks(&live) > 0, "mvcc={mvcc}: the query after a commit read no page");
        assert_eq!(seeks(&live), 0, "mvcc={mvcc}: the partitions read again were not held");

        writer.execute("BEGIN").unwrap();
        writer.execute(nearest(3002)).unwrap();
        let mut with = live.clone();
        add(&mut with, 3002);
        assert_eq!(top_ten(&writer, &q), exact_top_ten(&with, &q), "mvcc={mvcc}: the writer's own row");
        assert!(seeks(&live) > 0, "mvcc={mvcc}: a query took held rows while a writer was open");
        writer.execute("COMMIT").unwrap();
        assert!(seeks(&with) > 0, "mvcc={mvcc}: the query after the transaction read no page");
        assert_eq!(seeks(&with), 0, "mvcc={mvcc}: the partitions read after the transaction were not held");
        live = with;

        writer.execute("BEGIN").unwrap();
        writer.execute(nearest(3003)).unwrap();
        writer.execute("ROLLBACK").unwrap();
        seeks(&live);
        assert_eq!(seeks(&live), 0, "mvcc={mvcc}: the partitions read after the rollback were not held");

        // A tail left long enough to pack, and packed by OPTIMIZE INDEX alone.
        STATEMENT_UPKEEP.with(|on| on.set(false));
        for id in 3101..=3112i64 {
            writer.execute(nearest(id)).unwrap();
            add(&mut live, id);
        }
        STATEMENT_UPKEEP.with(|on| on.set(true));
        seeks(&live);
        assert_eq!(seeks(&live), 0, "mvcc={mvcc}: the partitions read after the inserts were not held");
        optimize(&writer, "v_ivf");
        assert!(seeks(&live) > 0, "mvcc={mvcc}: the query after OPTIMIZE INDEX read no page");
        assert_eq!(seeks(&live), 0, "mvcc={mvcc}: the partitions read after OPTIMIZE INDEX were not held");

        if !mvcc {
            writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            writer.execute(nearest(3004)).unwrap();
            add(&mut live, 3004);
            assert!(seeks(&live) > 0, "the query after a checkpoint and a commit read no page");
            assert_eq!(seeks(&live), 0, "the partitions read after the checkpoint were not held");
        }
    }
}

/// While a transaction writing one partition is open, a query probing every partition reads
/// that partition from pages and takes every other's held rows, under WAL and under MVCC; once
/// the transaction commits, the next query reads the partition again and finds its row.
#[test]
fn an_open_writer_keeps_from_the_held_rows_only_the_partitions_it_wrote() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (db, writer) = database(&format!("ivf-rq-open-writer-{mvcc}.db"), mvcc);
        let mut state = 239;
        let mut live: HashMap<i64, Vec<f32>> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
        let mut vectors: Vec<(i64, Vec<f32>)> = live.iter().map(|(id, v)| (*id, v.clone())).collect();
        vectors.sort_by_key(|(id, _)| *id);
        insert_all(&writer, &vectors);
        writer.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let reader = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
        let q = random(&mut state, DIMS);
        let seeks = |live: &HashMap<i64, Vec<f32>>| {
            let before = WALK_SEEKS.with(|c| c.get());
            assert_eq!(top_ten(&reader, &q), exact_top_ten(live, &q), "mvcc={mvcc}");
            WALK_SEEKS.with(|c| c.get()) - before
        };
        seeks(&live);
        seeks(&live);
        assert_eq!(seeks(&live), 0, "mvcc={mvcc}: a query over held partitions read pages");
        // One row near the query, coded into one partition's tail and nothing else.
        let near: Vec<f32> = q.iter().map(|a| 2.0 * a).collect();
        writer.execute("BEGIN").unwrap();
        STATEMENT_UPKEEP.with(|on| on.set(false));
        writer.execute(format!("INSERT INTO v VALUES (3001, {})", literal(&near))).unwrap();
        STATEMENT_UPKEEP.with(|on| on.set(true));
        assert_eq!(seeks(&live), 1, "mvcc={mvcc}: beside a writer of one partition, a query read other partitions' pages");
        assert_eq!(seeks(&live), 1, "mvcc={mvcc}: the partition the writer holds was held");
        writer.execute("COMMIT").unwrap();
        live.insert(3001, near);
        assert_eq!(seeks(&live), 1, "mvcc={mvcc}: the query after the commit read other partitions' pages");
        assert_eq!(seeks(&live), 0, "mvcc={mvcc}: the partition read after the commit was not held");
    }
}

/// A reader inside a transaction keeps its snapshot's answer while another connection commits a
/// row near its query and a third holds the partitions' rows under its later snapshot, under WAL
/// and under MVCC; once its transaction ends, the reader finds the row.
#[test]
fn a_snapshot_never_takes_rows_held_under_a_later_one() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (db, writer) = database(&format!("ivf-rq-later-rows-{mvcc}.db"), mvcc);
        let mut state = 223;
        let mut live: HashMap<i64, Vec<f32>> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
        let mut vectors: Vec<(i64, Vec<f32>)> = live.iter().map(|(id, v)| (*id, v.clone())).collect();
        vectors.sort_by_key(|(id, _)| *id);
        insert_all(&writer, &vectors);
        writer.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let (reader, other) = (db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap(), db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap());
        let q = random(&mut state, DIMS);
        let before = exact_top_ten(&live, &q);
        reader.execute("BEGIN").unwrap();
        assert_eq!(top_ten(&reader, &q), before, "mvcc={mvcc}");
        let v: Vec<f32> = q.iter().map(|a| 2.0 * a).collect();
        writer.execute(format!("INSERT INTO v VALUES (3001, {})", literal(&v))).unwrap();
        live.insert(3001, v);
        let after = exact_top_ten(&live, &q);
        assert_ne!(before, after, "the row does not change the query's top ten");
        assert_eq!(top_ten(&other, &q), after, "mvcc={mvcc}: the third connection's answer");
        assert_eq!(top_ten(&other, &q), after, "mvcc={mvcc}: the third connection's answer from its held rows");
        assert_eq!(top_ten(&reader, &q), before, "mvcc={mvcc}: the reader took rows held under a later snapshot");
        reader.execute("COMMIT").unwrap();
        assert_eq!(top_ten(&reader, &q), after, "mvcc={mvcc}: the reader's answer after its transaction");
    }
}

/// A query reads each partition it probes with one seek: its bookkeeping, dead rows, tail and
/// blocks lie under one key range, read in one walk.
#[test]
fn a_query_reads_each_probed_partition_with_one_seek() {
    let (_db, conn) = database("ivf-rq-one-seek.db", false);
    let mut state = 107;
    let vectors: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, random(&mut state, 16))).collect();
    insert_all(&conn, &vectors);
    // Nothing held, so every probe reads its partition from pages.
    conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 8, refine = 4, cache_mib = 0)").unwrap();
    // Tail entries and dead rows beside the blocks.
    for id in 3001..=3040i64 {
        conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&random(&mut state, 16)))).unwrap();
    }
    for id in (5..=3000i64).step_by(97) {
        conn.execute(format!("DELETE FROM v WHERE id = {id}")).unwrap();
    }
    let q = random(&mut state, 16);
    // The first query loads the model; the second finds it shared.
    top_ten(&conn, &q);
    let (seeks, probes) = (WALK_SEEKS.with(|c| c.get()), PROBES.with(|c| c.get()));
    top_ten(&conn, &q);
    let (seeks, probes) = (WALK_SEEKS.with(|c| c.get()) - seeks, PROBES.with(|c| c.get()) - probes);
    assert!(probes >= 8, "the query probed {probes} partitions");
    assert_eq!(seeks, probes, "{seeks} seeks for {probes} probed partitions");
}

/// CREATE INDEX over a populated table writes each vector's code once, into its block, so the
/// file it leaves holds almost no free pages.
#[test]
fn create_index_over_rows_leaves_almost_no_free_pages() {
    const DIMS: usize = 16;
    let (_db, conn) = database("ivf-rq-build-once.db", false);
    let mut state = 103;
    let vectors: Vec<(i64, Vec<f32>)> = (1..=20_000i64).map(|id| (id, random(&mut state, DIMS))).collect();
    for chunk in vectors.chunks(2_000) {
        insert_all(&conn, chunk);
    }
    conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 64, refine = 4)").unwrap();
    let pragma = |name: &str| rows(&conn, &format!("PRAGMA {name}"))[0][0].as_int().unwrap();
    let (pages, free) = (pragma("page_count"), pragma("freelist_count"));
    assert!(free * 20 <= pages, "{free} of {pages} pages free after the build");
}

/// A vector's row in the index names only where its code is, the code itself being in its block
/// or tail entry, so each code is stored once: after a build and after single inserts.
#[test]
fn a_vectors_row_holds_only_where_its_code_is() {
    let (_db, conn) = database("ivf-rq-one-copy.db", false);
    let mut state = 101;
    let vectors: Vec<(i64, Vec<f32>)> = (1..=300i64).map(|id| (id, random(&mut state, 32))).collect();
    insert_all(&conn, &vectors);
    conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 64, refine = 4)").unwrap();
    for id in 301..=320i64 {
        conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&random(&mut state, 32)))).unwrap();
    }
    let stored = rows(&conn, "SELECT * FROM __turso_internal_v_ivf_vectors");
    assert_eq!(stored.len(), 320);
    for row in &stored {
        assert_eq!(row.len(), 1, "a vector's row holds {} values: {row:?}", row.len());
    }
}

/// Each live partition's current centroid.
fn live_centroids(conn: &Arc<turso_core::Connection>) -> Vec<(i64, Vec<f32>)> {
    let index = stored(conn);
    let mut centroids = Vec::new();
    for r in backing(conn, "centroids", "value") {
        let key = r[0].as_int().unwrap();
        let p = key >> VERSION_BITS;
        if key < 0 || index.part(p).state != State::Live || index.versions.get(&p) != Some(&(key & ((1 << VERSION_BITS) - 1))) {
            continue;
        }
        centroids.push((p, dense(r[1].to_blob()).unwrap()));
    }
    centroids
}

/// Live vectors whose partition is not the live partition nearest them, of all live vectors.
fn misplaced(conn: &Arc<turso_core::Connection>, live: &HashMap<i64, Vec<f32>>) -> (usize, usize) {
    let centroids = live_centroids(conn);
    let mut wrong = 0;
    for r in backing(conn, "vectors", "location") {
        let rowid = r[0].as_int().unwrap();
        let p = match Location::decode(r[1].as_int().unwrap()) {
            Location::Block(key) | Location::Tail(key) => partition_of(key),
        };
        let v = &live[&rowid];
        let nearest = centroids.iter().min_by(|a, b| l2(&a.1, v).total_cmp(&l2(&b.1, v))).map(|(p, _)| *p).unwrap();
        wrong += (nearest != p) as usize;
    }
    (wrong, live.len())
}

/// Splits recheck the partitions beside them: once maintenance has run out of work over one
/// uniform blob of vectors, where every partition borders many others, almost every vector lives
/// in the live partition nearest it.
#[test]
fn vectors_live_in_their_nearest_partition() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-nearest-{mvcc}.db"), mvcc);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let mut state = 53;
        let mut live = HashMap::new();
        for id in 1..=3000i64 {
            let v = random(&mut state, DIMS);
            conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v))).unwrap();
            live.insert(id, v);
        }
        optimize(&conn, "v_ivf");
        let (wrong, total) = misplaced(&conn, &live);
        eprintln!("mvcc={mvcc}: {wrong} of {total} vectors outside their nearest partition, {} partitions", stored(&conn).live().len());
        assert!(wrong * 100 < total * MISPLACED_PERCENT, "mvcc={mvcc}: {wrong} of {total} vectors outside their nearest partition");
    }
}

/// A database, a connection to it, and the rows of its table `f`.
type Tagged = (Arc<turso_core::Database>, Arc<turso_core::Connection>, Vec<(i64, Vec<f32>)>);

/// A table `f` of 3000 random vectors, each tagged with its id modulo 10, and rare when its id
/// is a multiple of 200, under an ivf_rq index probing 16 partitions, about a third of them.
fn tagged(name: &str, mvcc: bool) -> Tagged {
    let (db, conn) = database(name, mvcc);
    conn.execute("CREATE TABLE f(id INTEGER PRIMARY KEY, e BLOB NOT NULL, tag INTEGER NOT NULL, rare INTEGER NOT NULL)").unwrap();
    let mut state = 73;
    let rows: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, random(&mut state, 16))).collect();
    let values: Vec<String> = rows.iter().map(|(id, v)| format!("({id}, {}, {}, {})", literal(v), id % 10, i64::from(id % 200 == 0))).collect();
    conn.execute(format!("INSERT INTO f VALUES {}", values.join(","))).unwrap();
    conn.execute("CREATE INDEX f_ivf ON f USING ivf_rq (e) WITH (nprobes = 16, refine = 4)").unwrap();
    (db, conn, rows)
}

/// An unfiltered query covers its ask: past its own probe count it goes on probing, nearest
/// partition first, until it has screened [`COVER`] times the rows it asks for, so a LIMIT larger
/// than its first partitions hold answers the exact nearest; a small LIMIT stops long before
/// every partition.
#[test]
fn an_unfiltered_query_probes_until_it_has_screened_its_cover() {
    for mvcc in [false, true] {
        let (_db, conn) = database(&format!("ivf-rq-cover-{mvcc}.db"), mvcc);
        let mut state = 101;
        let rows: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, random(&mut state, 16))).collect();
        let values: Vec<String> = rows.iter().map(|(id, v)| format!("({id}, {})", literal(v))).collect();
        conn.execute(format!("INSERT INTO v VALUES {}", values.join(","))).unwrap();
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1, refine = 4)").unwrap();
        let live = stored(&conn).live().len();
        assert!(live > 8, "mvcc={mvcc}: {live} partitions");
        for limit in [200usize, 1] {
            let q = random(&mut state, 16);
            let mut exact: Vec<(f64, i64)> = rows.iter().map(|(id, v)| (dot(&q, v), *id)).collect();
            exact.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
            let want: Vec<i64> = exact.iter().take(limit).map(|(_, id)| *id).collect();
            let before = PROBES.with(|c| c.get());
            let got = ids(&conn, &format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT {limit}", literal(&q)));
            let probed = PROBES.with(|c| c.get()) - before;
            if limit == 200 {
                // 200 rows times the cover is past the table's 3,000: every partition is probed.
                assert_eq!(got, want, "mvcc={mvcc}: the covered query's answer");
                assert!(probed >= live, "mvcc={mvcc}: {probed} of {live} partitions probed");
            } else {
                assert_eq!(got.len(), 1, "mvcc={mvcc}");
                assert!(probed < live, "mvcc={mvcc}: a LIMIT 1 query probed all {live} partitions");
            }
        }
    }
}

/// The ids of the ten rows nearest `q` among those whose id `keep` accepts.
fn exact_filtered(rows: &[(i64, Vec<f32>)], q: &[f32], keep: impl Fn(i64) -> bool) -> Vec<i64> {
    let mut exact: Vec<(f64, i64)> = rows.iter().filter(|(id, _)| keep(*id)).map(|(id, v)| (dot(q, v), *id)).collect();
    exact.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    exact.iter().take(10).map(|(_, id)| *id).collect()
}

fn opcodes(conn: &Arc<turso_core::Connection>, sql: &str) -> Vec<String> {
    rows(conn, &format!("EXPLAIN {sql}")).iter().map(|row| row[1].to_string()).collect()
}

fn ids(conn: &Arc<turso_core::Connection>, sql: &str) -> Vec<i64> {
    rows(conn, sql).iter().map(|row| row[0].as_int().unwrap()).collect()
}

/// A WHERE term the pattern does not cover no longer keeps a query off the index: the index
/// gives out rows in order of distance and is told which the term accepts, and the answer is
/// the exact top ten of the rows the term keeps, after no more than the four rounds a term
/// keeping one row in ten needs.
#[test]
fn a_filtered_query_returns_the_exact_top_ten_of_the_rows_its_filter_keeps() {
    for mvcc in [false, true] {
        let (_db, conn, rows) = tagged(&format!("ivf-rq-filtered-{mvcc}.db"), mvcc);
        let mut state = 79;
        for _ in 0..3 {
            let q = random(&mut state, 16);
            let sql = format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM f WHERE tag = 3 ORDER BY distance LIMIT 10", literal(&q));
            let plan = opcodes(&conn, &sql);
            assert!(plan.iter().any(|op| op == "IndexMethodQuery") && plan.iter().any(|op| op == "IndexMethodAccept"), "mvcc={mvcc}: the filtered query does not go through the index: {plan:?}");
            let before = QUERY_ROUNDS.with(|count| count.get());
            let got = ids(&conn, &sql);
            let rounds = QUERY_ROUNDS.with(|count| count.get()) - before;
            assert_eq!(got, exact_filtered(&rows, &q, |id| id % 10 == 3), "mvcc={mvcc}");
            assert!(rounds <= 4, "mvcc={mvcc}: a filter keeping one row in ten took {rounds} rounds");
        }
    }
}

/// A filter keeping one row in two hundred widens the search round by round until the rows the
/// index gave out hold the ten nearest it keeps, so its answer is exact too.
#[test]
fn a_rare_filter_widens_the_search_until_its_answer_is_exact() {
    for mvcc in [false, true] {
        let (_db, conn, rows) = tagged(&format!("ivf-rq-rare-{mvcc}.db"), mvcc);
        let mut state = 83;
        for _ in 0..3 {
            let q = random(&mut state, 16);
            let sql = format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM f WHERE rare = 1 ORDER BY distance LIMIT 10", literal(&q));
            assert!(opcodes(&conn, &sql).iter().any(|op| op == "IndexMethodAccept"), "mvcc={mvcc}: the filtered query does not go through the index");
            assert_eq!(ids(&conn, &sql), exact_filtered(&rows, &q, |id| id % 200 == 0), "mvcc={mvcc}");
        }
    }
}

/// The planner keeps a filtered query off the index where the plan without it costs less: a
/// rowid lookup, and a filter a B-tree index serves and the planner takes to keep few rows.
/// Each answer stays exact.
#[test]
fn a_filter_cheaper_served_without_the_index_keeps_its_plan() {
    for mvcc in [false, true] {
        let (_db, conn, rows) = tagged(&format!("ivf-rq-selective-{mvcc}.db"), mvcc);
        conn.execute("CREATE INDEX f_tag ON f(tag)").unwrap();
        let mut state = 89;
        let vector = random(&mut state, 16);
        let q = literal(&vector);
        let by_rowid = format!("SELECT id, vector_distance_dot(e, {q}) AS distance FROM f WHERE id = 1234 ORDER BY distance LIMIT 10");
        let by_tag = format!("SELECT id, vector_distance_dot(e, {q}) AS distance FROM f WHERE tag = 3 ORDER BY distance LIMIT 10");
        for sql in [&by_rowid, &by_tag] {
            let plan = opcodes(&conn, sql);
            assert!(!plan.iter().any(|op| op == "IndexMethodQuery"), "mvcc={mvcc}: {sql} goes through the index: {plan:?}");
        }
        assert_eq!(ids(&conn, &by_rowid), vec![1234], "mvcc={mvcc}");
        assert_eq!(ids(&conn, &by_tag), exact_filtered(&rows, &vector, |id| id % 10 == 3), "mvcc={mvcc}");
    }
}

/// The index applies the LIMIT to the rows it gives out, before a DISTINCT would thin them, so a
/// DISTINCT query, filtered or not, is planned without it and applies its LIMIT after its
/// DISTINCT; each answer is exact.
#[test]
fn a_distinct_query_applies_its_limit_after_its_distinct() {
    for mvcc in [false, true] {
        let (_db, conn, rows) = tagged(&format!("ivf-rq-distinct-{mvcc}.db"), mvcc);
        let mut state = 97;
        let vector = random(&mut state, 16);
        let q = literal(&vector);
        let all = format!("SELECT DISTINCT id, vector_distance_dot(e, {q}) AS distance FROM f ORDER BY distance LIMIT 10");
        let rare = format!("SELECT DISTINCT id, vector_distance_dot(e, {q}) AS distance FROM f WHERE rare = 1 ORDER BY distance LIMIT 10");
        // The planner now sees that DISTINCT over the primary key thins nothing, and may use the
        // index with the LIMIT (upstream's change); each answer must still be exact, as the
        // LIMIT applies after whatever DISTINCT does.
        assert_eq!(ids(&conn, &all), exact_filtered(&rows, &vector, |_| true), "mvcc={mvcc}");
        assert_eq!(ids(&conn, &rare), exact_filtered(&rows, &vector, |id| id % 200 == 0), "mvcc={mvcc}");
    }
}

/// LIMIT 0 asks the index for no rows, and a negative LIMIT for every row, in order of distance
/// from every partition, not only the probed ones.
#[test]
fn a_zero_limit_gives_no_rows_and_a_negative_one_every_row() {
    for mvcc in [false, true] {
        let (_db, conn, rows) = tagged(&format!("ivf-rq-limits-{mvcc}.db"), mvcc);
        let mut state = 101;
        let vector = random(&mut state, 16);
        let q = literal(&vector);
        let none = format!("SELECT id, vector_distance_dot(e, {q}) AS distance FROM f ORDER BY distance LIMIT 0");
        let every = format!("SELECT id, vector_distance_dot(e, {q}) AS distance FROM f ORDER BY distance LIMIT -1");
        for sql in [&none, &every] {
            assert!(opcodes(&conn, sql).iter().any(|op| op == "IndexMethodQuery"), "mvcc={mvcc}: {sql} does not go through the index");
        }
        assert!(ids(&conn, &none).is_empty(), "mvcc={mvcc}");
        let mut exact: Vec<(f64, i64)> = rows.iter().map(|(id, v)| (dot(&vector, v), *id)).collect();
        exact.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        assert_eq!(ids(&conn, &every), exact.iter().map(|(_, id)| *id).collect::<Vec<_>>(), "mvcc={mvcc}");
    }
}

/// The share of live vectors outside their nearest partition the nearest-partition test allows:
/// over these 3000 vectors, rechecking 8 neighbours per split leaves 1.7%, 4 leave 5.3% and none
/// 9.7%.
const MISPLACED_PERCENT: usize = 3;

/// A database, a connection to it, its rows by id, and the centres of its two clusters.
type Clustered = (Arc<turso_core::Database>, Arc<turso_core::Connection>, HashMap<i64, Vec<f32>>, [Vec<f32>; 2]);

/// Two clusters far apart, each trained into partitions of its own, so a split of one
/// cluster's partition marks none of the other's moving: the rows, and a connection to the
/// index with every partition probed, on which a query has read the model.
fn two_clusters(name: &str) -> Clustered {
    const DIMS: usize = 16;
    let (db, conn) = database(name, true);
    let mut state = 913u64;
    let centres = [vec![8.0f32; DIMS], vec![-8.0f32; DIMS]];
    let rows: Vec<(i64, Vec<f32>)> = (1..=4000).map(|id| (id, near(&mut state, &centres, 1.0))).collect();
    for chunk in rows.chunks(500) {
        insert_all(&conn, chunk);
    }
    conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 8)").unwrap();
    let live: HashMap<i64, Vec<f32>> = rows.into_iter().collect();
    assert_eq!(top_ten(&conn, &centres[0]), exact_top_ten(&live, &centres[0]));
    (db, conn, live, centres)
}

/// `n` rows from `first` on, all but at `centre`, so they grow the one partition nearest it.
fn crowd(first: i64, n: i64, centre: &[f32]) -> Vec<(i64, Vec<f32>)> {
    (first..first + n).map(|id| (id, centre.iter().enumerate().map(|(d, x)| x + 1e-3 * ((id as usize * 31 + d * 7) % 13) as f32).collect())).collect()
}

/// Two transactions under MVCC, open at once, each grow a partition of a different cluster past
/// the split bound: both splits are made, opening different slots, and both commit, where the
/// model's one generation row let one split at a time. A query on another connection, its model
/// read before, then answers exactly, from the partitions the splits opened too.
#[test]
fn splits_of_partitions_apart_in_open_transactions_both_commit() {
    let (db, conn, mut live, centres) = two_clusters("ivf-rq-concurrent-splits.db");
    let before = check(&conn, "v_ivf").unwrap();
    let (first, second) = (db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap(), db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap());
    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    for (writer, (from, centre)) in [(&first, (10_000, &centres[0])), (&second, (20_000, &centres[1]))] {
        let rows = crowd(from, 300, centre);
        insert_all(writer, &rows);
        live.extend(rows);
    }
    first.execute("COMMIT").unwrap();
    second.execute("COMMIT").unwrap();
    let after = check(&conn, "v_ivf").unwrap();
    assert!(after.consistent(), "{after:?}");
    assert_eq!(after.partitions, before.partitions + 2, "both splits made: {before:?} then {after:?}");
    for _ in 0..10 {
        optimize(&conn, "v_ivf");
    }
    let mut state = 5u64;
    for q in centres.iter().cloned().chain((0..4).map(|_| random(&mut state, 16).iter().map(|x| 16.0 * x).collect())) {
        assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q));
    }
}

/// The index's directory rows: each partition's version, state and tag.
fn directory(conn: &Arc<turso_core::Connection>) -> HashMap<i64, Directory> {
    rows(conn, "SELECT rowid, value FROM __turso_internal_v_ivf_centroids WHERE rowid < -1").iter().map(|row| (-2 - row[0].as_int().unwrap(), Directory::from_bytes(row[1].to_blob().unwrap()).unwrap())).collect()
}

/// A split rolled back with its transaction leaves the slot it opened vacant beside one a
/// committed split opened past it: the model is read with the gap, the index answers exactly
/// and checks consistent, and splits take retired and vacant slots alike, lowest first, until
/// the vacant one is taken.
#[test]
fn a_rolled_back_split_leaves_a_vacant_slot_a_later_split_takes() {
    let (db, conn, mut live, centres) = two_clusters("ivf-rq-vacant-slot.db");
    let before = check(&conn, "v_ivf").unwrap();
    let (first, second) = (db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap(), db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap());
    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    insert_all(&first, &crowd(10_000, 300, &centres[0]));
    let grown = crowd(20_000, 300, &centres[1]);
    insert_all(&second, &grown);
    live.extend(grown);
    first.execute("ROLLBACK").unwrap();
    second.execute("COMMIT").unwrap();
    let held = directory(&conn);
    let last = *held.keys().max().unwrap();
    let vacant = last - 1;
    assert!(!held.contains_key(&vacant), "slot {vacant} was not left vacant: {:?}", held.keys().collect::<BTreeSet<_>>());
    let reader = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
    for q in &centres {
        assert_eq!(top_ten(&reader, q), exact_top_ten(&live, q));
    }
    let middle = check(&conn, "v_ivf").unwrap();
    assert!(middle.consistent() && middle.partitions == before.partitions + 1, "{before:?} then {middle:?}");
    // Each split takes the lowest slot retired or vacant: the retired ones below the vacant
    // slot first.
    let retired = held.iter().filter(|(p, d)| **p < vacant && d.state == State::Retired).count();
    let mut state = 29u64;
    let mut taken = BTreeSet::new();
    for round in 0..=retired as i64 {
        let point: Vec<f32> = centres[0].iter().zip(random(&mut state, 16)).map(|(c, n)| c + 2.0 * n).collect();
        let grown = crowd(30_000 + 1_000 * round, 300, &point);
        insert_all(&conn, &grown);
        live.extend(grown);
        let now = directory(&conn);
        taken.extend(now.iter().filter(|(p, d)| d.state == State::Live && held.get(p).is_none_or(|was| was.state != State::Live)).map(|(p, _)| *p));
    }
    assert!(taken.contains(&vacant), "splits took {taken:?}, not the vacant slot {vacant}, past {retired} retired below it");
    let after = check(&conn, "v_ivf").unwrap();
    assert!(after.consistent(), "{after:?}");
    for q in &centres {
        assert_eq!(top_ten(&reader, q), exact_top_ten(&live, q));
    }
}

/// Where the index's writers are not recorded, as where other processes may write it, a
/// cursor checks the model it holds against every directory row: a query on one connection
/// answers exactly from the partitions a split on another opened since its last query.
#[test]
fn where_writers_are_not_recorded_a_query_reads_the_whole_directory() {
    use turso_core::{ConnectOpts, Database, DatabaseOpts, OpenFlags, PlatformIO};
    use turso_sqlite::SqliteDialect;
    const DIMS: usize = 16;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("directory.db");
    let db = Database::open_file_with_flags(Arc::new(PlatformIO::new().unwrap()), path.to_str().unwrap(), OpenFlags::default(), DatabaseOpts::new().with_index_method(true).with_multiprocess_wal(true), None, Arc::new(SqliteDialect)).unwrap();
    let (writer, reader) = (db.connect(ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap(), db.connect(ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap());
    writer.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)").unwrap();
    let mut state = 77u64;
    let centres = [vec![8.0f32; DIMS], vec![-8.0f32; DIMS]];
    let rows: Vec<(i64, Vec<f32>)> = (1..=3000).map(|id| (id, near(&mut state, &centres, 1.0))).collect();
    for chunk in rows.chunks(500) {
        insert_all(&writer, chunk);
    }
    writer.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 8)").unwrap();
    let mut live: HashMap<i64, Vec<f32>> = rows.into_iter().collect();
    assert_eq!(top_ten(&reader, &centres[0]), exact_top_ten(&live, &centres[0]));
    let before = check(&writer, "v_ivf").unwrap();
    let rows = crowd(10_000, 300, &centres[0]);
    insert_all(&writer, &rows);
    live.extend(rows);
    for _ in 0..10 {
        optimize(&writer, "v_ivf");
    }
    let after = check(&writer, "v_ivf").unwrap();
    assert!(after.consistent() && after.partitions > before.partitions, "{before:?} then {after:?}");
    for q in &centres {
        assert_eq!(top_ten(&reader, q), exact_top_ten(&live, q));
    }
}

/// The values of every row of index `v_ivf`'s backing tables that are blobs: codes, dead rows'
/// blocks and centroids.
fn backing_blobs(conn: &Arc<turso_core::Connection>) -> HashSet<Vec<u8>> {
    let mut blobs = HashSet::new();
    for (store, columns) in [("partitions", "c0, c1, c2"), ("centroids", "value")] {
        for row in rows(conn, &format!("SELECT {columns} FROM __turso_internal_v_ivf_{store}")) {
            blobs.extend(row.iter().filter_map(|value| value.to_blob()).map(<[u8]>::to_vec));
        }
    }
    blobs
}

/// REINDEX makes an ivf_rq index afresh from the table's rows: after half its vectors are
/// deleted and the index made again it holds the rest alone and answers exactly from them, and
/// under `secure_delete` none of the codes, dead rows or centroids of the index it replaced is
/// left in the checkpointed file, in WAL and in MVCC mode.
#[test]
fn reindex_makes_the_index_afresh_and_leaves_none_of_the_old_one_in_the_file() {
    use turso_core::{ConnectOpts, Database, DatabaseOpts, OpenFlags, PlatformIO};
    use turso_sqlite::SqliteDialect;
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reindex.db");
        let db = Database::open_file_with_flags(Arc::new(PlatformIO::new().unwrap()), path.to_str().unwrap(), OpenFlags::default(), DatabaseOpts::new().with_index_method(true), None, Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect(ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute("PRAGMA secure_delete = ON").unwrap();
        conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)").unwrap();
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e)").unwrap();
        let mut state = 17;
        let rows: Vec<(i64, Vec<f32>)> = (1..=3000).map(|id| (id, random(&mut state, DIMS))).collect();
        for chunk in rows.chunks(300) {
            insert_all(&conn, chunk);
        }
        for _ in 0..20 {
            optimize(&conn, "v_ivf");
        }
        // Stale copies in blocks, of vectors rewritten and of vectors then deleted.
        for (id, v) in rows.iter().filter(|(id, _)| id % 5 == 0) {
            let w: Vec<f32> = v.iter().map(|x| -x).collect();
            conn.execute(format!("UPDATE v SET e = {} WHERE id = {id}", literal(&w))).unwrap();
        }
        let old = backing_blobs(&conn);
        conn.execute("DELETE FROM v WHERE id % 2 = 0").unwrap();
        let live: HashMap<i64, Vec<f32>> = rows.iter().filter(|(id, _)| id % 2 == 1).map(|(id, v)| (*id, if id % 5 == 0 { v.iter().map(|x| -x).collect() } else { v.clone() })).collect();
        conn.execute("REINDEX v_ivf").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let found = check(&conn, "v_ivf").unwrap();
        assert!(found.consistent() && found.rows == live.len() as u64 && found.stale_copies == 0 && found.partitions > 4, "mvcc={mvcc}: {found:?}");
        for _ in 0..5 {
            let q = random(&mut state, DIMS);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
        }
        let new = backing_blobs(&conn);
        drop(conn);
        drop(db);
        let kept = old.iter().filter(|blob| blob.len() >= 16 && new.contains(*blob)).count();
        assert_eq!(kept, 0, "mvcc={mvcc}: the index made again holds {kept} rows of the one it replaced");
        let gone: Vec<&Vec<u8>> = old.iter().filter(|blob| blob.len() >= 16).collect();
        assert!(gone.len() > 100, "mvcc={mvcc}: the old index held only {} rows", gone.len());
        let file = std::fs::read(&path).unwrap();
        let prefixes: HashSet<&[u8]> = gone.iter().map(|blob| &blob[..16]).collect();
        let left: HashSet<&[u8]> = file.windows(16).filter(|window| prefixes.contains(window)).collect();
        let left: Vec<&&Vec<u8>> = gone.iter().filter(|blob| left.contains(&blob[..16]) && file.windows(blob.len()).any(|w| w == blob.as_slice())).collect();
        assert!(left.is_empty(), "mvcc={mvcc}: {} of {} rows of the old index left in the file", left.len(), gone.len());
    }
}

/// A REINDEX marks every partition changed. A reader that read the model it replaces while it
/// ran, under an earlier snapshot, held that model on: once a REINDEX that leaves fewer
/// partitions commits, the reader's next statement reads the whole directory, so the rows it
/// inserts go to partitions the index has, the index checks consistent and a fresh connection
/// finds them, in WAL and in MVCC mode.
#[test]
fn a_model_read_while_a_reindex_ran_is_not_used_after_it() {
    const DIMS: usize = 16;
    for mvcc in [false, true] {
        let (db, conn) = database(&format!("ivf-rq-reindex-reader-{mvcc}.db"), mvcc);
        let mut state = 919u64;
        let centres = [vec![8.0f32; DIMS], vec![-8.0f32; DIMS]];
        let rows: Vec<(i64, Vec<f32>)> = (1..=4000).map(|id| (id, near(&mut state, &centres, 1.0))).collect();
        for chunk in rows.chunks(500) {
            insert_all(&conn, chunk);
        }
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 8)").unwrap();
        conn.execute("DELETE FROM v WHERE id > 300").unwrap();
        let mut live: HashMap<i64, Vec<f32>> = rows.into_iter().filter(|(id, _)| *id <= 300).collect();
        let before = check(&conn, "v_ivf").unwrap().partitions;
        let reader = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("REINDEX v_ivf").unwrap();
        // The reader's snapshot precedes the REINDEX, so it reads the model being replaced.
        assert_eq!(top_ten(&reader, &centres[0]), exact_top_ten(&live, &centres[0]), "mvcc={mvcc}");
        conn.execute("COMMIT").unwrap();
        let after = check(&conn, "v_ivf").unwrap().partitions;
        assert!(after < before, "mvcc={mvcc}: the REINDEX left {after} partitions of {before}");
        let more: Vec<(i64, Vec<f32>)> = (10_001..=10_200).map(|id| (id, near(&mut state, &centres, 1.0))).collect();
        insert_all(&reader, &more);
        live.extend(more);
        let found = check(&conn, "v_ivf").unwrap();
        assert!(found.consistent() && found.rows == live.len() as u64, "mvcc={mvcc}: {found:?}");
        let fresh = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
        for centre in &centres {
            assert_eq!(top_ten(&fresh, centre), exact_top_ten(&live, centre), "mvcc={mvcc}");
        }
    }
}

/// A split claims its partition and the slot it opens alone. Under MVCC, beside another open
/// transaction whose job holds the partition nearest the one split, the split is made and
/// leaves that neighbour a mark, which a job takes up once the other transaction has ended; the
/// mark is no claim, so the other transaction's next job of the neighbour is made too. A split
/// that claimed its neighbours too was refused by a job of any open transaction near it.
#[test]
fn a_split_beside_a_neighbour_another_transaction_holds_is_made_and_marks_it() {
    let (db, conn, mut live, centres) = two_clusters("ivf-rq-split-held-neighbour.db");
    optimize(&conn, "v_ivf");
    let before = check(&conn, "v_ivf").unwrap();
    let centroids = live_centroids(&conn);
    let nearest = |to: &[f32], but: Option<i64>| centroids.iter().filter(|(p, _)| Some(*p) != but).min_by(|a, b| l2(&a.1, to).total_cmp(&l2(&b.1, to))).map(|(p, c)| (*p, c.clone())).unwrap();
    // The rows that grow a partition past the split bound, and the partition the index codes
    // them into by the loss it ranks partitions with: a probe inserts them, its maintenance
    // off, and rolls back.
    let grown = crowd(20_000, 300, &centres[0]);
    let probe = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
    probe.execute("BEGIN CONCURRENT").unwrap();
    STATEMENT_UPKEEP.with(|on| on.set(false));
    insert_all(&probe, &grown);
    STATEMENT_UPKEEP.with(|on| on.set(true));
    let coded: HashSet<i64> = backing(&probe, "vectors", "location")
        .iter()
        .filter(|r| r[0].as_int().unwrap() >= 20_000)
        .map(|r| match Location::decode(r[1].as_int().unwrap()) {
            Location::Block(key) | Location::Tail(key) => partition_of(key),
        })
        .collect();
    probe.execute("ROLLBACK").unwrap();
    assert_eq!(coded.len(), 1, "the rows are coded into partitions {coded:?}");
    let split = coded.into_iter().next().unwrap();
    let centre = centroids.iter().find(|(p, _)| *p == split).map(|(_, c)| c.clone()).unwrap();
    let (neighbour, beside) = nearest(&centre, Some(split));
    let (holder, splitter) = (db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap(), db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap());
    holder.execute("BEGIN CONCURRENT").unwrap();
    // One job, a pack of the neighbour's tail, whose claim the holder keeps until it ends.
    STATEMENT_BUDGET.with(|budget| budget.set(1));
    let held = crowd(10_000, 2 * PACK_AT as i64, &beside);
    insert_all(&holder, &held);
    STATEMENT_BUDGET.with(|budget| budget.set(usize::MAX));
    splitter.execute("BEGIN CONCURRENT").unwrap();
    insert_all(&splitter, &grown);
    splitter.execute("COMMIT").unwrap();
    live.extend(grown);
    let middle = check(&conn, "v_ivf").unwrap();
    assert!(middle.consistent() && middle.partitions == before.partitions + 1, "partition {split}, beside {neighbour}: {before:?} then {middle:?}");
    assert!(stored(&conn).marks.contains_key(&neighbour), "the held neighbour {neighbour} has no mark: {middle:?}");
    // A query beside the mark answers exactly.
    assert_eq!(top_ten(&conn, &beside), exact_top_ten(&live, &beside));
    // A mark is no job's write of the neighbour, so the holder's next job packs it again.
    STATEMENT_BUDGET.with(|budget| budget.set(1));
    let more = crowd(11_000, 2 * PACK_AT as i64, &beside);
    insert_all(&holder, &more);
    STATEMENT_BUDGET.with(|budget| budget.set(usize::MAX));
    let tail = stored(&holder).tails.get(&neighbour).copied().unwrap_or(0);
    assert!(tail < 2 * PACK_AT, "the holder's job left {tail} entries in the tail of the neighbour {neighbour}");
    holder.execute("COMMIT").unwrap();
    live.extend(held);
    live.extend(more);
    // The pack rewrote the neighbour's blocks while its mark stood: a query beside it answers
    // exactly.
    assert_eq!(top_ten(&conn, &beside), exact_top_ten(&live, &beside));
    optimize(&conn, "v_ivf");
    let after = check(&conn, "v_ivf").unwrap();
    assert!(after.consistent() && after.marks == 0, "{after:?}");
    for q in &centres {
        assert_eq!(top_ten(&conn, q), exact_top_ten(&live, q));
    }
}

/// A mark is no bookkeeping: a query of a marked partition scores the blocks held for it under
/// the version its bookkeeping row holds, read just before the mark. Read as bookkeeping, the
/// mark's zeros put the held blocks aside and held none in their place, so none of the
/// partition's packed codes was scored. The query's answer is twelve rows far out along the
/// first cluster's centre, packed in one partition; the mark is planted beside a row coded into
/// the partition's tail, a write of the partition, which a split's mark is too, so the query
/// reads the partition's rows again, the mark among them.
#[test]
fn a_query_of_a_marked_partition_scores_the_blocks_held_for_it() {
    let (_db, conn, mut live, centres) = two_clusters("ivf-rq-marked-query.db");
    let far: Vec<f32> = centres[0].iter().map(|x| 3.0 * x).collect();
    let outliers = crowd(20_000, 12, &far);
    insert_all(&conn, &outliers);
    live.extend(outliers);
    optimize(&conn, "v_ivf");
    let packed: HashSet<i64> = backing(&conn, "vectors", "location")
        .iter()
        .filter(|r| r[0].as_int().unwrap() >= 20_000)
        .map(|r| match Location::decode(r[1].as_int().unwrap()) {
            Location::Block(key) => partition_of(key),
            Location::Tail(key) => panic!("an outlier's code is in the tail of partition {}", partition_of(key)),
        })
        .collect();
    assert_eq!(packed.len(), 1, "the outliers are packed in partitions {packed:?}");
    let partition = *packed.iter().next().unwrap();
    let q = &centres[0];
    let exact = exact_top_ten(&live, q);
    assert!(exact.iter().all(|id| *id >= 20_000), "{exact:?}");
    // The query reads the partition's blocks and holds them.
    assert_eq!(top_ten(&conn, q), exact);
    let centroid = live_centroids(&conn).into_iter().find(|(p, _)| *p == partition).map(|(_, c)| c).unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.start_nested();
    let planted = conn.execute(format!("INSERT INTO __turso_internal_v_ivf_partitions (rowid, c0, c1, c2, c3, c4, c5, c6, c7) VALUES ({}, 0, 0, 0, 0, 0, 0, 0, 0)", mark_key(partition, 0).unwrap()));
    conn.end_nested();
    planted.unwrap();
    STATEMENT_UPKEEP.with(|on| on.set(false));
    insert_all(&conn, &[(30_000, centroid.clone())]);
    STATEMENT_UPKEEP.with(|on| on.set(true));
    conn.execute("COMMIT").unwrap();
    live.insert(30_000, centroid);
    let tail = backing(&conn, "vectors", "location").iter().find(|r| r[0].as_int() == Some(30_000)).map(|r| Location::decode(r[1].as_int().unwrap()));
    assert!(matches!(tail, Some(Location::Tail(key)) if partition_of(key) == partition), "{tail:?}");
    assert_eq!(check(&conn, "v_ivf").unwrap().marks, 1);
    let exact = exact_top_ten(&live, q);
    for round in 0..2 {
        assert_eq!(top_ten(&conn, q), exact, "partition {partition} marked, round {round}");
    }
    optimize(&conn, "v_ivf");
    let found = check(&conn, "v_ivf").unwrap();
    assert!(found.consistent() && found.marks == 0, "{found:?}");
}

/// A database, a connection to it, the rows of its table by id, and a row a transaction still
/// open rewrote with its new vector.
type Outgrown = (Arc<turso_core::Database>, Arc<turso_core::Connection>, Arc<turso_core::Connection>, HashMap<i64, Vec<f32>>, (i64, Vec<f32>));

/// An MVCC index holding 3,000 rows in one partition's tail, unmaintained, so a rebuild is due,
/// beside a transaction kept open that rewrote one of the rows, its maintenance off: a write
/// of a row every rebuild rewrites.
fn outgrown_beside_a_holder(name: &str) -> Outgrown {
    let (db, conn) = database(name, true);
    let mut state = 281;
    let rows: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, random(&mut state, 16))).collect();
    STATEMENT_UPKEEP.with(|on| on.set(false));
    conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
    insert_all(&conn, &rows);
    let holder = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
    holder.execute("BEGIN CONCURRENT").unwrap();
    let moved = (7, random(&mut state, 16));
    holder.execute(format!("UPDATE v SET e = {} WHERE id = {}", literal(&moved.1), moved.0)).unwrap();
    STATEMENT_UPKEEP.with(|on| on.set(true));
    (db, conn, holder, rows.into_iter().collect(), moved)
}

/// The redo rows of `rowid` `conn` sees.
fn redo_rows_of(conn: &Arc<turso_core::Connection>, rowid: i64) -> usize {
    backing(conn, "redo", "writer").iter().filter(|r| r[0].as_int().unwrap() >> REDO_BITS == rowid).count()
}

/// Insert `rows` in one statement of a transaction of their own, `open` leaving it open: how many
/// of them it left to redo, as its transaction sees them, and the open transaction's connection.
fn insert_left_to_redo(db: &Arc<turso_core::Database>, rows: &[(i64, Vec<f32>)], open: bool) -> (usize, Option<Arc<turso_core::Connection>>) {
    let writer = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
    writer.execute("BEGIN CONCURRENT").unwrap();
    insert_all(&writer, rows);
    let left = rows.iter().filter(|(rowid, _)| redo_rows_of(&writer, *rowid) > 0).count();
    if open {
        return (left, Some(writer));
    }
    writer.execute("COMMIT").unwrap();
    (left, None)
}

/// Under MVCC a rebuild writes every row of the index, so OPTIMIZE INDEX beside another
/// transaction's open write of a row it rewrites announces the rebuild and waits, each step
/// sleeping before the next and doing nothing else: it settles no redo row, and a write another
/// transaction makes meanwhile is left to redo, starting no maintenance. Once the open
/// transaction commits a step makes the rebuild, beside a writer still open whose writes are
/// redo rows alone. The rebuild met the open write before, failing OPTIMIZE INDEX. It is stepped
/// on the connection the test reads from, whose transaction it takes no part in: the reads
/// made meanwhile each end their own, and see the rebuild once it is made.
#[test]
fn a_rebuild_beside_an_open_write_waits_for_it_and_is_made() {
    let (db, conn, holder, mut live, moved) = outgrown_beside_a_holder("ivf-rq-rebuild-beside-a-write.db");
    let mut state = 283;
    REBUILT.with(|count| count.set(0));
    let mut stepped = conn.prepare("OPTIMIZE INDEX v_ivf").unwrap();
    // The first step surveys; the next announces the rebuild, the holder's write in its way.
    assert_eq!(step_to_pause(&db, &mut stepped), Paused::Yielded);
    assert_eq!(step_to_pause(&db, &mut stepped), Paused::Slept);
    let first = vec![(5_000, random(&mut state, 16))];
    let (left, _) = insert_left_to_redo(&db, &first, false);
    assert_eq!(left, 1, "a write beside the announced rebuild");
    // The writer of twenty more stays open. Its maintenance, a job for each row, starts none
    // beside the rebuild: it would settle the first writer's row, a write the rebuild meets.
    let more: Vec<(i64, Vec<f32>)> = (5_001..5_021).map(|id| (id, random(&mut state, 16))).collect();
    let (left, writer) = insert_left_to_redo(&db, &more, true);
    assert_eq!(left, more.len(), "writes beside the announced rebuild");
    assert_eq!(step_to_pause(&db, &mut stepped), Paused::Slept);
    assert_eq!(redo_rows_of(&conn, 5_000), 1, "a step waiting for its rebuild settled a redo row");
    assert_eq!(REBUILT.with(|count| count.get()), 0);
    holder.execute("COMMIT").unwrap();
    live.insert(moved.0, moved.1);
    let mut paused = 0;
    while step_to_pause(&db, &mut stepped) != Paused::Ended {
        paused += 1;
        assert!(paused < 10_000, "OPTIMIZE INDEX did not end");
    }
    assert_eq!(REBUILT.with(|count| count.get()), 1, "the rebuild once the holder committed");
    writer.unwrap().execute("COMMIT").unwrap();
    live.extend(first);
    live.extend(more);
    optimize(&conn, "v_ivf");
    let found = check(&conn, "v_ivf").unwrap();
    assert!(found.consistent() && found.codes as usize == live.len() && found.pending == 0, "{found:?}");
    for _ in 0..3 {
        let q = random(&mut state, 16);
        assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q));
    }
}

/// OPTIMIZE INDEX waits for the writes before an announced rebuild no longer than its steps
/// waiting in a row: past them it ends, the rebuild still due, and once it has ended another
/// transaction's write is coded again, not left to redo.
#[test]
fn optimize_waiting_on_an_open_write_ends_with_the_rebuild_due() {
    let (db, conn, holder, mut live, moved) = outgrown_beside_a_holder("ivf-rq-rebuild-waits-out.db");
    let mut state = 287;
    REBUILT.with(|count| count.set(0));
    let mut stepped = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap().prepare("OPTIMIZE INDEX v_ivf").unwrap();
    let mut slept = 0;
    loop {
        match step_to_pause(&db, &mut stepped) {
            Paused::Ended => break,
            Paused::Slept => slept += 1,
            Paused::Yielded => {}
        }
        assert!(slept < 1_000, "OPTIMIZE INDEX waited on and on");
    }
    assert!(slept > 0 && REBUILT.with(|count| count.get()) == 0, "slept {slept} steps");
    let fresh = vec![(5_000, random(&mut state, 16))];
    let (left, _) = insert_left_to_redo(&db, &fresh, false);
    assert_eq!(left, 0, "a write once OPTIMIZE INDEX ended");
    holder.execute("COMMIT").unwrap();
    live.insert(moved.0, moved.1);
    live.extend(fresh);
    optimize(&conn, "v_ivf");
    assert_eq!(REBUILT.with(|count| count.get()), 1);
    let found = check(&conn, "v_ivf").unwrap();
    assert!(found.consistent() && found.codes as usize == live.len() && found.pending == 0, "{found:?}");
}

/// A rebuild announced and left unstepped holds writes to redo until its lease runs out, and no
/// longer than its statement lives.
#[test]
fn an_announced_rebuild_left_unstepped_lapses() {
    let (db, conn, holder, mut live, moved) = outgrown_beside_a_holder("ivf-rq-rebuild-lapses.db");
    let mut state = 289;
    let mut stepped = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap().prepare("OPTIMIZE INDEX v_ivf").unwrap();
    assert_eq!(step_to_pause(&db, &mut stepped), Paused::Yielded);
    assert_eq!(step_to_pause(&db, &mut stepped), Paused::Slept);
    let rows: Vec<(i64, Vec<f32>)> = (5_000..5_004).map(|id| (id, random(&mut state, 16))).collect();
    assert_eq!(insert_left_to_redo(&db, &rows[0..1], false).0, 1, "within the lease");
    LEASE.with(|lease| lease.set(std::time::Duration::ZERO));
    assert_eq!(insert_left_to_redo(&db, &rows[1..2], false).0, 0, "past the lease");
    LEASE.with(|lease| lease.set(REBUILD_LEASE));
    assert_eq!(insert_left_to_redo(&db, &rows[2..3], false).0, 1, "within the lease");
    drop(stepped);
    assert_eq!(insert_left_to_redo(&db, &rows[3..4], false).0, 0, "once the statement is dropped");
    holder.execute("COMMIT").unwrap();
    live.insert(moved.0, moved.1);
    live.extend(rows);
    optimize(&conn, "v_ivf");
    let found = check(&conn, "v_ivf").unwrap();
    assert!(found.consistent() && found.codes as usize == live.len() && found.pending == 0, "{found:?}");
}

/// A statement that inserts rows enough to outgrow the index makes the rebuild they call for
/// itself only where no other transaction has written the index unseen: beside another's open
/// write of a row it rewrites, it leaves the rebuild to OPTIMIZE INDEX and commits, where its
/// rebuild met that write before, failing the statement. In a quiet index it makes the rebuild,
/// and another transaction's write after it, its own transaction still open, is coded, not left
/// to redo.
#[test]
fn a_statement_beside_an_open_write_leaves_its_rebuild_to_optimize() {
    for quiet in [false, true] {
        let (db, conn) = database(&format!("ivf-rq-statement-rebuild-{quiet}.db"), true);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let mut state = 293;
        let first: Vec<(i64, Vec<f32>)> = (1..=100i64).map(|id| (id, random(&mut state, 16))).collect();
        insert_all(&conn, &first);
        let mut live: HashMap<i64, Vec<f32>> = first.into_iter().collect();
        let holder = db.connect(turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser)).unwrap();
        let moved = random(&mut state, 16);
        if !quiet {
            holder.execute("BEGIN CONCURRENT").unwrap();
            STATEMENT_UPKEEP.with(|on| on.set(false));
            holder.execute(format!("UPDATE v SET e = {} WHERE id = 7", literal(&moved))).unwrap();
            STATEMENT_UPKEEP.with(|on| on.set(true));
        }
        // Rows enough to outgrow the index's one partition, in one statement doing one job.
        let bulk: Vec<(i64, Vec<f32>)> = (1_000..3_000i64).map(|id| (id, random(&mut state, 16))).collect();
        conn.execute("BEGIN CONCURRENT").unwrap();
        STATEMENT_BUDGET.with(|budget| budget.set(1));
        REBUILT.with(|count| count.set(0));
        insert_all(&conn, &bulk);
        STATEMENT_BUDGET.with(|budget| budget.set(usize::MAX));
        assert_eq!(REBUILT.with(|count| count.get()), quiet as usize, "quiet={quiet}: the rebuilds the statement made");
        live.extend(bulk);
        let fresh = vec![(5_000, random(&mut state, 16))];
        let (left, _) = insert_left_to_redo(&db, &fresh, false);
        assert_eq!(left, 0, "quiet={quiet}: a write beside the statement's open transaction");
        live.extend(fresh);
        conn.execute("COMMIT").unwrap();
        if !quiet {
            holder.execute("COMMIT").unwrap();
            live.insert(7, moved);
        }
        optimize(&conn, "v_ivf");
        assert_eq!(REBUILT.with(|count| count.get()), 1, "quiet={quiet}: the rebuilds made");
        let found = check(&conn, "v_ivf").unwrap();
        assert!(found.consistent() && found.codes as usize == live.len() && found.pending == 0, "quiet={quiet}: {found:?}");
        for _ in 0..3 {
            let q = random(&mut state, 16);
            assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "quiet={quiet}");
        }
    }
}
