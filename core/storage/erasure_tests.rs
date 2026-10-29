//! A deleted row leaves no byte in the database's files once a truncating checkpoint has run. A
//! b-tree page never keeps a byte of a cell it no longer holds, nor a new page a byte of the
//! buffer it was given; under `PRAGMA secure_delete = ON` a freed page is zeroed as well; and the
//! checkpoint empties the WAL and, under MVCC, the logical log. These tests read the files back:
//! every byte of the checkpointed database outside its live structure must be zero, and no value a
//! row stopped holding may be found in any file.

use crate::{
    ConnectOpts, Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect,
};
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};
use std::collections::{BTreeMap, HashSet};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

fn u16_at(bytes: &[u8], at: usize) -> usize {
    u16::from_be_bytes([bytes[at], bytes[at + 1]]) as usize
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(bytes[at..at + 4].try_into().expect("four bytes"))
}

/// A SQLite varint and its length.
fn varint(bytes: &[u8]) -> (u64, usize) {
    let mut value = 0u64;
    for (i, byte) in bytes.iter().take(8).enumerate() {
        value = value << 7 | u64::from(byte & 0x7f);
        if byte & 0x80 == 0 {
            return (value, i + 1);
        }
    }
    (value << 8 | u64::from(bytes[8]), 9)
}

/// A cell's extent from its first byte: its size on its page, its payload's length, the range of
/// the payload held on the page, and the first overflow page holding the rest.
struct Cell {
    size: usize,
    payload: usize,
    local: Range<usize>,
    overflow: Option<u32>,
}

impl Cell {
    fn parse(kind: u8, cell: &[u8], usable: usize) -> Cell {
        if kind == 5 {
            let (_, n) = varint(&cell[4..]);
            return Cell {
                size: (4 + n).max(4),
                payload: 0,
                local: 0..0,
                overflow: None,
            };
        }
        let mut at = if kind == 2 { 4 } else { 0 };
        let (payload, n) = varint(&cell[at..]);
        at += n;
        if kind == 13 {
            at += varint(&cell[at..]).1;
        }
        let payload = payload as usize;
        let max_local = if kind == 13 {
            usable - 35
        } else {
            (usable - 12) * 64 / 255 - 23
        };
        let min_local = (usable - 12) * 32 / 255 - 23;
        if payload <= max_local {
            return Cell {
                size: (at + payload).max(4),
                payload,
                local: at..at + payload,
                overflow: None,
            };
        }
        let spill = min_local + (payload - min_local) % (usable - 4);
        let local = if spill <= max_local { spill } else { min_local };
        Cell {
            size: at + local + 4,
            payload,
            local: at..at + local,
            overflow: Some(u32_at(cell, at + local)),
        }
    }
}

/// A database file read back page by page: which pages each structure holds and which bytes of
/// them it holds.
struct Layout<'a> {
    bytes: &'a [u8],
    page_size: usize,
    usable: usize,
    owned: Vec<Vec<bool>>,
    kinds: Vec<Option<&'static str>>,
    problems: Vec<String>,
}

impl<'a> Layout<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        let page_size = match u16_at(bytes, 16) {
            1 => 65536,
            size => size,
        };
        let usable = page_size - bytes[20] as usize;
        let pages = bytes.len() / page_size;
        Layout {
            bytes,
            page_size,
            usable,
            owned: vec![vec![false; usable]; pages],
            kinds: vec![None; pages],
            problems: Vec::new(),
        }
    }

    fn page(&self, page: u32) -> &'a [u8] {
        let start = (page as usize - 1) * self.page_size;
        &self.bytes[start..start + self.page_size]
    }

    /// Claim `page` for one structure; a page out of the file or held twice is a problem.
    fn claim(&mut self, page: u32, kind: &'static str) -> bool {
        let Some(slot) = (page as usize)
            .checked_sub(1)
            .and_then(|i| self.kinds.get_mut(i))
        else {
            self.problems
                .push(format!("{kind} page {page} is outside the file"));
            return false;
        };
        if let Some(held) = slot {
            self.problems.push(format!(
                "page {page} is both a {held} page and a {kind} page"
            ));
            return false;
        }
        *slot = Some(kind);
        true
    }

    fn own(&mut self, page: u32, bytes: Range<usize>) {
        for owned in &mut self.owned[page as usize - 1][bytes] {
            *owned = true;
        }
    }

    /// Walk the b-tree rooted at `page`, gathering the payload of each table leaf cell into
    /// `payloads` when one is given.
    fn btree(&mut self, page: u32, mut payloads: Option<&mut Vec<Vec<u8>>>) {
        if !self.claim(page, "b-tree") {
            return;
        }
        let bytes = self.page(page);
        let h = if page == 1 { 100 } else { 0 };
        let kind = bytes[h];
        let interior = match kind {
            2 | 5 => true,
            10 | 13 => false,
            _ => {
                self.problems
                    .push(format!("page {page} has b-tree page type {kind}"));
                return;
            }
        };
        let header = if interior { 12 } else { 8 };
        let cells = u16_at(bytes, h + 3);
        self.own(page, 0..h + header + 2 * cells);
        let mut freeblock = u16_at(bytes, h + 1);
        while freeblock != 0 {
            self.own(page, freeblock..freeblock + 4);
            freeblock = u16_at(bytes, freeblock);
        }
        for i in 0..cells {
            let at = u16_at(bytes, h + header + 2 * i);
            let cell = Cell::parse(kind, &bytes[at..], self.usable);
            self.own(page, at..at + cell.size);
            if interior {
                self.btree(u32_at(bytes, at), payloads.as_deref_mut());
            }
            let mut payload = bytes[at + cell.local.start..at + cell.local.end].to_vec();
            if let Some(first) = cell.overflow {
                self.overflow(first, cell.payload - cell.local.len(), &mut payload);
            }
            if let (13, Some(out)) = (kind, payloads.as_deref_mut()) {
                out.push(payload);
            }
        }
        if interior {
            self.btree(u32_at(bytes, h + 8), payloads);
        }
    }

    fn overflow(&mut self, mut page: u32, mut left: usize, payload: &mut Vec<u8>) {
        while left > 0 {
            if !self.claim(page, "overflow") {
                return;
            }
            let bytes = self.page(page);
            let taken = left.min(self.usable - 4);
            self.own(page, 0..4 + taken);
            payload.extend_from_slice(&bytes[4..4 + taken]);
            left -= taken;
            page = u32_at(bytes, 0);
        }
    }

    fn freelist(&mut self) {
        let mut trunk = u32_at(self.bytes, 32);
        while trunk != 0 {
            if !self.claim(trunk, "freelist trunk") {
                return;
            }
            let bytes = self.page(trunk);
            let leaves = u32_at(bytes, 4) as usize;
            self.own(trunk, 0..8 + 4 * leaves);
            for i in 0..leaves {
                self.claim(u32_at(bytes, 8 + 4 * i), "freelist leaf");
            }
            trunk = u32_at(bytes, 0);
        }
    }

    /// The problems found, and every page that holds a nonzero byte no structure holds.
    fn strays(mut self) -> Vec<String> {
        for i in 0..self.owned.len() {
            let page = (i + 1) as u32;
            let bytes = self.page(page);
            let stray: Vec<usize> = (0..self.usable)
                .filter(|at| !self.owned[i][*at] && bytes[*at] != 0)
                .collect();
            if let Some(first) = stray.first() {
                let kind = self.kinds[i].unwrap_or("unreached");
                self.problems.push(format!("page {page} ({kind}): {} nonzero bytes no structure holds, the first at {first}", stray.len()));
            }
        }
        self.problems
    }
}

/// The value of column `column` of a record, when it is an integer.
fn record_integer(record: &[u8], column: usize) -> Option<i64> {
    let (header, mut at) = varint(record);
    let mut offset = header as usize;
    let mut index = 0;
    while at < header as usize {
        let (serial, n) = varint(&record[at..]);
        at += n;
        let len = match serial {
            0 | 8 | 9 => 0,
            1..=4 => serial as usize,
            5 => 6,
            6 | 7 => 8,
            n if n >= 12 => (n as usize - 12) / 2,
            _ => 0,
        };
        if index == column {
            return match serial {
                8 => Some(0),
                9 => Some(1),
                1..=6 => {
                    let raw = &record[offset..offset + len];
                    let unsigned = raw
                        .iter()
                        .fold(0u64, |acc, byte| acc << 8 | u64::from(*byte));
                    let shift = 64 - 8 * len as u32;
                    Some(((unsigned << shift) as i64) >> shift)
                }
                _ => None,
            };
        }
        offset += len;
        index += 1;
    }
    None
}

/// Every problem with the checkpointed database file at `path`: a page held twice, or a nonzero
/// byte that no b-tree, overflow chain or freelist page of it holds.
fn stray_bytes(path: &Path) -> Vec<String> {
    let bytes = std::fs::read(path).unwrap();
    let mut layout = Layout::new(&bytes);
    let mut schema = Vec::new();
    layout.btree(1, Some(&mut schema));
    for record in &schema {
        match record_integer(record, 3) {
            Some(root) if root > 0 => layout.btree(root as u32, None),
            _ => {}
        }
    }
    layout.freelist();
    layout.strays()
}

fn file_bytes(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

const MARK: &[u8] = b"erasemark";

/// Every marker in `bytes`, found in one pass.
fn markers_in(bytes: &[u8]) -> HashSet<String> {
    let mut markers = HashSet::new();
    let mut at = 0;
    while let Some(i) = bytes[at..]
        .windows(MARK.len())
        .position(|window| window == MARK)
    {
        let start = at + i;
        let digits = &bytes
            [(start + MARK.len()).min(bytes.len())..(start + MARK.len() + 10).min(bytes.len())];
        if digits.len() == 10 && digits.iter().all(u8::is_ascii_digit) {
            markers.insert(
                String::from_utf8_lossy(&bytes[start..start + MARK.len() + 10]).into_owned(),
            );
        }
        at = start + 1;
    }
    markers
}

fn open(path: &Path) -> (Arc<Database>, Arc<Connection>) {
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    (db, conn)
}

fn integer(conn: &Arc<Connection>, sql: &str) -> i64 {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut value = None;
    stmt.run_with_row_callback(|row| {
        value = row.get_values().next().and_then(|v| v.as_int());
        Ok(())
    })
    .unwrap();
    value.expect("a row with an integer")
}

/// Rows of `t` written, rewritten and deleted at random, each value carrying markers no other
/// value carries: one at the start of its text, and at both ends of its blob, whose end lies on an
/// overflow page when the blob is long. A value's markers are dead once no row holds it.
struct Workload {
    rng: ChaCha8Rng,
    next: u64,
    rows: BTreeMap<i64, Vec<String>>,
    dead: Vec<String>,
}

impl Workload {
    fn new(seed: u64) -> Self {
        Workload {
            rng: ChaCha8Rng::seed_from_u64(seed),
            next: 0,
            rows: BTreeMap::new(),
            dead: Vec::new(),
        }
    }

    fn marker(&mut self) -> String {
        self.next += 1;
        format!("erasemark{:010}", self.next)
    }

    /// A fresh text and blob for a row, as SQL literals, and their markers.
    fn values(&mut self) -> (String, String, Vec<String>) {
        let markers = vec![self.marker(), self.marker(), self.marker()];
        let text_fill = (self.rng.next_u32() % 300) as usize;
        let text = format!("'{} {}'", markers[0], "t".repeat(text_fill));
        let blob_fill = match self.rng.next_u32() % 4 {
            0 => 6000 + (self.rng.next_u32() % 9000) as usize,
            _ => (self.rng.next_u32() % 700) as usize,
        };
        let mut blob = markers[1].as_bytes().to_vec();
        blob.extend((0..blob_fill).map(|_| b'a' + (self.rng.next_u32() % 26) as u8));
        blob.extend_from_slice(markers[2].as_bytes());
        let hex: String = blob.iter().map(|byte| format!("{byte:02x}")).collect();
        (text, format!("X'{hex}'"), markers)
    }

    fn insert(&mut self, conn: &Arc<Connection>, id: i64) {
        let (text, blob, markers) = self.values();
        conn.execute(format!("INSERT INTO t VALUES ({id}, {text}, {blob})"))
            .unwrap();
        self.rows.insert(id, markers);
    }

    fn update(&mut self, conn: &Arc<Connection>, id: i64) {
        let (text, blob, markers) = self.values();
        conn.execute(format!(
            "UPDATE t SET a = {text}, b = {blob} WHERE id = {id}"
        ))
        .unwrap();
        let old = self.rows.insert(id, markers).expect("an updated row");
        self.dead.extend(old);
    }

    fn delete(&mut self, conn: &Arc<Connection>, ids: Range<i64>) {
        conn.execute(format!(
            "DELETE FROM t WHERE id >= {} AND id < {}",
            ids.start, ids.end
        ))
        .unwrap();
        let gone: Vec<i64> = self.rows.range(ids).map(|(id, _)| *id).collect();
        for id in gone {
            let old = self.rows.remove(&id).expect("a deleted row");
            self.dead.extend(old);
        }
    }

    fn pick(&mut self) -> Option<i64> {
        let ids: Vec<i64> = self.rows.keys().copied().collect();
        (!ids.is_empty()).then(|| ids[self.rng.next_u32() as usize % ids.len()])
    }

    /// Inserts, rewrites and deletes, some of them checkpointed into the file before they are
    /// deleted, then the deletion of most rows, checkpointed.
    fn run(&mut self, conn: &Arc<Connection>) {
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b BLOB)")
            .unwrap();
        conn.execute("CREATE INDEX t_a ON t(a)").unwrap();
        for round in 0..12i64 {
            for id in round * 40..round * 40 + 40 {
                self.insert(conn, id);
            }
            for _ in 0..15 {
                if let Some(id) = self.pick() {
                    self.update(conn, id);
                }
            }
            for _ in 0..8 {
                if let Some(id) = self.pick() {
                    let span = 1 + (self.rng.next_u32() % 4) as i64;
                    self.delete(conn, id..id + span);
                }
            }
            if round % 3 == 2 {
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            }
        }
        self.delete(conn, 30..400);
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
}

/// What a workload left in the files: the stray bytes of the database file, the bytes left in the
/// WAL and the logical log, and the dead markers found anywhere.
struct Left {
    strays: Vec<String>,
    wal: usize,
    log: usize,
    dead_found: Vec<String>,
    live_missed: Vec<String>,
}

fn erase(dir: &Path, mvcc: bool, secure: &str) -> Left {
    let path = dir.join("erase.db");
    let (db, conn) = open(&path);
    if mvcc {
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    }
    conn.execute(format!("PRAGMA secure_delete = {secure}"))
        .unwrap();
    let mut workload = Workload::new(0x5ec0_de1e);
    workload.run(&conn);
    drop(conn);
    drop(db);
    let files: Vec<Vec<u8>> = ["erase.db", "erase.db-wal", "erase.db-log"]
        .iter()
        .map(|name| file_bytes(&dir.join(name)))
        .collect();
    let in_db = markers_in(&files[0]);
    let anywhere: HashSet<String> = files.iter().flat_map(|f| markers_in(f)).collect();
    let dead_found = workload
        .dead
        .iter()
        .filter(|m| anywhere.contains(*m))
        .cloned()
        .collect();
    let live_missed = workload
        .rows
        .values()
        .flatten()
        .filter(|m| !in_db.contains(*m))
        .cloned()
        .collect();
    Left {
        strays: stray_bytes(&path),
        wal: files[1].len(),
        log: files[2].len(),
        dead_found,
        live_missed,
    }
}

fn scratch() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// Under `secure_delete = ON`, rows inserted, rewritten and deleted, many of them checkpointed into
/// the file before they went, leave no byte in the database file outside its live structure and
/// no value of theirs in any file, while every live value is found: under WAL and under MVCC.
#[test]
fn deleted_rows_leave_no_byte_in_the_files_after_a_truncating_checkpoint() {
    for mvcc in [false, true] {
        let dir = scratch();
        let left = erase(dir.path(), mvcc, "ON");
        assert!(
            left.live_missed.is_empty(),
            "mvcc={mvcc}: the scan missed live values {:?}",
            &left.live_missed[..left.live_missed.len().min(5)]
        );
        assert!(left.strays.is_empty(), "mvcc={mvcc}: {:#?}", left.strays);
        assert_eq!(
            (left.wal, left.log),
            (0, 0),
            "mvcc={mvcc}: the WAL and the log keep bytes"
        );
        assert!(
            left.dead_found.is_empty(),
            "mvcc={mvcc}: {} dead values left, the first {:?}",
            left.dead_found.len(),
            &left.dead_found[..left.dead_found.len().min(5)]
        );
    }
}

/// The same workload under `secure_delete = OFF` leaves the pages it freed as they were: the
/// checker finds their bytes and the scan the dead values on them, so neither passes for want of
/// looking. Inside the pages still in use nothing is left.
#[test]
fn freed_pages_keep_their_bytes_unless_secure_delete_is_on() {
    for mvcc in [false, true] {
        let dir = scratch();
        let left = erase(dir.path(), mvcc, "OFF");
        assert!(
            left.live_missed.is_empty(),
            "mvcc={mvcc}: the scan missed live values"
        );
        assert!(
            !left.dead_found.is_empty(),
            "mvcc={mvcc}: no dead value on the freed pages"
        );
        assert!(
            left.strays.iter().all(|s| s.contains("(freelist")),
            "mvcc={mvcc}: bytes left in pages still in use: {:#?}",
            left.strays
        );
        assert!(
            !left.strays.is_empty(),
            "mvcc={mvcc}: the checker found no freed page's bytes"
        );
    }
}

/// Dropping a table and its index, and clearing a table with a DELETE that names no row, leaves
/// none of their values in the file under `secure_delete = ON`.
#[test]
fn dropped_and_cleared_tables_leave_no_byte_in_the_file() {
    for mvcc in [false, true] {
        let dir = scratch();
        let path = dir.path().join("erase.db");
        let (db, conn) = open(&path);
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute("PRAGMA secure_delete = ON").unwrap();
        let mut workload = Workload::new(7);
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b BLOB)")
            .unwrap();
        conn.execute("CREATE INDEX t_a ON t(a)").unwrap();
        for id in 0..300 {
            workload.insert(&conn, id);
        }
        conn.execute("CREATE TABLE kept(id INTEGER PRIMARY KEY, a TEXT, b BLOB)")
            .unwrap();
        conn.execute("INSERT INTO kept SELECT * FROM t").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("DROP TABLE t").unwrap();
        conn.execute("DELETE FROM kept").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        drop(conn);
        drop(db);
        let markers: Vec<String> = workload.rows.into_values().flatten().collect();
        let in_file = markers_in(&file_bytes(&path));
        let left: Vec<&String> = markers.iter().filter(|m| in_file.contains(*m)).collect();
        assert!(
            left.is_empty(),
            "mvcc={mvcc}: {} values left, the first {:?}",
            left.len(),
            left.first()
        );
        let strays = stray_bytes(&path);
        assert!(strays.is_empty(), "mvcc={mvcc}: {strays:#?}");
    }
}

/// `PRAGMA secure_delete` reads 0, 1 or 2, takes OFF, ON, FAST or those numbers, refuses any other
/// number, and holds for every connection to the database.
#[test]
fn secure_delete_is_set_for_the_database() {
    let dir = scratch();
    let path = dir.path().join("pragma.db");
    let (db, conn) = open(&path);
    let other = db.connect(ConnectOpts::new()).unwrap();
    assert_eq!(integer(&conn, "PRAGMA secure_delete"), 0);
    for (value, code) in [
        ("ON", 1),
        ("FAST", 2),
        ("0", 0),
        ("1", 1),
        ("2", 2),
        ("OFF", 0),
        ("true", 1),
    ] {
        conn.execute(format!("PRAGMA secure_delete = {value}"))
            .unwrap();
        assert_eq!(integer(&conn, "PRAGMA secure_delete"), code, "{value}");
        assert_eq!(
            integer(&other, "PRAGMA main.secure_delete"),
            code,
            "{value}, read by another connection"
        );
    }
    assert!(conn.execute("PRAGMA secure_delete = 3").is_err());
}

/// Pages taken back off the freelist leave no pointer to them on its trunk page past the leaves it
/// still lists.
#[test]
fn pages_taken_off_the_freelist_leave_no_pointer_behind() {
    for mvcc in [false, true] {
        let dir = scratch();
        let path = dir.path().join("erase.db");
        let (db, conn) = open(&path);
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute("PRAGMA secure_delete = ON").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)")
            .unwrap();
        for id in 0..20 {
            conn.execute(format!("INSERT INTO t VALUES ({id}, zeroblob(20000))"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("DELETE FROM t").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let free = integer(&conn, "PRAGMA freelist_count");
        conn.execute("INSERT INTO t VALUES (100, zeroblob(9000))")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let taken = free - integer(&conn, "PRAGMA freelist_count");
        drop(conn);
        drop(db);
        assert!(
            free > 20 && taken > 1,
            "mvcc={mvcc}: {free} pages freed, {taken} taken back"
        );
        let strays = stray_bytes(&path);
        assert!(strays.is_empty(), "mvcc={mvcc}: {strays:#?}");
    }
}
