//! Point lookups by rowid over a table whose rows were checkpointed into the B-tree and then all
//! updated, so every B-tree row is shadowed by a newer MVCC version. An exact seek that walks
//! forward over shadowed rows costs one step per shadowed row after its target.
//!
//!     shadow_seek DIR ROWS
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let (dir, rows): (&str, i64) = (&args[1], args[2].parse()?);
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir)?;
    let database = turso::Builder::new_local(&format!("{dir}/seek.db")).experimental_mvcc_passive_checkpoint(true).build().await?;
    let conn = database.connect()?;
    for pragma in ["PRAGMA journal_mode = 'mvcc'", "PRAGMA mvcc_checkpoint_threshold = -1"] {
        let mut r = conn.query(pragma, ()).await?;
        while r.next().await?.is_some() {}
    }
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB NOT NULL)", ()).await?;
    for start in (1..=rows).step_by(1000) {
        conn.execute("BEGIN", ()).await?;
        for id in start..(start + 1000).min(rows + 1) {
            conn.execute(&format!("INSERT INTO t VALUES ({id}, randomblob(16))"), ()).await?;
        }
        conn.execute("COMMIT", ()).await?;
    }
    // Move every row into the B-tree, then shadow each one with a newer version.
    let mut r = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;
    while r.next().await?.is_some() {}
    drop(r);
    conn.execute("UPDATE t SET v = randomblob(16)", ()).await?;
    let mut lookups = Vec::with_capacity(rows as usize);
    let started = std::time::Instant::now();
    for id in 1..=rows {
        let one = std::time::Instant::now();
        let mut r = conn.query("SELECT v FROM t WHERE id = ?1", (id,)).await?;
        let found = r.next().await?.is_some();
        while r.next().await?.is_some() {}
        anyhow::ensure!(found, "row {id} not found");
        lookups.push(one.elapsed().as_micros() as u64);
    }
    let total = started.elapsed();
    lookups.sort_unstable();
    let at = |p: f64| lookups[((lookups.len() - 1) as f64 * p).round() as usize] as f64 / 1000.0;
    println!(
        "{}",
        serde_json::json!({
            "rows": rows,
            "lookups_ms_total": total.as_secs_f64() * 1000.0,
            "lookup_ms_p50": at(0.5),
            "lookup_ms_p99": at(0.99),
            "lookup_ms_max": at(1.0),
        })
    );
    Ok(())
}
