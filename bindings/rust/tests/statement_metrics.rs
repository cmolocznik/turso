//! A prepared statement's execution metrics through `turso::Statement` (`statement_metrics`):
//! a range read off an index reports the rows it walked, an unindexed ORDER BY reports its
//! sort, and `reset_metrics` zeroes the counters.
#![cfg(feature = "statement_metrics")]

use turso::Builder;

#[tokio::test]
async fn a_statement_reports_the_rows_it_walked_and_its_sorts_and_reset_zeroes_them() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("metrics.db");
    let db = Builder::new_local(path.to_str().unwrap())
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER)",
        (),
    )
    .await
    .unwrap();
    conn.execute("CREATE INDEX t_k ON t (k)", ()).await.unwrap();
    for i in 0..500 {
        conn.execute("INSERT INTO t (k, v) VALUES (?1, ?2)", (i, 500 - i))
            .await
            .unwrap();
    }

    let mut range = conn
        .prepare("SELECT k FROM t INDEXED BY t_k WHERE k >= 100 AND k < 300")
        .await
        .unwrap();
    let mut rows = range.query(()).await.unwrap();
    let mut seen = 0;
    while rows.next().await.unwrap().is_some() {
        seen += 1;
    }
    drop(rows);
    assert_eq!(seen, 200);
    let m = range.metrics().expect("a live statement has metrics");
    assert!(
        m.rows_read >= 200,
        "the range walked {} rows, fewer than it returned",
        m.rows_read
    );
    assert!(
        m.rows_read <= 202,
        "the range walked {} rows for 200 matches",
        m.rows_read
    );
    assert_eq!(m.sort_operations, 0);

    range.reset_metrics();
    let m = range.metrics().unwrap();
    assert_eq!((m.rows_read, m.sort_operations), (0, 0));

    let mut sorted = conn
        .prepare("SELECT id FROM t ORDER BY v LIMIT 5")
        .await
        .unwrap();
    let mut rows = sorted.query(()).await.unwrap();
    while rows.next().await.unwrap().is_some() {}
    drop(rows);
    let m = sorted.metrics().unwrap();
    assert!(
        m.sort_operations >= 1,
        "an unindexed ORDER BY reported no sort: {m:?}"
    );
    assert!(
        m.rows_read >= 500,
        "the sort read {} of 500 rows",
        m.rows_read
    );
}
