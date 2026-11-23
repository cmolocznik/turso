//! The raw WAL API through `turso::Connection` (`conn_raw_api`): a replicator reads a database's committed frames
//! and appends them to a fresh copy, which then reads every row the source committed.
#![cfg(feature = "conn_raw_api")]

use turso::Builder;

#[tokio::test]
async fn committed_frames_read_from_one_database_rebuild_its_rows_in_another() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.db");
    let source = Builder::new_local(source_path.to_str().unwrap()).build().await.unwrap();
    let conn = source.connect().unwrap();
    conn.execute("PRAGMA journal_mode='wal'", ()).await.unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ()).await.unwrap();
    for i in 0..20 {
        conn.execute("INSERT INTO t (id, v) VALUES (?1, ?2)", (i, format!("row {i}")))
            .await
            .unwrap();
    }
    let state = conn.wal_state().unwrap();
    assert!(state.max_frame > 0, "the writes left frames in the WAL");
    let page_size = 4096 + 24;
    let mut frames = Vec::new();
    let mut last_commit = 0;
    for frame_no in 1..=state.max_frame {
        let mut frame = vec![0u8; page_size];
        let info = conn.wal_get_frame(frame_no, &mut frame).unwrap();
        if info.is_commit_frame() {
            last_commit = frame_no;
        }
        frames.push(frame);
    }
    assert_eq!(last_commit, state.max_frame, "the WAL ends on a commit frame");

    let copy_path = dir.path().join("copy.db");
    let copy = Builder::new_local(copy_path.to_str().unwrap()).build().await.unwrap();
    let copy_conn = copy.connect().unwrap();
    copy_conn.execute("PRAGMA journal_mode='wal'", ()).await.unwrap();
    copy_conn.wal_insert_begin().unwrap();
    for (index, frame) in frames.iter().enumerate() {
        copy_conn.wal_insert_frame(index as u64 + 1, frame).unwrap();
    }
    copy_conn.wal_insert_end(false).unwrap();
    let reader = copy.connect().unwrap();
    let mut rows = reader.query("SELECT count(*), max(v) FROM t", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 20);
    assert_eq!(row.get::<String>(1).unwrap(), "row 9");
}
