//! The retired file through the checkpoint, the IO and the open (DESIGN.md §6.5; the auditor's
//! #6186 item 3, #6194 and #6269). Each item is appended through the lists' own append, with its
//! writer's slot and start, for a value a versioned write in a real transaction retired: what S5's
//! writes will call. Each test returns its words and fails only once every database is dropped.

use super::*;
use crate::storage::atoll::btree::value::ValueRef;
use crate::storage::atoll::btree::StoredValue;
use crate::storage::atoll::txn::versioned::HEADER;
use crate::storage::atoll::txn::Ts;
use crate::LimboError;

type Words<T> = std::result::Result<T, String>;

macro_rules! ensure {
    ($cond:expr, $($words:tt)+) => {
        if !$cond {
            return Err(format!($($words)+));
        }
    };
}

/// Bytes of a value that lies out of line.
const VALUE: usize = 5_000;

/// Items written by a checkpoint are read back by the open after a kill, into the owed list in
/// the record's order, each value's page marked retired, and every row reads its newest value.
#[test]
fn a_kill_after_a_checkpoint_opens_with_its_items_in_the_owed_list() {
    words(|| {
        let crash = Arc::new(CrashIO::new(11));
        let db = Db::create(crash.clone());
        let retired = retire(&db, 0..6)?;
        checkpoint(&db)?;
        let root = db.tree.root();
        drop(db);
        let db = reopen(&crash, root)?;
        let owed = owed(&db)?;
        ensure!(
            owed == retired,
            "the owed list {owed:?}, where {retired:?} were retired"
        );
        for r in &retired {
            ensure!(
                db.db.store().retired(r.pid),
                "{:?} not marked retired",
                r.pid
            );
        }
        rows_read_back(&db, 6)
    })
}

/// After an open, a checkpoint that writes takes only blocks the record in charge holds nowhere: its
/// own blocks among them, read at the open. Killed before each write of the retired file or the
/// control file lands, and every third operation, each image opens with the record the open read
/// or the checkpoint's own, whole, and every row reads back (the auditor's #6229, as the open
/// builds its free blocks since #6308's finding 3; mutant: the record's own blocks left out of
/// what the open holds).
#[test]
fn a_checkpoint_after_an_open_killed_anywhere_leaves_the_record_the_open_read_whole() {
    words(|| {
        for seed in 0..2 {
            let crash = Arc::new(CrashIO::new(95 + seed));
            let db = Db::create(crash.clone());
            let first = retire(&db, 0..4)?;
            checkpoint(&db)?;
            let root = db.tree.root();
            drop(db);
            let opened = Arc::new(crash.crash(0));
            let db = go("open", || Db::recover(opened.clone(), root, 1))?;
            let second = retire(&db, 4..8)?;
            let both: Vec<ValueRef> = first.iter().chain(&second).copied().collect();
            let retired = path();
            opened.capture(move |p| p == retired || p == BASE, 3, seed);
            checkpoint(&db)?;
            let images = opened.captured();
            drop(db);
            ensure!(images.len() > 2, "seed {seed}: {} images", images.len());
            for (i, image) in images.into_iter().enumerate() {
                let db = go("open", || Db::recover(Arc::new(image), root, 1))?;
                let owed = owed(&db)?;
                let rows = rows_read_back(&db, 8);
                drop(db);
                ensure!(
                    owed == first || owed == both,
                    "seed {seed}, image {i}: the owed list {owed:?}"
                );
                rows.map_err(|e| format!("seed {seed}, image {i}: {e}"))?;
            }
        }
        Ok(())
    })
}

/// A checkpoint with no new item and no head moved writes nothing to the retired file, issues no
/// sync of it, and names the header the one before named; a kill after it opens with that header
/// and the same owed list (the auditor's #6269, 1).
#[test]
fn a_checkpoint_with_nothing_new_leaves_the_retired_file_alone_and_names_the_header_before() {
    words(|| {
        let crash = Arc::new(CrashIO::new(12));
        let db = Db::create(crash.clone());
        let retired = retire(&db, 0..3)?;
        checkpoint(&db)?;
        let before = db.db.checkpointer().superblock();
        ensure!(
            before.retired.is_some(),
            "the first checkpoint named no header"
        );
        // Any write or sync of the retired file now fails the checkpoint.
        crash.set_faults_on(Some(&path()));
        crash.set_faults(Faults {
            read: 0.0,
            write: 1.0,
            sync: 1.0,
            short_write: 0.0,
        });
        checkpoint(&db)?;
        crash.set_faults(Faults {
            read: 0.0,
            write: 0.0,
            sync: 0.0,
            short_write: 0.0,
        });
        let after = db.db.checkpointer().superblock();
        ensure!(
            after.checkpoint > before.checkpoint && after.retired == before.retired,
            "checkpoint {} names {:?}, where {} named {:?}",
            after.checkpoint,
            after.retired,
            before.checkpoint,
            before.retired
        );
        let root = db.tree.root();
        drop(db);
        let db = reopen(&crash, root)?;
        ensure!(
            db.db.checkpointer().superblock().retired == before.retired,
            "the open's header"
        );
        let owed = owed(&db)?;
        ensure!(owed == retired, "the owed list {owed:?}, where {retired:?}");
        Ok(())
    })
}

/// A superblock naming a header place that holds another checkpoint's header, or none, fails the
/// open as corrupt (the auditor's #6269, 1).
#[test]
fn a_superblock_naming_another_checkpoints_header_or_none_fails_the_open() {
    words(|| {
        let crash = Arc::new(CrashIO::new(13));
        let db = Db::create(crash.clone());
        retire(&db, 0..2)?;
        checkpoint(&db)?;
        let Some((place, checkpoint)) = db.db.checkpointer().superblock().retired else {
            return Err("no header named".into());
        };
        drop(db);
        for (what, named) in [
            ("another checkpoint's header", (place, checkpoint - 1)),
            ("a place holding none", (1 - place, checkpoint)),
        ] {
            let image: Arc<dyn IO> = Arc::new(crash.crash(0));
            control::rewrite_in_charge(image.clone(), BASE, |sb| sb.retired = Some(named));
            match try_open(image, 1) {
                Err(LimboError::Corrupt(_)) => {}
                Err(e) => return Err(format!("{what}: {e}")),
                Ok(_) => return Err(format!("{what}: the open went on")),
            }
        }
        Ok(())
    })
}

/// A superblock naming, under another checkpoint's number, a header whose record names no list
/// fails the open as corrupt: with no record block to read, only the header's own checkpoint
/// refuses it (the auditor's #6269, 1; mutant: the header's checkpoint not compared at the open).
#[test]
fn a_superblock_naming_a_header_of_no_lists_under_another_checkpoint_fails_the_open() {
    words(|| {
        let crash = Arc::new(CrashIO::new(14));
        let db = Db::create(crash.clone());
        retire(&db, 0..2)?;
        checkpoint(&db)?;
        let root = db.tree.root();
        drop(db);
        let opened = Arc::new(crash.crash(0));
        let db = go("open", || Db::recover(opened.clone(), root, 1))?;
        let owed = db.db.checkpointer().retired().lists().owed();
        let freed = free(&db, owed, 1 << 20)?;
        checkpoint(&db)?;
        let named = db.db.checkpointer().superblock().retired;
        let left = owed_items(&db)?;
        drop(db);
        let Some((place, checkpoint)) = named else {
            return Err("the checkpoint after the pass named no header".into());
        };
        ensure!(
            freed == 2 && left == 0,
            "the pass freed {freed} pages, {left} items left"
        );
        let image: Arc<dyn IO> = Arc::new(opened.crash(0));
        control::rewrite_in_charge(image.clone(), BASE, |sb| {
            sb.retired = Some((place, checkpoint - 1))
        });
        match try_open(image, 1) {
            Err(LimboError::Corrupt(_)) => Ok(()),
            Err(e) => Err(format!("the open failed otherwise: {e}")),
            Ok(_) => Err("the open went on".into()),
        }
    })
}

/// A free of the head item's pages made after a cut and before that checkpoint is durable, then
/// a kill with that checkpoint in charge: the item is in the file with the progress the cut
/// recorded, so the open marks, and its pass frees, exactly the pages the cut had not seen freed
/// (the auditor's #6186, item 3). The value is imaged before it is replaced, so the image keeps it
/// and its item is written (DESIGN.md §6.5).
#[test]
fn a_free_after_the_cut_leaves_the_progress_the_cut_recorded_and_the_open_frees_the_rest() {
    words(|| {
        let crash = Arc::new(CrashIO::new(15));
        let db = Db::create(crash.clone());
        let len = crate::storage::atoll::btree::value::chunk() + 12_345;
        db.put("big", &vec![7; len]);
        checkpoint(&db)?;
        let (txn, old) = retiring(&db, "big", vec![8; len])?;
        let slot = txn.slot();
        commit(&db, txn)?;
        let pages = value_pages(&db, old.ok_or("big not out of line")?)?;
        ensure!(pages.len() == 3, "the value's pages {pages:?}");
        ensure!(
            free(&db, slot, 1)? == 1,
            "its first chunk freed before the cut"
        );
        let checkpointer = db.db.checkpointer();
        let txns = db.db.transactions();
        let mut ticket = checkpointer.request_full();
        until_cut(&db)?;
        ensure!(
            free(&db, slot, 1)? == 1,
            "its second chunk freed after the cut"
        );
        go("checkpoint", || {
            drive(&db.io, || checkpointer.step_full(&mut ticket, txns))
        })?;
        let root = db.tree.root();
        drop(db);
        let db = reopen(&crash, root)?;
        let marked: Vec<bool> = pages.iter().map(|&p| db.db.store().retired(p)).collect();
        ensure!(
            marked == [true, false, true],
            "the directory, first and second chunk marked by the open: {marked:?}"
        );
        let owed = db.db.checkpointer().retired().lists().owed();
        let freed = free(&db, owed, 10)?;
        ensure!(
            freed == 2,
            "the owed pass freed {freed} pages, where the cut had seen one of three"
        );
        let read = db.read("big")?;
        ensure!(read == Some(vec![8; len]), "big reads back");
        Ok(())
    })
}

/// A kill anywhere in a checkpoint that writes new items: before each write of the retired file
/// or of the control file lands, and every few operations, each image keeping or losing what was
/// not synced. Each opens with the record before or the checkpoint's own, whole, and every row
/// reads back. The images taken before the control file's write come after the retired file's
/// sync: a kill between that sync and the superblock (the auditor's #6269).
#[test]
fn a_kill_anywhere_in_a_checkpoint_that_writes_items_opens_with_one_whole_record() {
    words(|| {
        for seed in 0..3 {
            let crash = Arc::new(CrashIO::new(16 + seed));
            let db = Db::create(crash.clone());
            let first = retire(&db, 0..4)?;
            checkpoint(&db)?;
            let second = retire(&db, 4..8)?;
            let both: Vec<ValueRef> = first.iter().chain(&second).copied().collect();
            let retired = path();
            crash.capture(move |p| p == retired || p == BASE, 3, seed);
            checkpoint(&db)?;
            let images = crash.captured();
            let root = db.tree.root();
            drop(db);
            ensure!(images.len() > 2, "seed {seed}: {} images", images.len());
            for (i, image) in images.into_iter().enumerate() {
                let db = go("open", || Db::recover(Arc::new(image), root, 1))?;
                let owed = owed(&db)?;
                ensure!(
                    owed == first || owed == both,
                    "seed {seed}, image {i}: the owed list {owed:?}"
                );
                rows_read_back(&db, 8)?;
            }
        }
        Ok(())
    })
}

/// A write of the retired file that fails fails its checkpoint, and the open after it reads the
/// record before, whole (the auditor's #6269).
#[test]
fn a_checkpoint_whose_retired_write_fails_leaves_the_record_before_in_charge() {
    words(|| {
        let crash = Arc::new(CrashIO::new(19));
        let db = Db::create(crash.clone());
        let first = retire(&db, 0..3)?;
        checkpoint(&db)?;
        retire(&db, 3..6)?;
        crash.set_faults_on(Some(&path()));
        crash.set_faults(faults(1.0));
        let failed = checkpoint(&db);
        crash.set_faults(faults(0.0));
        ensure!(
            failed.is_err(),
            "the checkpoint went on past its failed write"
        );
        let root = db.tree.root();
        drop(db);
        let db = reopen(&crash, root)?;
        let owed = owed(&db)?;
        ensure!(
            owed == first,
            "the owed list {owed:?}, where the record before held {first:?}"
        );
        rows_read_back(&db, 6)
    })
}

/// A checkpoint's write of the file that fails moves nothing and frees the blocks it took, so the
/// next writes the same items again into the same blocks, and its record reads back whole (the
/// auditor's #6269). A failed checkpoint stops every later one until the database opens again, so
/// this test drives the retired file's own steps by hand, as the checkpoint makes them (accepted
/// in #6282).
#[test]
fn a_failed_write_of_the_retired_file_moves_nothing_and_the_next_writes_the_same_items() {
    words(|| {
        let crash = Arc::new(CrashIO::new(20));
        let db = Db::create(crash.clone());
        let items = retire(&db, 0..3)?;
        let retired = db.db.checkpointer().retired();
        let txns = db.db.transactions();
        let c = txns.clock_now();
        let mut run = crate::storage::atoll::gc::RetiredRun::default();
        retired.cut(&mut run);
        crash.set_faults_on(Some(&path()));
        crash.set_faults(faults(1.0));
        let failed = file_steps(&db, &mut run, c);
        crash.set_faults(faults(0.0));
        ensure!(failed.is_err(), "the failed write went unseen");
        let taken = retired.taken(&run);
        retired.failed(&mut run);
        while !retired.give_back_taken(&mut run) {}
        let mut again = crate::storage::atoll::gc::RetiredRun::default();
        retired.cut(&mut again);
        go("again", || file_steps(&db, &mut again, c))?;
        ensure!(
            retired.taken(&again) == taken,
            "the blocks taken again {:?}, where the failed write took {taken:?}",
            retired.taken(&again)
        );
        let (place, checkpoint) = retired.written(&again).ok_or("nothing written")?;
        let record = go("record", || retired.read_record(&db.io, place, checkpoint))?;
        let read: Vec<ValueRef> = record.iter().map(|item| item.reference).collect();
        retired.failed(&mut again);
        while !retired.give_back_taken(&mut again) {}
        ensure!(
            read == items,
            "the record's items {read:?}, where {items:?}"
        );
        Ok(())
    })
}

/// A writer committed just above the snapshot whose item was in its list in memory at the cut:
/// its item is not written, so the open's replay applies its body and frees the value it replaced
/// once, and the owed list does not hold it (the auditor's #6269, 3).
#[test]
fn a_writer_committed_just_above_the_snapshot_is_replayed_and_not_owed() {
    words(|| {
        let crash = Arc::new(CrashIO::new(21));
        let db = Db::create(crash.clone());
        db.put("w", &vec![1; VALUE]);
        checkpoint(&db)?;
        let (txn, old) = retiring(&db, "w", vec![2; VALUE])?;
        let old = old.ok_or("w not out of line")?;
        let checkpointer = db.db.checkpointer();
        let txns = db.db.transactions();
        let mut ticket = checkpointer.request_full();
        until_cut(&db)?;
        commit(&db, txn)?;
        go("checkpoint", || {
            drive(&db.io, || checkpointer.step_full(&mut ticket, txns))
        })?;
        let snapshot = checkpointer.superblock().snapshot;
        let root = db.tree.root();
        drop(db);
        let db = reopen(&crash, root)?;
        let owed = owed(&db)?;
        ensure!(
            !owed.contains(&old),
            "the owed list holds a writer's item above {snapshot}"
        );
        let entry = go("entry", || drive(&db.io, || db.db.store().entry(old.pid)))?;
        ensure!(
            entry == crate::storage::atoll::radix::FREE,
            "the value replay replaced is not free: {entry}"
        );
        let read = db.read("w")?;
        ensure!(read == Some(vec![2; VALUE]), "w reads back");
        Ok(())
    })
}

/// Every edit of the image's page table comes before the Retired phase. A checkpoint with edits
/// of both roads (images landing, and a value made and replaced since the last checkpoint left
/// out), stepped a unit at a time, counts the edits each unit makes by the phase it began in: none
/// is made by a unit begun in the Retired phase or after it. So the test build's second judging of
/// the items, once every metadata page is written, reads what the image holds (the auditor's
/// #6288 and #6290).
#[test]
fn no_edit_of_the_images_page_table_comes_once_the_retired_phase_begins() {
    words(|| {
        let db = Db::create(Arc::new(CrashIO::new(41)));
        retire(&db, 0..3)?;
        db.put("a", &vec![9; VALUE]);
        let (txn, _) = retiring(&db, "a", vec![10; VALUE])?;
        commit(&db, txn)?;
        let checkpointer = db.db.checkpointer();
        let store = db.db.store();
        let txns = db.db.transactions();
        let mut ticket = checkpointer.request_full();
        let mut phases: Vec<(String, u64)> = Vec::new();
        let mut units = 0;
        loop {
            let phase = checkpointer.phase_for_test();
            if phase.is_none() && !phases.is_empty() {
                break;
            }
            ensure!(units < 1 << 16, "the checkpoint not done in {units} units");
            units += 1;
            let before = store.cut_edits();
            if let Some(c) = go("unit", || checkpointer.one_unit(txns))? {
                go("wait", || {
                    crate::storage::atoll::park::wait_for_completion(db.io.as_ref(), c)
                })?;
            }
            let Some(phase) = phase else { continue };
            let edits = store.cut_edits() - before;
            match phases.last_mut() {
                Some((last, n)) if *last == phase => *n += edits,
                _ => phases.push((phase, edits)),
            }
        }
        go("checkpoint", || {
            drive(&db.io, || checkpointer.step_full(&mut ticket, txns))
        })?;
        drop(db);
        let retired = phases
            .iter()
            .position(|(phase, _)| phase == "Retired")
            .ok_or_else(|| format!("no Retired phase among {phases:?}"))?;
        let before: u64 = phases[..retired].iter().map(|(_, n)| n).sum();
        let after: Vec<&(String, u64)> = phases[retired..].iter().filter(|(_, n)| *n > 0).collect();
        ensure!(before > 0, "no edit before the Retired phase: {phases:?}");
        ensure!(
            after.is_empty(),
            "edits of the image's page table by units begun at or after the Retired phase: \
             {after:?}, of {phases:?}"
        );
        ensure!(
            phases[retired..].iter().any(|(phase, _)| phase == "Uncut"),
            "the checkpoint did not reach Uncut: {phases:?}"
        );
        Ok(())
    })
}

/// A checkpoint that fails gives the blocks it took back a run a unit: 1,500 blocks in three runs
/// of at most 512, then a call that finds none, and they are taken again in the same order (the
/// auditor's #6308, finding 3: what a failed checkpoint goes through whole, in runs; mutant: all
/// given back in one call).
#[test]
fn a_failed_checkpoint_gives_back_the_blocks_it_took_a_run_a_unit() {
    words(|| {
        let db = Db::create(Arc::new(CrashIO::new(87)));
        let retired = db.db.checkpointer().retired();
        let mut run = crate::storage::atoll::gc::RetiredRun::default();
        retired.cut(&mut run);
        let taken = retired.take_for_test(&mut run, 1_500);
        retired.failed(&mut run);
        let mut runs = 0;
        while !retired.give_back_taken(&mut run) {
            runs += 1;
            ensure!(
                runs < 100,
                "the blocks still not given back after {runs} runs"
            );
        }
        let mut again = crate::storage::atoll::gc::RetiredRun::default();
        let taken_again = retired.take_for_test(&mut again, 1_500);
        retired.failed(&mut again);
        while !retired.give_back_taken(&mut again) {}
        drop(db);
        ensure!(runs == 3, "{runs} runs gave back the 1,500 blocks taken");
        ensure!(
            taken_again == taken,
            "the blocks taken again are not those given back"
        );
        Ok(())
    })
}

/// Items an old reader's later checkpoints take, in each.
const NEW: usize = 10;

/// While an old reader holds a slot's list back, each checkpoint reads the list from where the last
/// one stopped, not from its first page: an old reader holding back 101 items or 1,010 across
/// three checkpoints, the third's Retired phase reads one list page for each of its [`NEW`] items
/// and no other, for either (the auditor's #6308, finding 2; mutant: the place reset each
/// checkpoint). The items held back end at a page's end, 101 to a page, so the second checkpoint
/// reads the page it stopped on once more to find its next.
#[test]
fn a_checkpoint_reads_a_held_back_list_from_where_the_last_one_stopped() {
    words(|| {
        let (few, _) = held_back(91, 101)?;
        let (many, _) = held_back(92, 1_010)?;
        let new = NEW as u64;
        ensure!(
            few[1..] == [new + 1, new] && many[1..] == few[1..],
            "the list pages read by the Retired phase in each of three checkpoints: {few:?} for \
             101 items held back, {many:?} for 1,010"
        );
        Ok(())
    })
}

/// A slot's reading place is used only while the cut's head is behind it. A checkpoint writes
/// items 0 to 9 and leaves the place at item 10, on the list's first page; items 10 to 101 are
/// appended, so the first page (101 items) fills and a second is taken; a pass frees all 102, its
/// head passes the place, and the first page is given back; items 102 to 106 are appended. The
/// next checkpoint reads the list from its head, one list page for each of the five, not from the
/// place's page, and after a kill the open owes exactly those five (the auditor's #6308 finding 2,
/// the place dropped once a give-back passes it; mutant: the place used whatever the head).
#[test]
fn a_reading_place_the_head_has_passed_is_dropped_and_the_list_read_from_its_head() {
    words(|| {
        let crash = Arc::new(CrashIO::new(97));
        let db = Db::create(crash.clone());
        for k in 0..107 {
            db.put(&key(k), &vec![k as u8 + 1; VALUE]);
        }
        checkpoint(&db)?;
        let (slot, _) = replace(&db, 0..10)?;
        checkpoint(&db)?;
        replace(&db, 10..102)?;
        let freed = free(&db, slot, 1 << 20)?;
        let (_, newest) = replace(&db, 102..107)?;
        step_while(&db, |phase| phase.is_some())?;
        crate::storage::atoll::gc::take_list_reads();
        checkpoint(&db)?;
        let reads = crate::storage::atoll::gc::take_list_reads();
        let root = db.tree.root();
        drop(db);
        ensure!(
            freed == 102,
            "the pass freed {freed} pages of the 102 values"
        );
        ensure!(
            reads == 5,
            "the checkpoint after the give-back read {reads} list pages for its 5 items"
        );
        let db = reopen(&crash, root)?;
        let owed = owed(&db);
        let rows = rows_read_back(&db, 107);
        drop(db);
        let owed = owed?;
        ensure!(
            owed == newest,
            "the owed list {owed:?}, where the five newest {newest:?}"
        );
        rows
    })
}

/// Writes each of `keys` again, a transaction of its own each, its item appended as S5's write
/// will, all from one slot: that slot and the references they retired, in order.
fn replace(db: &Db, keys: std::ops::Range<usize>) -> Words<(u16, Vec<ValueRef>)> {
    let mut slot = None;
    let mut out = Vec::new();
    for k in keys {
        let (txn, old) = retiring(db, &key(k), vec![(k as u8).wrapping_add(101); VALUE])?;
        let first = *slot.get_or_insert(txn.slot());
        let this = txn.slot();
        commit(db, txn)?;
        ensure!(
            this == first,
            "key {k} written from slot {this}, the keys before from {first}"
        );
        out.push(old.ok_or_else(|| format!("key {k} not out of line"))?);
    }
    Ok((slot.ok_or("no key")?, out))
}

/// Writes `key`s again in turn, each `fill` bytes of its value's length in a transaction of its
/// own, its item appended as S5's write will, all from one slot: that slot and the references
/// they retired, in order.
fn retire_each(db: &Db, writes: &[(usize, u8)]) -> Words<(u16, Vec<ValueRef>)> {
    let mut slot = None;
    let mut out = Vec::new();
    for &(k, fill) in writes {
        let (txn, old) = retiring(db, &key(k), vec![fill; VALUE])?;
        let first = *slot.get_or_insert(txn.slot());
        let this = txn.slot();
        commit(db, txn)?;
        ensure!(
            this == first,
            "key {k} written from slot {this}, the writes before from {first}"
        );
        out.push(old.ok_or_else(|| format!("key {k} not out of line"))?);
    }
    Ok((slot.ok_or("no write")?, out))
}

/// A checkpoint that fails drops every slot's reading place: its next read walks the list from its
/// first page. Driven by hand at the Retired file's level, as the failed write's test is (a failed
/// checkpoint stops every later one until an open): a run writes 110 items, so the place lies on
/// the list's second page; five more are appended; a run whose writes fault fails; the next run
/// reads the first page once more to reach the second, then the five items (the auditor's #6308
/// finding 2; mutant: failed() keeping the places).
#[test]
fn a_failed_checkpoint_drops_the_reading_places_and_the_next_reads_from_the_lists_first_page() {
    words(|| {
        let crash = Arc::new(CrashIO::new(98));
        let db = Db::create(crash.clone());
        for k in 0..115 {
            db.put(&key(k), &vec![k as u8 + 1; VALUE]);
        }
        checkpoint(&db)?;
        replace(&db, 0..110)?;
        step_while(&db, |phase| phase.is_some())?;
        let retired = db.db.checkpointer().retired();
        let c = db.db.transactions().clock_now();
        let mut first = crate::storage::atoll::gc::RetiredRun::default();
        retired.cut(&mut first);
        go("the first run", || file_steps(&db, &mut first, c))?;
        let written = retired.written(&first);
        retired.durable(&mut first);
        replace(&db, 110..115)?;
        ensure!(
            db.db.checkpointer().phase_for_test().is_none(),
            "a checkpoint began beside the hand-driven runs"
        );
        let c = db.db.transactions().clock_now();
        let mut failing = crate::storage::atoll::gc::RetiredRun::default();
        retired.cut(&mut failing);
        crash.set_faults_on(Some(&path()));
        crash.set_faults(faults(1.0));
        let failed = file_steps(&db, &mut failing, c);
        crash.set_faults(faults(0.0));
        retired.failed(&mut failing);
        while !retired.give_back_taken(&mut failing) {}
        let mut next = crate::storage::atoll::gc::RetiredRun::default();
        retired.cut(&mut next);
        crate::storage::atoll::gc::take_list_reads();
        let stepped = file_steps(&db, &mut next, c);
        let reads = crate::storage::atoll::gc::take_list_reads();
        retired.failed(&mut next);
        while !retired.give_back_taken(&mut next) {}
        drop(db);
        ensure!(written.is_some(), "the first run wrote nothing");
        ensure!(failed.is_err(), "the failing run did not fail");
        go("the next run", || stepped)?;
        ensure!(
            reads == 6,
            "the run after the failure read {reads} list pages: the first page once to reach the \
             second, then 5 items"
        );
        Ok(())
    })
}

/// A slot's list pages from the cut's head on stay while a checkpoint reads them, though a pass
/// passes them, and a give-back after the checkpoint's plan or its failure frees them: the bound
/// the reading place rests on. After a run's cut a pass frees all 203 items (three list pages) and
/// its give-back leaves the three; the run reads every item; once its plan is made, the next
/// step's give-back leaves the last page alone. Then a run cut and failed at once: a pass after it
/// gives back the two pages wholly behind its head. Driven at the Retired file's level (the
/// auditor's #6308 finding 2; mutants: the cut keeping nothing, the plan keeping the bound, a
/// failure keeping it).
#[test]
fn a_list_page_a_pass_passes_stays_until_the_checkpoint_has_read_it_or_failed() {
    words(|| {
        let crash = Arc::new(CrashIO::new(100));
        let db = Db::create(crash);
        for k in 0..203 {
            db.put(&key(k), &vec![k as u8 + 1; VALUE]);
        }
        checkpoint(&db)?;
        let (slot, _) = replace(&db, 0..203)?;
        step_while(&db, |phase| phase.is_some())?;
        let retired = db.db.checkpointer().retired();
        let pages = || go("pages", || retired.lists().pages(&db.io, slot));
        let before = pages()?;
        let c = db.db.transactions().clock_now();
        let mut run = crate::storage::atoll::gc::RetiredRun::default();
        retired.cut(&mut run);
        let freed = free(&db, slot, 1 << 20)?;
        let during = pages()?;
        crate::storage::atoll::gc::take_list_reads();
        let stepped = file_steps(&db, &mut run, c);
        let reads = crate::storage::atoll::gc::take_list_reads();
        free(&db, slot, 1 << 20)?;
        let after_plan = pages()?;
        retired.durable(&mut run);
        replace(&db, 0..203)?;
        let mut failing = crate::storage::atoll::gc::RetiredRun::default();
        retired.cut(&mut failing);
        retired.failed(&mut failing);
        while !retired.give_back_taken(&mut failing) {}
        let freed_again = free(&db, slot, 1 << 20)?;
        let after_failure = pages()?;
        drop(db);
        ensure!(
            before.len() == 3 && freed == 203,
            "{} list pages for 203 items, {freed} values freed",
            before.len()
        );
        ensure!(
            during == before,
            "a pass after the cut left {during:?} of the list pages {before:?}"
        );
        go("the run", || stepped)?;
        ensure!(
            reads == 205,
            "the run read {reads} list pages for 203 items on 3 pages"
        );
        ensure!(
            after_plan == before[2..],
            "a give-back after the run's plan left {after_plan:?} of {before:?}"
        );
        ensure!(
            freed_again == 203 && after_failure.len() == 1,
            "a give-back after a failed run left {after_failure:?}, {freed_again} values freed"
        );
        Ok(())
    })
}

/// A read that ended at a page's end keeps that page and the position past it; once the pass's
/// head reaches that position, its give-back frees the page, and the next checkpoint reads from
/// the head, on the next page. Slot A's 101 items fill its first page, and a checkpoint reads them
/// all; a writer W of A appends item 101, on a second page, and stays open, so the pass frees 0
/// to 100, stops at 101 and gives the first page back; slot B's item 101 takes the freed page,
/// which so holds a position 101 too. W commits, and the next checkpoint reads A's item 101 from
/// its second page; after a kill the open owes W's item and B's 102, and no other (the auditor's
/// #6334 gap 2, the boundary of `head < place.at()`; mutant: `head <= place.at()`).
#[test]
fn a_read_that_ended_at_a_pages_end_goes_on_from_the_head_once_its_page_is_given_back() {
    words(|| {
        let crash = Arc::new(CrashIO::new(102));
        let db = Db::create(crash.clone());
        for k in 0..204 {
            db.put(&key(k), &vec![k as u8 + 1; VALUE]);
        }
        checkpoint(&db)?;
        let (a, _) = replace(&db, 0..101)?;
        checkpoint(&db)?;
        let lists = db.db.checkpointer().retired().lists();
        let pages = |slot| go("pages", || lists.pages(&db.io, slot));
        let a_first = pages(a)?;
        // B's first 101 items, written while a reader holds A's slot.
        let holder = db.begin();
        let held = holder.slot();
        let b_first = replace(&db, 101..202);
        let mut ended = Commit::new(holder);
        go("the holder's end", || {
            drive(&db.io, || db.db.transactions().commit(&mut ended))
        })?;
        let (b, mut b_refs) = b_first?;
        let (w, w_old) = retiring(&db, &key(202), vec![(202u8).wrapping_add(101); VALUE])?;
        let w_slot = w.slot();
        // What the pass frees while W is open, A's pages after it, B's last write's slot and the
        // reference it retired, and B's pages.
        type WhileOpen = (u64, Vec<PageId>, u16, Vec<ValueRef>, Vec<PageId>);
        let while_open = (|| -> Words<WhileOpen> {
            let freed = free(&db, a, 1 << 20)?;
            let a_after = pages(a)?;
            let (b_last, last) = replace(&db, 203..204)?;
            Ok((freed, a_after, b_last, last, pages(b)?))
        })();
        commit(&db, w)?;
        let (freed, a_after, b_last, last, b_pages) = while_open?;
        step_while(&db, |phase| phase.is_some())?;
        crate::storage::atoll::gc::take_list_reads();
        checkpoint(&db)?;
        let reads = crate::storage::atoll::gc::take_list_reads();
        let root = db.tree.root();
        drop(db);
        ensure!(
            held == a && w_slot == a && b != a && b_last == b,
            "slots: A {a}, the holder {held}, W {w_slot}, B {b} and {b_last}"
        );
        ensure!(
            freed == 101 && a_first.len() == 1 && a_after.len() == 1 && a_after != a_first,
            "the pass freed {freed} under W; A's pages {a_first:?}, then {a_after:?}"
        );
        ensure!(
            b_pages.len() == 2 && b_pages[1] == a_first[0],
            "B's pages {b_pages:?}: its second is not A's freed first page {:?}",
            a_first[0]
        );
        ensure!(
            reads == 1 + 103,
            "the checkpoint read {reads} list pages: one for A's item 101, 103 for B's 102 items \
             on two pages"
        );
        let db = reopen(&crash, root)?;
        let owed = owed(&db);
        drop(db);
        let mut owed = owed?;
        b_refs.extend(last);
        b_refs.push(w_old.ok_or("key 202 not out of line")?);
        owed.sort_by_key(|r| (r.pid, r.id));
        b_refs.sort_by_key(|r| (r.pid, r.id));
        ensure!(
            owed == b_refs,
            "the open owes {} items, where W's one and B's 102: {owed:?}",
            owed.len()
        );
        Ok(())
    })
}

/// A checkpoint reads each slot's list from its cut's head, though a pass after the cut frees the
/// items' values and moves the head across pages: the cut keeps the pages from its head on until
/// the plan, and the image owes those values, so the checkpoint writes every item; once it is
/// done, the pass's next give-back frees the pages behind the head. 203 items on three pages; the
/// pass frees all 203 between the cut and the Retired phase; the record then holds the 203, and
/// after a kill the open owes them, in order (the auditor's #6334 gap 3; mutants: the cut keeping
/// nothing, the give-back ignoring the mark, the mark not lifted at the plan).
#[test]
fn a_checkpoint_reads_from_its_cuts_head_the_items_a_pass_frees_after_the_cut_across_pages() {
    words(|| {
        let crash = Arc::new(CrashIO::new(103));
        let db = Db::create(crash.clone());
        for k in 0..203 {
            db.put(&key(k), &vec![k as u8 + 1; VALUE]);
        }
        checkpoint(&db)?;
        let (slot, refs) = replace(&db, 0..203)?;
        step_while(&db, |phase| phase.is_some())?;
        let lists = db.db.checkpointer().retired().lists();
        let pages = || go("pages", || lists.pages(&db.io, slot));
        let before = pages()?;
        let checkpointer = db.db.checkpointer();
        let mut ticket = checkpointer.request_full();
        until_cut(&db)?;
        let phase = checkpointer.phase_for_test();
        let freed = free(&db, slot, 1 << 20)?;
        let during = pages()?;
        crate::storage::atoll::gc::take_list_reads();
        let done = drive(&db.io, || {
            checkpointer.step_full(&mut ticket, db.db.transactions())
        });
        let reads = crate::storage::atoll::gc::take_list_reads();
        free(&db, slot, 1 << 20)?;
        let after = pages()?;
        let items = owed_items(&db)?;
        let root = db.tree.root();
        drop(db);
        ensure!(
            before.len() == 3 && freed == 203,
            "{} list pages for 203 items; {freed} values freed after the cut, in {phase:?}",
            before.len()
        );
        ensure!(
            during == before,
            "a pass after the cut left {during:?} of the list pages {before:?}"
        );
        go("the checkpoint", || done)?;
        ensure!(
            reads == 205 && items == 203,
            "the checkpoint read {reads} list pages for 203 items on 3 pages and wrote {items}"
        );
        ensure!(
            after == before[2..],
            "a give-back after the checkpoint left {after:?} of {before:?}"
        );
        let db = reopen(&crash, root)?;
        let owed = owed(&db);
        drop(db);
        let owed = owed?;
        ensure!(
            owed == refs,
            "the open owes {} items, where the 203 the pass freed after the cut",
            owed.len()
        );
        Ok(())
    })
}

/// A slot's head past an item the image left out, inside one block of the retired file: after a
/// kill the open owes exactly the items not freed. Five items of one slot: position 0 retires key
/// 0's imaged value; position 1 key 0's next value, made and replaced between two checkpoints, so
/// the image leaves it out and the checkpoint writes no item for it; positions 2, 3 and 4 the
/// imaged values of keys 1, 2 and 3. The checkpoint writes items 0, 2, 3 and 4 into one block. A
/// pass frees three pages, the values of positions 0, 1 and 2, so the head stands at position 3;
/// the next checkpoint writes the head; a kill; the open must owe the values of positions 3 and 4
/// (the auditor's #6336, its probe; mutants: the head's place in its block reckoned by positions).
#[test]
fn a_head_past_a_left_out_item_in_its_block_leaves_every_item_not_freed_owed() {
    words(|| left_out_killed_after(132, LeftOut::HeadPast))
}

/// As above with the head part-freed: position 3 retires a chunked value, and the pass frees the
/// values of positions 0, 1 and 2 and the first chunk of position 3's, so the head stands at (3,
/// 1). After a kill the open owes positions 3 and 4, and marks retired every page of both but the
/// chunk the pass freed: the head's progress goes to its own item, never to the one after it (the
/// auditor's #6336; mutants: the head's progress given to the item at its index).
#[test]
fn a_part_freed_head_past_a_left_out_item_gives_its_progress_to_its_own_item() {
    words(|| left_out_killed_after(133, LeftOut::PartFreed))
}

/// A block whose items' positions span more than its count, with the head inside that span, stays
/// in the record: ten items of one slot, positions 0, 5 and 9 kept by the image and the others
/// made and replaced between two checkpoints, so the block holds positions 0, 5 and 9; a pass
/// frees the values of positions 0 to 6, so the head stands at 7, past the block's first item
/// plus its count; the next checkpoint keeps the block, and after a kill the open owes position
/// 9's value alone (the auditor's #6336; mutants: a block judged wholly behind the head by its
/// count).
#[test]
fn a_block_whose_positions_span_past_the_head_stays_in_the_record() {
    words(|| left_out_killed_after(134, LeftOut::Span))
}

/// A head on its block's last item, part-freed, keeps the block: the writes of the first case, the
/// last of them a chunked value, and a pass that frees positions 0 to 3 and the first chunk of
/// position 4's value, so the head stands at (4, 1) on the block's last item. After a kill the
/// open owes position 4's value, marked but for its freed chunk (the auditor's #6343; mutant: a
/// block judged wholly behind the head at its last item).
#[test]
fn a_head_on_its_blocks_last_item_part_freed_keeps_the_block_and_its_progress() {
    words(|| left_out_killed_after(135, LeftOut::LastPartFreed))
}

/// A head the open found by its position is named again by its index: the first case killed after
/// the checkpoint that writes its head (3, 0), and opened, owing positions 3 and 4; the owed list's
/// pass frees one page more, position 3's value; a checkpoint; a kill; the open owes position 4's
/// value alone and every row reads back (the auditor's #6343, the one road where a part found by
/// position is named again by index; mutant: the open holding the part by its position).
#[test]
fn a_head_found_by_its_position_is_named_by_index_after_an_open_and_owed_across_two_opens() {
    words(|| {
        let crash = Arc::new(CrashIO::new(136));
        let made = left_out(&crash, LeftOut::HeadPast)?;
        let done = checkpoint(&made.db);
        let root = made.db.tree.root();
        drop(made.db);
        done?;
        let opened = Arc::new(crash.crash(0));
        let db = go("the first open", || Db::recover(opened.clone(), root, 1))?;
        let first = owed(&db);
        let owed_list = db.db.checkpointer().retired().lists().owed();
        let freed = free(&db, owed_list, 1);
        let done = checkpoint(&db);
        drop(db);
        let (first, freed) = (first?, freed?);
        done?;
        ensure!(
            first == made.second && freed == 1,
            "the first open owes {first:?} and its pass freed {freed} pages, where {:?} and 1",
            made.second
        );
        let db = go("the second open", || {
            Db::recover(Arc::new(opened.crash(0)), root, 1)
        })?;
        let seen = opened_as(&db, &made.writes, &[]);
        drop(db);
        let (owed, _) = seen?;
        ensure!(
            owed == made.second[1..],
            "the second open owes {owed:?}, where position 4's value alone: {:?}",
            &made.second[1..]
        );
        Ok(())
    })
}

/// Each of the four cases of LeftOut killed before each write of the checkpoint that writes the
/// head to the retired file or the control file, at every third operation and after its last
/// write, three seeds each: each image opens with the record before in charge, owing its items, or with the new one, owing
/// exactly the items not freed, its marks as the case's, and every row reads back; each case opens
/// both ways in some image (the auditor's #6338: the head's entry is new bytes on disk).
#[test]
fn a_left_out_case_killed_anywhere_in_the_checkpoint_writing_its_head_owes_one_records_items() {
    words(|| {
        for case in [
            LeftOut::HeadPast,
            LeftOut::PartFreed,
            LeftOut::Span,
            LeftOut::LastPartFreed,
        ] {
            let (mut old, mut new) = (0, 0);
            for seed in 0..3 {
                let crash = Arc::new(CrashIO::new(140 + seed));
                let made = left_out(&crash, case)?;
                let retired = path();
                crash.capture(move |p| p == retired || p == BASE, 3, seed);
                let done = checkpoint(&made.db);
                let mut images = crash.captured();
                // And the kill after its last write.
                images.push(crash.crash(0));
                let root = made.db.tree.root();
                drop(made.db);
                done?;
                ensure!(
                    images.len() > 2,
                    "{case:?}, seed {seed}: {} images",
                    images.len()
                );
                for (i, image) in images.into_iter().enumerate() {
                    let db = go("open", || Db::recover(Arc::new(image), root, 1));
                    let db = db.map_err(|e| format!("{case:?}, seed {seed}, image {i}: {e}"))?;
                    let seen = opened_as(&db, &made.writes, &made.pages);
                    drop(db);
                    let (owed, marked) =
                        seen.map_err(|e| format!("{case:?}, seed {seed}, image {i}: {e}"))?;
                    if owed == made.first && marked.iter().all(|&m| m) {
                        old += 1;
                    } else {
                        ensure!(
                            owed == made.second && marked == made.marked,
                            "{case:?}, seed {seed}, image {i}: the open owes {owed:?} and marks \
                             {marked:?}, where the record before's {:?}, or the new one's {:?} \
                             marking {:?}",
                            made.first,
                            made.second,
                            made.marked
                        );
                        new += 1;
                    }
                }
            }
            ensure!(
                old > 0 && new > 0,
                "{case:?}: {old} images opened with the record before, {new} with the new one"
            );
        }
        Ok(())
    })
}

/// #6336's three cases.
#[derive(Debug, Clone, Copy, PartialEq)]
enum LeftOut {
    /// The head past a left-out item inside its block.
    HeadPast,
    /// The same with the head's own item part-freed: a chunked value, its first chunk freed.
    PartFreed,
    /// A block of positions 0, 5 and 9, the head at 7.
    Span,
    /// The head on its block's last item, a chunked value, its first chunk freed.
    LastPartFreed,
}

/// One of the cases made up to the checkpoint that writes the head: the database, the writes, the
/// items the record before holds and those the new record must leave owed, and the pages of
/// position 3's value and 4's (PartFreed's), with the marks the new record's open must leave.
struct LeftOutMade {
    db: Db,
    writes: Vec<(usize, u8)>,
    first: Vec<ValueRef>,
    second: Vec<ValueRef>,
    pages: Vec<PageId>,
    marked: Vec<bool>,
}

/// The case up to the checkpoint that writes the head, every count on the way checked.
fn left_out(crash: &Arc<CrashIO>, case: LeftOut) -> Words<LeftOutMade> {
    let db = Db::create(crash.clone());
    let keys = if case == LeftOut::Span { 3 } else { 6 };
    for k in 0..keys {
        let chunked_key = match case {
            LeftOut::PartFreed => Some(2),
            LeftOut::LastPartFreed => Some(3),
            _ => None,
        };
        let len = if chunked_key == Some(k) {
            chunked()
        } else {
            VALUE
        };
        db.put(&key(k), &vec![k as u8 + 1; len]);
    }
    checkpoint(&db)?;
    let writes: Vec<(usize, u8)> = match case {
        LeftOut::Span => (0..5)
            .map(|i| (0, 101 + i))
            .chain((0..4).map(|i| (1, 111 + i)))
            .chain([(2, 121)])
            .collect(),
        _ => vec![(0, 101), (0, 151), (1, 102), (2, 103), (3, 104)],
    };
    let (slot, refs) = retire_each(&db, &writes)?;
    let kept: Vec<usize> = match case {
        LeftOut::Span => vec![0, 5, 9],
        _ => vec![0, 2, 3, 4],
    };
    let pages = match case {
        LeftOut::PartFreed | LeftOut::LastPartFreed => {
            let mut pages = value_pages(&db, refs[3])?;
            pages.extend(value_pages(&db, refs[4])?);
            pages
        }
        _ => Vec::new(),
    };
    checkpoint(&db)?;
    let written = owed_items(&db)?;
    let (budget, head) = match case {
        LeftOut::HeadPast => (3, (3, 0)),
        LeftOut::PartFreed => (4, (3, 1)),
        LeftOut::Span => (7, (7, 0)),
        LeftOut::LastPartFreed => (5, (4, 1)),
    };
    let freed = free(&db, slot, budget)?;
    let at = db.db.checkpointer().retired().collector().head(slot);
    let checked = (|| {
        ensure!(
            written == kept.len(),
            "{case:?}: the first record holds {written} items, where {}",
            kept.len()
        );
        ensure!(
            freed == budget && at == head,
            "{case:?}: the pass freed {freed} pages and its head stands at {at:?}, where {head:?}"
        );
        ensure!(
            pages.is_empty() || pages.len() == 4,
            "{case:?}: {} pages of positions 3 and 4",
            pages.len()
        );
        Ok(())
    })();
    if let Err(words) = checked {
        drop(db);
        return Err(words);
    }
    let not_freed = match case {
        LeftOut::Span => 9..10,
        LeftOut::LastPartFreed => 4..5,
        _ => 3..5,
    };
    // Position 3's value and 4's, each directory first: the new record's open marks every page of
    // an item it owes but a chunk the pass freed of its head item.
    let marked = match case {
        LeftOut::PartFreed => vec![true, false, true, true],
        LeftOut::LastPartFreed => vec![false, true, false, true],
        _ => Vec::new(),
    };
    Ok(LeftOutMade {
        db,
        writes,
        first: kept.iter().map(|&i| refs[i]).collect(),
        second: refs[not_freed].to_vec(),
        pages,
        marked,
    })
}

/// The case's checkpoint that writes the head, then a kill and the open: it owes the items not
/// freed, marks as the case wants, and every row reads back.
fn left_out_killed_after(seed: u64, case: LeftOut) -> Words<()> {
    let crash = Arc::new(CrashIO::new(seed));
    let made = left_out(&crash, case)?;
    let done = checkpoint(&made.db);
    let root = made.db.tree.root();
    drop(made.db);
    done?;
    let db = reopen(&crash, root)?;
    let seen = opened_as(&db, &made.writes, &made.pages);
    drop(db);
    let (owed, marked) = seen?;
    ensure!(
        owed == made.second,
        "{case:?}: the open owes {owed:?}, where the values not freed: {:?}",
        made.second
    );
    ensure!(
        marked == made.marked,
        "{case:?}: pages marked retired by the open, position 3's (directory first) then 4's: \
         {marked:?}, where {:?}",
        made.marked
    );
    Ok(())
}

/// What an open of a case's image owes, and which of `pages` it marked retired; every key reads
/// its last write's value.
fn opened_as(
    db: &Db,
    writes: &[(usize, u8)],
    pages: &[PageId],
) -> Words<(Vec<ValueRef>, Vec<bool>)> {
    let owed = owed(db)?;
    let marked = pages
        .iter()
        .map(|&pid| db.db.store().retired(pid))
        .collect();
    let keys = writes.iter().map(|&(k, _)| k + 1).max().unwrap_or(0);
    for k in 0..keys {
        let want = writes
            .iter()
            .rev()
            .find(|&&(w, _)| w == k)
            .map_or(k as u8 + 1, |&(_, fill)| fill);
        let read = db.read(&key(k))?;
        ensure!(
            read.as_ref().and_then(|v| v.first()) == Some(&want),
            "key {k} reads {:?}, where its last write's {want}",
            read.map(|v| (v.len(), v.first().copied()))
        );
    }
    Ok((owed, marked))
}

/// The free blocks across checkpoints: a durable record holds every block its checkpoint took, so
/// a give-back after its superblock is durable (a later phase failing) gives back none; the next
/// durable record frees the blocks the one before held alone, its own record's; and once a pass
/// frees every item of the slot's list, the durable record after it frees the list's blocks. At
/// each step the held and free blocks are apart and together the whole file. Driven at the
/// Retired file's level, as the failed write's test is (the auditor's #6308 finding 3, what the
/// runs of free blocks keep across checkpoints; mutants: the taken blocks kept at the durable, the
/// freed blocks not freed, a list's passed blocks kept).
#[test]
fn a_durable_record_holds_what_it_took_and_frees_what_the_record_before_held_alone() {
    words(|| {
        let crash = Arc::new(CrashIO::new(99));
        let db = Db::create(crash);
        for k in 0..6 {
            db.put(&key(k), &vec![k as u8 + 1; VALUE]);
        }
        checkpoint(&db)?;
        let (slot, _) = replace(&db, 0..3)?;
        step_while(&db, |phase| phase.is_some())?;
        let retired = db.db.checkpointer().retired();
        let durable_run = || -> Words<crate::storage::atoll::gc::RetiredRun> {
            ensure!(
                db.db.checkpointer().phase_for_test().is_none(),
                "a checkpoint began beside the hand-driven runs"
            );
            let c = db.db.transactions().clock_now();
            let mut run = crate::storage::atoll::gc::RetiredRun::default();
            retired.cut(&mut run);
            go("a run", || file_steps(&db, &mut run, c))?;
            ensure!(retired.written(&run).is_some(), "a run wrote nothing");
            retired.durable(&mut run);
            Ok(run)
        };
        let mut a = durable_run()?;
        let after_a = retired.blocks_for_test();
        let gave_none = retired.give_back_taken(&mut a);
        let after_give = retired.blocks_for_test();
        replace(&db, 3..6)?;
        durable_run()?;
        let after_b = retired.blocks_for_test();
        let freed = free(&db, slot, 1 << 20)?;
        durable_run()?;
        let after_c = retired.blocks_for_test();
        drop(db);
        for (name, (held, free, len)) in [("A", &after_a), ("B", &after_b), ("C", &after_c)] {
            let all: std::collections::BTreeSet<u64> = held.union(free).copied().collect();
            ensure!(
                held.is_disjoint(free) && all == (0..*len).collect(),
                "after {name}: held {held:?} and free {free:?} are not the file's {len} blocks, \
                 apart"
            );
        }
        ensure!(
            gave_none && after_give == after_a,
            "a give-back after A was durable gave back {:?}",
            after_give.1.difference(&after_a.1).collect::<Vec<_>>()
        );
        let record_a: std::collections::BTreeSet<u64> =
            after_a.0.difference(&after_b.0).copied().collect();
        ensure!(
            !record_a.is_empty() && record_a.is_subset(&after_b.1),
            "the blocks A's record held alone {record_a:?}, where after B free {:?}",
            after_b.1
        );
        ensure!(freed == 6, "the pass freed {freed} pages of the 6 values");
        ensure!(
            after_c.0.is_disjoint(&after_b.0),
            "after the pass and C, held {:?}, where B held {:?}",
            after_c.0,
            after_b.0
        );
        Ok(())
    })
}

/// The owed list's blocks across checkpoints: an open holds the blocks of every owed list part; a
/// durable record after the owed head passes a block frees it, and once the head passes the whole
/// part, the rest. 150 items written by a checkpoint, two blocks of 101 and 49, are owed after a
/// kill; a pass frees 101 and a checkpoint follows, then a pass frees the 49 and another. At each
/// step the held and free blocks are apart and together the whole file, and every row reads back
/// (the auditor's #6308 finding 3, what the runs of free blocks keep across checkpoints; mutants:
/// a passed block of a part kept, a whole part's blocks kept).
#[test]
fn an_owed_lists_blocks_are_freed_by_the_durable_record_after_its_head_passes_them() {
    words(|| {
        let crash = Arc::new(CrashIO::new(101));
        let db = Db::create(crash.clone());
        retire(&db, 0..150)?;
        checkpoint(&db)?;
        let root = db.tree.root();
        drop(db);
        let db = reopen(&crash, root)?;
        let retired = db.db.checkpointer().retired();
        let owed = retired.lists().owed();
        let opened = retired.blocks_for_test();
        let first = free(&db, owed, 101)?;
        checkpoint(&db)?;
        let after_part = retired.blocks_for_test();
        let rest = free(&db, owed, 1 << 20)?;
        checkpoint(&db)?;
        let after_all = retired.blocks_for_test();
        let rows = rows_read_back(&db, 150);
        drop(db);
        for (name, (held, free, len)) in [
            ("the open", &opened),
            ("a block passed", &after_part),
            ("the part passed", &after_all),
        ] {
            let all: std::collections::BTreeSet<u64> = held.union(free).copied().collect();
            ensure!(
                held.is_disjoint(free) && all == (0..*len).collect(),
                "after {name}: held {held:?} and free {free:?} are not the file's {len} blocks, \
                 apart"
            );
        }
        ensure!(
            first == 101 && rest == 49,
            "the passes freed {first} and {rest} of the 150 owed values"
        );
        let kept: Vec<u64> = opened.0.intersection(&after_part.0).copied().collect();
        ensure!(
            kept.len() == 1
                && opened
                    .0
                    .difference(&after_part.0)
                    .all(|b| after_part.1.contains(b)),
            "after a block passed, of the open's held {:?} still held {kept:?}, free {:?}",
            opened.0,
            after_part.1
        );
        ensure!(
            after_all.0.is_disjoint(&opened.0) && opened.0.is_subset(&after_all.1),
            "after the part passed, held {:?}, where the open held {:?}",
            after_all.0,
            opened.0
        );
        rows
    })
}

/// A checkpoint's bookkeeping of the retired file is in proportion to what it writes and what the
/// heads passed, not to what an old reader holds back: the third of three checkpoints goes through
/// as many elements of it for 101 items held back as for 1,010 (the auditor's #6308, finding 3;
/// mutant: the free blocks built again from every block held, each checkpoint).
#[test]
fn a_checkpoints_bookkeeping_does_not_grow_with_what_an_old_reader_holds_back() {
    words(|| {
        let (_, few) = held_back(93, 101)?;
        let (_, many) = held_back(94, 1_010)?;
        ensure!(
            few == many,
            "the third checkpoint's bookkeeping: {few} elements for 101 items held back, {many} \
             for 1,010"
        );
        Ok(())
    })
}

/// Three checkpoints of one slot's list while an old reader holds every item back: the first
/// takes `held` items, the next two [`NEW`] each, every value imaged by a checkpoint before it is
/// replaced. The list pages the Retired phase read in each, and the elements of bookkeeping the
/// third went through.
fn held_back(seed: u64, held: usize) -> Words<([u64; 3], u64)> {
    let db = Db::create(Arc::new(CrashIO::new(seed)));
    let mut reader = db.begin();
    let none = read_as(&db, &mut reader, "none")?;
    let mut reads = [0; 3];
    let mut bookkeeping = 0;
    let mut next = 0;
    let mut slot = None;
    for (i, count) in [held, NEW, NEW].into_iter().enumerate() {
        for k in next..next + count {
            db.put(&key(k), &vec![1; VALUE]);
        }
        checkpoint(&db)?;
        for k in next..next + count {
            let (txn, old) = retiring(&db, &key(k), vec![2; VALUE])?;
            slot.get_or_insert(txn.slot());
            commit(&db, txn)?;
            old.ok_or_else(|| format!("key {k} not out of line"))?;
        }
        next += count;
        let freed = free(&db, slot.ok_or("no slot")?, 1 << 20)?;
        ensure!(
            freed == 0,
            "the pass freed {freed} pages under the old reader"
        );
        step_while(&db, |phase| phase.is_some())?;
        crate::storage::atoll::gc::take_bookkeeping();
        crate::storage::atoll::gc::take_list_reads();
        checkpoint(&db)?;
        reads[i] = crate::storage::atoll::gc::take_list_reads();
        bookkeeping = crate::storage::atoll::gc::take_bookkeeping();
    }
    let mut ended = Commit::new(reader);
    go("the old reader's end", || {
        drive(&db.io, || db.db.transactions().commit(&mut ended))
    })?;
    drop(db);
    ensure!(none.is_none(), "the old reader read a row");
    Ok((reads, bookkeeping))
}

/// A value's page whose number is taken, and whose frame is not yet in the pool, when a
/// checkpoint's cut walks the pool (VALUE-LEAK1's window) is left out of that image by the
/// Untaken phase. Its writer is open at the cut, and the Retired phase judges no item for it; the
/// writer commits after the cut, and a kill and the open bring the value back by replay (the
/// auditor's #6308, 5).
#[test]
fn a_value_page_taken_with_no_frame_at_a_cut_is_left_out_by_untaken_and_judged_by_no_item() {
    use std::cell::RefCell;
    use std::rc::Rc;
    words(|| {
        let crash = Arc::new(CrashIO::new(85));
        let db = Rc::new(Db::create(crash.clone()));
        db.put("u", &vec![1; VALUE]);
        checkpoint(&db)?;
        let txns = db.db.transactions();
        let mut txn = db.begin();
        let ticket = Rc::new(RefCell::new(None));
        let cut: Rc<RefCell<Words<()>>> = Rc::new(RefCell::new(Err("the cut not taken".into())));
        {
            let (db, ticket, cut) = (db.clone(), ticket.clone(), cut.clone());
            crate::race::at("buffer.install_publish", move || {
                *ticket.borrow_mut() = Some(db.db.checkpointer().request_full());
                *cut.borrow_mut() = until_cut(&db);
            });
        }
        let mut op = WriteOp::put(b"v", vec![3; VALUE], Mode::Upsert);
        let wrote = go("write", || {
            drive(&db.io, || {
                db.tree.write(txns, &db.versions, &mut txn, &mut op)
            })
        })?;
        let hooked = !crate::race::pending("buffer.install_publish");
        let cut = cut.replace(Ok(()));
        let v = stored(&db, "v")?;
        let mut ticket = ticket
            .borrow_mut()
            .take()
            .ok_or("no checkpoint asked for")?;
        let checkpointer = db.db.checkpointer();
        go("checkpoint", || {
            drive(&db.io, || checkpointer.step_full(&mut ticket, txns))
        })?;
        let snapshot = checkpointer.superblock().snapshot;
        let mut commit = Commit::new(txn);
        let ts = go("commit", || drive(&db.io, || txns.commit(&mut commit)))?;
        let v = v.ok_or("v not out of line")?;
        let roads = db.db.store().left_out_by(v.pid);
        let judgings = checkpointer.retired().judgings(v);
        let image = crash.crash(0);
        let root = db.tree.root();
        drop(db);
        ensure!(
            hooked && matches!(wrote, Write::Done(_)),
            "the write {wrote:?}, hooked {hooked}"
        );
        cut?;
        ensure!(
            roads == ["untaken"],
            "v's page left out by {roads:?}, where the Untaken phase leaves it out"
        );
        ensure!(
            ts.is_some_and(|ts| ts > snapshot),
            "v's writer committed at {ts:?}, the cut at {snapshot}"
        );
        ensure!(judgings == 0, "an item judged for v {judgings} times");
        let db = go("open", || Db::recover(Arc::new(image), root, 1))?;
        let read = db.read("v")?;
        drop(db);
        ensure!(read == Some(vec![3; VALUE]), "v reads back after replay");
        Ok(())
    })
}

/// Where a value's pages stand before its checkpoint, for the auditor's #6282 condition 2.
#[derive(Debug, Clone, Copy)]
enum Swap {
    /// Its one page written to swap and evicted before the cut.
    Before,
    /// A value of two chunks and a directory, all written back to swap, then its first chunk alone
    /// evicted: that chunk in swap alone, the others resident with their swap copies.
    Chunked,
    /// Its one page written to swap and evicted between the cut and the Retired phase.
    AfterTheCut,
}

/// (s1) a value made and replaced at or below C, its one page in swap before the cut (case a).
#[test]
fn a_value_made_and_retired_below_c_in_swap_before_the_cut_is_freed_once() {
    words(|| swap_case(31, Swap::Before, false))
}

/// (s2) a chunked value of case (a), some chunks in swap and some never evicted.
#[test]
fn a_chunked_value_made_and_retired_below_c_partly_in_swap_is_freed_once() {
    words(|| swap_case(32, Swap::Chunked, false))
}

/// (s3) a value of case (a) whose page is evicted between the cut and the Retired phase.
#[test]
fn a_value_made_and_retired_below_c_evicted_after_the_cut_is_freed_once() {
    words(|| swap_case(33, Swap::AfterTheCut, false))
}

/// (s4) the three for a value an earlier checkpoint imaged (case d).
#[test]
fn an_imaged_value_retired_below_c_in_swap_before_the_cut_is_freed_once() {
    words(|| swap_case(34, Swap::Before, true))
}

#[test]
fn an_imaged_chunked_value_retired_below_c_partly_in_swap_is_freed_once() {
    words(|| swap_case(35, Swap::Chunked, true))
}

#[test]
fn an_imaged_value_retired_below_c_evicted_after_the_cut_is_freed_once() {
    words(|| swap_case(36, Swap::AfterTheCut, true))
}

/// A value written, imaged by a checkpoint first when `imaged`, replaced by a retiring write, its
/// pages placed as `swap` says, then a checkpoint, a kill and the open, and the owed list's pass:
/// no page leaked (the pages in use of each of its classes back to what they were, less its own),
/// none freed twice, its row reading back. The words say where each page stood and what the image
/// kept, which the auditor asked for before (ii) is built.
fn swap_case(seed: u64, swap: Swap, imaged: bool) -> Words<()> {
    let crash = Arc::new(CrashIO::new(seed));
    let db = Db::create(crash.clone());
    let len = match swap {
        Swap::Chunked => crate::storage::atoll::btree::value::chunk() + 12_345,
        _ => VALUE,
    };
    db.put("v", &vec![1; len]);
    let old = stored(&db, "v")?.ok_or("v not out of line")?;
    let pages = value_pages(&db, old)?;
    if imaged {
        checkpoint(&db)?;
    }
    match swap {
        Swap::Before => to_swap(&db)?,
        Swap::Chunked => {
            let pool = db.db.pool();
            let mut sweep = pool.sweep();
            go("sweep", || drive(&db.io, || sweep.step(pool)))?;
            let evicted = pool.evict_clean(pages[1]);
            let swapped: Vec<bool> = pages
                .iter()
                .map(|&p| db.db.store().swap_slot(p).is_some())
                .collect();
            eprintln!(
                "CONDITION-2 chunked, after the write-back: in swap {swapped:?}; its first chunk \
                 evicted {evicted}"
            );
        }
        Swap::AfterTheCut => {}
    }
    let (txn, _) = retiring(&db, "v", vec![2; len])?;
    commit(&db, txn)?;
    let classes: BTreeMap<u64, u64> = pages
        .iter()
        .map(|p| (p.class().index() as u64, 0))
        .collect();
    let mut expected = BTreeMap::new();
    for &class in classes.keys() {
        let n = in_use(&db, class)?;
        let own = pages
            .iter()
            .filter(|p| p.class().index() as u64 == class)
            .count() as u64;
        expected.insert(class, n - own);
    }
    let in_swap = |db: &Db| -> Vec<bool> {
        pages
            .iter()
            .map(|&p| db.db.store().swap_slot(p).is_some())
            .collect()
    };
    let before_the_cut = in_swap(&db);
    let mut live = Vec::new();
    for &p in &pages {
        live.push(go("entry", || drive(&db.io, || db.db.store().entry(p)))?);
    }
    eprintln!("CONDITION-2 live map entries before the cut {live:?}");
    match swap {
        Swap::AfterTheCut => {
            let checkpointer = db.db.checkpointer();
            let mut ticket = checkpointer.request_full();
            until_cut(&db)?;
            to_swap(&db)?;
            go("checkpoint", || {
                drive(&db.io, || {
                    checkpointer.step_full(&mut ticket, db.db.transactions())
                })
            })?;
        }
        _ => checkpoint(&db)?,
    }
    let at_the_end = in_swap(&db);
    let roads: Vec<Vec<&str>> = pages
        .iter()
        .map(|&p| db.db.store().left_out_by(p))
        .collect();
    let root = db.tree.root();
    drop(db);
    let left_out_by_values = roads
        .iter()
        .all(|r| r.as_slice() == if imaged { &[][..] } else { &["values"][..] });
    ensure!(
        left_out_by_values,
        "{swap:?}, imaged first {imaged}: its pages left out by {roads:?}"
    );
    let placed = format!(
        "{swap:?}, imaged first {imaged}: {} pages, in swap before the cut {before_the_cut:?}, \
         at the checkpoint's end {at_the_end:?}",
        pages.len()
    );
    eprintln!("CONDITION-2 {placed}");
    let db = reopen(&crash, root).map_err(|e| format!("{placed}; {e}"))?;
    let mut kept = Vec::new();
    for &p in &pages {
        let entry = go("entry", || drive(&db.io, || db.db.store().entry(p)))?;
        kept.push(entry != crate::storage::atoll::radix::FREE);
    }
    let owed = owed(&db)?.contains(&old);
    let place = format!("{placed}; kept by the image {kept:?}; its item in the file {owed}");
    eprintln!("CONDITION-2 {place}");
    let lists = db.db.checkpointer().retired().lists();
    let freed =
        free(&db, lists.owed(), 1 << 20).map_err(|e| format!("{place}; the owed pass: {e}"))?;
    let mut after = BTreeMap::new();
    for &class in classes.keys() {
        after.insert(class, in_use(&db, class)?);
    }
    ensure!(
        after == expected,
        "{place}; the owed pass freed {freed}; pages in use {after:?}, where {expected:?}"
    );
    let read = db.read("v")?;
    ensure!(read == Some(vec![2; len]), "{place}; v reads back wrong");
    Ok(())
}

/// A chunked value live at the cut, its checkpoint killed just before each write of a data file
/// lands while the Images and Values phases run, each image keeping or losing what was not synced:
/// each opens with the checkpoint before in charge, and the value reads back whole or not at all
/// (the auditor's #6285 condition 2: the step of "a value's pages agree" that was read, not shown).
#[test]
fn a_kill_while_a_chunked_value_is_imaged_opens_with_the_image_before_and_the_value_whole() {
    words(|| {
        for seed in 0..2 {
            let crash = Arc::new(CrashIO::new(51 + seed));
            let db = Db::create(crash.clone());
            db.put("x", &[5; 100]);
            checkpoint(&db)?;
            let len = chunked();
            db.put("c", &vec![6; len]);
            let pages = value_pages(&db, stored(&db, "c")?.ok_or("c not out of line")?)?;
            let checkpointer = db.db.checkpointer();
            // A checkpoint the put began, as a writer steps them, ends first.
            step_while(&db, |phase| phase.is_some())?;
            let before = checkpointer.superblock().checkpoint;
            let mut ticket = checkpointer.request_full();
            step_while(&db, |phase| phase != Some("Images"))?;
            let data = format!("{BASE}.c");
            let chunks = format!("{BASE}.c{}.", pages[1].class().index());
            let chunk_writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counted = chunk_writes.clone();
            crash.capture(
                move |p| {
                    if p.starts_with(&chunks) {
                        counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    p.strip_prefix(&data)
                        .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
                },
                u64::MAX,
                seed,
            );
            step_while(&db, |phase| matches!(phase, Some("Images" | "Values")))?;
            let images = crash.captured();
            let chunk_writes = chunk_writes.load(std::sync::atomic::Ordering::Relaxed);
            go("checkpoint", || {
                drive(&db.io, || {
                    checkpointer.step_full(&mut ticket, db.db.transactions())
                })
            })?;
            let root = db.tree.root();
            drop(db);
            ensure!(
                images.len() >= pages.len() && chunk_writes >= pages.len() - 1,
                "seed {seed}: {} images and {chunk_writes} writes of chunks, for a value of {} \
                 pages",
                images.len(),
                pages.len()
            );
            for (i, image) in images.into_iter().enumerate() {
                let image: Arc<dyn IO> = Arc::new(image);
                // Read before the open, which makes a checkpoint of its own.
                let in_charge = in_charge(&image)?;
                let db = go("open", || Db::recover(image, root, 1))?;
                let c = db.read("c")?;
                let x = db.read("x")?;
                drop(db);
                ensure!(
                    in_charge == before,
                    "seed {seed}, image {i}: checkpoint {in_charge} in charge, where {before} was \
                     the one before"
                );
                ensure!(
                    c.as_ref().is_none_or(|c| *c == vec![6; len]),
                    "seed {seed}, image {i}: c reads {:?}",
                    c.map(|c| c.len())
                );
                ensure!(
                    x == Some(vec![5; 100]),
                    "seed {seed}, image {i}: x reads back"
                );
            }
        }
        Ok(())
    })
}

/// A value made and replaced since the checkpoint before (case a), held back by an old snapshot:
/// the first checkpoint leaves its item out; its page is written to swap, read in by the old
/// snapshot and evicted again; then a second checkpoint and a kill. Each checkpoint's Values phase
/// leaves the page out, owed to each until it is freed; its item is judged once, the first
/// checkpoint's written mark passing it though it wrote nothing. The open finds no item and the
/// page free, frees nothing twice, leaks nothing, and the row reads back. Once the old snapshot
/// ends, the pass frees the value, and the test build's check at that free finds its page with no
/// slot (the auditor's #6282 condition 3, as #6285 changed it).
#[test]
fn a_value_left_out_and_held_back_by_an_old_snapshot_is_freed_once_after_a_kill() {
    words(|| {
        let crash = Arc::new(CrashIO::new(55));
        let db = Db::create(crash.clone());
        db.put("v", &vec![1; VALUE]);
        let old = stored(&db, "v")?.ok_or("v not out of line")?;
        let pages = value_pages(&db, old)?;
        let mut reader = db.begin();
        let first = read_as(&db, &mut reader, "v")?;
        let (txn, _) = retiring(&db, "v", vec![2; VALUE])?;
        let slot = txn.slot();
        commit(&db, txn)?;
        let classes = classes_of(&pages);
        let expected = less(in_use_of(&db, &classes)?, &pages);
        checkpoint(&db)?;
        to_swap(&db)?;
        let in_swap = db.db.store().swap_slot(old.pid).is_some();
        let held = read_as(&db, &mut reader, "v")?;
        to_swap(&db)?;
        checkpoint(&db)?;
        let roads = db.db.store().left_out_by(old.pid);
        let judgings = db.db.checkpointer().retired().judgings(old);
        let image = crash.crash(0);
        let mut ended = Commit::new(reader);
        go("the old snapshot's end", || {
            drive(&db.io, || db.db.transactions().commit(&mut ended))
        })?;
        let freed = free(&db, slot, 10)?;
        let root = db.tree.root();
        drop(db);
        ensure!(
            first == Some(vec![1; VALUE]) && held == first,
            "the old snapshot read {:?}, then {:?}",
            first.map(|v| v[0]),
            held.map(|v| v[0])
        );
        ensure!(in_swap, "v's page not written to swap");
        ensure!(
            roads == ["values", "values"],
            "v's page left out by {roads:?}, where each checkpoint's Values phase leaves it out"
        );
        ensure!(
            judgings == 1,
            "v's item judged {judgings} times: its written mark did not pass it"
        );
        ensure!(freed == 1, "the pass freed {freed} pages of v's one");
        let db = go("open", || Db::recover(Arc::new(image), root, 1))?;
        let owed = owed(&db)?;
        let entry = go("entry", || drive(&db.io, || db.db.store().entry(old.pid)))?;
        let lists = db.db.checkpointer().retired().lists();
        let passed = free(&db, lists.owed(), 1 << 20)?;
        let after = in_use_of(&db, &classes)?;
        let row = db.read("v")?;
        drop(db);
        ensure!(!owed.contains(&old), "the owed list holds v's item");
        ensure!(
            entry == crate::storage::atoll::radix::FREE,
            "v's page reads {entry} in the image"
        );
        ensure!(passed == 0, "the owed pass freed {passed} pages");
        ensure!(
            after == expected,
            "pages in use {after:?}, where {expected:?}"
        );
        ensure!(row == Some(vec![2; VALUE]), "v reads back");
        Ok(())
    })
}

/// A pass that frees a retired value's chunk while a checkpoint's cut is in effect hands the page
/// to the checkpointer: the next checkpoint's Orphans phase leaves it out of its image and frees
/// it, and a kill after that checkpoint opens with the item's progress past that chunk, so the
/// owed list frees the rest and the chunk is freed once (the Orphans phase's `left_out`, which a
/// value's page reaches this way).
#[test]
fn a_chunk_a_pass_frees_during_a_cut_is_left_out_by_the_next_checkpoint_and_freed_once() {
    words(|| {
        let crash = Arc::new(CrashIO::new(81));
        let db = Db::create(crash.clone());
        let len = chunked();
        db.put("big", &vec![7; len]);
        checkpoint(&db)?;
        let (txn, old) = retiring(&db, "big", vec![8; len])?;
        let slot = txn.slot();
        commit(&db, txn)?;
        let pages = value_pages(&db, old.ok_or("big not out of line")?)?;
        let classes = classes_of(&pages);
        let expected = less(in_use_of(&db, &classes)?, &pages);
        let checkpointer = db.db.checkpointer();
        let mut ticket = checkpointer.request_full();
        until_cut(&db)?;
        let during = free(&db, slot, 1)?;
        go("checkpoint", || {
            drive(&db.io, || {
                checkpointer.step_full(&mut ticket, db.db.transactions())
            })
        })?;
        let after_the_cut = db.db.store().left_out_by(pages[1]);
        checkpoint(&db)?;
        let roads = db.db.store().left_out_by(pages[1]);
        let root = db.tree.root();
        drop(db);
        ensure!(during == 1, "the pass during the cut freed {during} pages");
        ensure!(
            after_the_cut.is_empty() && roads == ["orphans"],
            "the first chunk left out by {after_the_cut:?} at the cut's checkpoint, then {roads:?}"
        );
        let db = reopen(&crash, root)?;
        let marked: Vec<bool> = pages.iter().map(|&p| db.db.store().retired(p)).collect();
        let owed = db.db.checkpointer().retired().lists().owed();
        let freed = free(&db, owed, 1 << 20)?;
        let after = in_use_of(&db, &classes)?;
        let read = db.read("big")?;
        drop(db);
        ensure!(
            marked == [true, false, true],
            "the directory, first and second chunk marked by the open: {marked:?}"
        );
        ensure!(freed == 2, "the owed pass freed {freed} pages");
        ensure!(
            after == expected,
            "pages in use {after:?}, where {expected:?}"
        );
        ensure!(read == Some(vec![8; len]), "big reads back");
        Ok(())
    })
}

/// When a value's writer (T1) and its replacer (T2) commit against the snapshot `C` of the
/// checkpoint a kill follows (the designer's cases in message 7, the auditor's #6282 condition 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Case {
    /// (a) T1 and T2 at or below `C`, both after the checkpoint before.
    BothBelow,
    /// (b) T1 at or below `C`, T2 open at the cut and committed above it.
    ReplacedAbove,
    /// (b) T1 at or below `C`, T2 open at the cut and still open at the kill.
    ReplacedOpen,
    /// (c) T1 and T2 above `C`.
    BothAbove,
    /// (d) T1 imaged by an earlier checkpoint, T2 at or below `C`.
    Imaged,
}

/// (a): the item is not written, the image names the value's pages free, the open frees nothing
/// twice and leaks nothing.
#[test]
fn a_value_made_and_replaced_below_c_is_left_out_of_the_image_and_the_file() {
    words(|| case_run(61, Case::BothBelow, false))
}

#[test]
fn a_chunked_value_made_and_replaced_below_c_is_left_out_of_the_image_and_the_file() {
    words(|| case_run(62, Case::BothBelow, true))
}

/// (b): the value is imaged, its replacer's item is not written, and replay frees it once.
#[test]
fn a_value_replaced_by_a_writer_committed_above_c_is_freed_once_by_replay() {
    words(|| case_run(63, Case::ReplacedAbove, false))
}

/// (b), the replacer never committed: the value stays, and the replacer's pages leak nowhere.
#[test]
fn a_value_replaced_by_a_writer_open_at_the_kill_stays_and_nothing_leaks() {
    words(|| case_run(64, Case::ReplacedOpen, false))
}

/// (c): neither is in the image or the file; replay makes the value and frees it.
#[test]
fn a_value_made_and_replaced_above_c_is_made_and_freed_by_replay() {
    words(|| case_run(65, Case::BothAbove, false))
}

/// (d): the item is written, the image keeps the value, and the owed list frees it once.
#[test]
fn an_imaged_value_replaced_below_c_is_owed_and_freed_once() {
    words(|| case_run(66, Case::Imaged, false))
}

#[test]
fn an_imaged_chunked_value_replaced_below_c_is_owed_and_freed_once() {
    words(|| case_run(67, Case::Imaged, true))
}

/// Writes a value and replaces it as `case` says around a full checkpoint, then a kill, the open
/// and the owed list's pass: its item is in the file only in case (d); no page of the value's
/// classes leaked (the pages in use back to what they were, less the value's own where it was
/// replaced); none freed twice (the open's assertion, the pass's frees); and every row reads back.
fn case_run(seed: u64, case: Case, chunked: bool) -> Words<()> {
    let crash = Arc::new(CrashIO::new(seed));
    let db = Db::create(crash.clone());
    let len = if chunked { self::chunked() } else { VALUE };
    let (v1, v2) = (vec![1; len], vec![2; len]);
    db.put("x", &[5; 100]);
    let checkpointer = db.db.checkpointer();
    let txns = db.db.transactions();
    let (old, expected, image) = match case {
        Case::BothBelow | Case::Imaged => {
            db.put("v", &v1);
            if case == Case::Imaged {
                checkpoint(&db)?;
            }
            let (txn, old) = retiring(&db, "v", v2.clone())?;
            commit(&db, txn)?;
            let old = old.ok_or("v not out of line")?;
            let pages = value_pages(&db, old)?;
            let expected = less(in_use_of(&db, &classes_of(&pages))?, &pages);
            checkpoint(&db)?;
            (old, expected, crash.crash(0))
        }
        Case::ReplacedAbove | Case::ReplacedOpen => {
            db.put("v", &v1);
            let old = stored(&db, "v")?.ok_or("v not out of line")?;
            let pages = value_pages(&db, old)?;
            let live = in_use_of(&db, &classes_of(&pages))?;
            let (txn, _) = retiring(&db, "v", v2.clone())?;
            let mut ticket = checkpointer.request_full();
            until_cut(&db)?;
            let open = match case {
                Case::ReplacedAbove => {
                    commit(&db, txn)?;
                    None
                }
                _ => Some(txn),
            };
            let expected = match open {
                None => less(in_use_of(&db, &classes_of(&pages))?, &pages),
                Some(_) => live,
            };
            go("checkpoint", || {
                drive(&db.io, || checkpointer.step_full(&mut ticket, txns))
            })?;
            let image = crash.crash(0);
            if let Some(txn) = open {
                roll_back(&db, txn, 1)?;
            }
            (old, expected, image)
        }
        Case::BothAbove => {
            let mut ticket = checkpointer.request_full();
            until_cut(&db)?;
            db.put("v", &v1);
            let (txn, old) = retiring(&db, "v", v2.clone())?;
            commit(&db, txn)?;
            let old = old.ok_or("v not out of line")?;
            let pages = value_pages(&db, old)?;
            let expected = less(in_use_of(&db, &classes_of(&pages))?, &pages);
            go("checkpoint", || {
                drive(&db.io, || checkpointer.step_full(&mut ticket, txns))
            })?;
            (old, expected, crash.crash(0))
        }
    };
    let pages = value_pages(&db, old)?;
    let classes = classes_of(&pages);
    let roads: Vec<Vec<&str>> = pages
        .iter()
        .map(|&p| db.db.store().left_out_by(p))
        .collect();
    let root = db.tree.root();
    drop(db);
    let db = go("open", || Db::recover(Arc::new(image), root, 1))?;
    let mut kept = Vec::new();
    for &p in &pages {
        let entry = go("entry", || drive(&db.io, || db.db.store().entry(p)))?;
        kept.push(entry != crate::storage::atoll::radix::FREE);
    }
    let owed = owed(&db)?.contains(&old);
    let lists = db.db.checkpointer().retired().lists();
    let freed = free(&db, lists.owed(), 1 << 20)?;
    let after = in_use_of(&db, &classes)?;
    let v = db.read("v")?;
    let x = db.read("x")?;
    let checked = tree_check(&db);
    drop(db);
    let what = format!(
        "{case:?}, chunked {chunked}: kept by the image {kept:?}, left out by {roads:?}, owed \
         {owed}"
    );
    ensure!(
        owed == (case == Case::Imaged),
        "{what}: its item in the file"
    );
    match case {
        Case::BothBelow => ensure!(
            kept.iter().all(|k| !k) && roads.iter().all(|r| r == &["values"]),
            "{what}: the Values phase leaves each page out of the image"
        ),
        Case::Imaged | Case::ReplacedOpen => {
            ensure!(
                kept.iter().all(|&k| k),
                "{what}: the image leaves a page out"
            )
        }
        Case::ReplacedAbove | Case::BothAbove => {}
    }
    ensure!(
        freed == if owed { pages.len() as u64 } else { 0 },
        "{what}: the owed pass freed {freed}"
    );
    ensure!(
        after == expected,
        "{what}: pages in use {after:?}, where {expected:?}"
    );
    let want = if case == Case::ReplacedOpen { v1 } else { v2 };
    ensure!(
        v == Some(want),
        "{what}: v reads {:?}",
        v.map(|v| (v.len(), v[0]))
    );
    ensure!(x == Some(vec![5; 100]), "{what}: x reads back");
    checked.map_err(|e| format!("{what}: {e}"))
}

/// A writer committed exactly at the snapshot `C`, its body in the log past the redo point `L`,
/// since another transaction's spilled part holds the cut's `L` back: the open frees the value it
/// replaced once, by the owed list, and replay skips its body (the auditor's #6269, 3).
#[test]
fn a_writer_committed_exactly_at_the_snapshot_with_its_body_past_the_redo_point_is_owed_once() {
    words(|| {
        let crash = Arc::new(CrashIO::new(71));
        let db = Db::create(crash.clone());
        db.put("w", &vec![1; VALUE]);
        checkpoint(&db)?;
        let txns = db.db.transactions();
        let mut holder = db.begin();
        for i in 0..2000 {
            let mut op = WriteOp::put(format!("h{i:05}").as_bytes(), vec![3; 8], Mode::Upsert);
            go("holder's write", || {
                drive(&db.io, || {
                    db.tree.write(txns, &db.versions, &mut holder, &mut op)
                })
            })?;
        }
        let end = txns.held_from();
        go("spill", || drive(&db.io, || txns.spill(&mut holder)))?;
        let held = txns.held_from();
        let (txn, old) = retiring(&db, "w", vec![2; VALUE])?;
        let old = old.ok_or("w not out of line")?;
        let mut commit = Commit::new(txn);
        let ts = go("commit", || drive(&db.io, || txns.commit(&mut commit)))?;
        let body_from = db.db.log().end();
        checkpoint(&db)?;
        let sb = db.db.checkpointer().superblock();
        let image = crash.crash(0);
        roll_back(&db, holder, 0)?;
        let root = db.tree.root();
        drop(db);
        ensure!(
            held >= end && held < body_from,
            "the holder's part at {held:?}, the log's end {end:?} before it and {body_from:?} after w"
        );
        ensure!(
            Some(sb.snapshot) == ts && sb.redo == held,
            "the checkpoint cut at {} with its redo at {:?}; w committed at {ts:?}, the part at \
             {held:?}",
            sb.snapshot,
            sb.redo
        );
        let db = go("open", || Db::recover(Arc::new(image), root, 1))?;
        let owed = owed(&db)?;
        let lists = db.db.checkpointer().retired().lists();
        let freed = free(&db, lists.owed(), 1 << 20)?;
        let w = db.read("w")?;
        let h = db.read("h00000")?;
        drop(db);
        ensure!(
            owed == vec![old],
            "the owed list {owed:?}, where w's item {old:?}"
        );
        ensure!(freed == 1, "the owed pass freed {freed} pages");
        ensure!(w == Some(vec![2; VALUE]), "w reads back");
        ensure!(h.is_none(), "the holder's write came back");
        Ok(())
    })
}

/// #6194 (a): three one-page values and a chunked one retired, a checkpoint and a kill; the open's
/// pass frees three pages (two values and the chunked one's first chunk), a checkpoint makes their
/// numbers reusable and new values take them, another checkpoint names the owed list's head inside
/// the chunked value, a kill, and the open frees the rest: every page retired is freed once (the
/// frees count it exactly), every row reads back, the tree checks, and no page of their classes
/// leaks.
#[test]
fn an_owed_list_freed_in_part_across_two_kills_frees_each_page_once_and_no_live_one() {
    words(|| {
        let crash = Arc::new(CrashIO::new(73));
        let db = Db::create(crash.clone());
        let lens = [VALUE, VALUE, chunked(), VALUE];
        for (k, &len) in lens.iter().enumerate() {
            db.put(&key(k), &vec![k as u8 + 1; len]);
        }
        checkpoint(&db)?;
        let mut values = Vec::new();
        for (k, &len) in lens.iter().enumerate() {
            let (txn, old) = retiring(&db, &key(k), vec![k as u8 + 101; len])?;
            commit(&db, txn)?;
            let old = old.ok_or_else(|| format!("key {k} not out of line"))?;
            values.push(value_pages(&db, old)?);
        }
        let pages: Vec<PageId> = values.iter().flatten().copied().collect();
        let classes = classes_of(&pages);
        checkpoint(&db)?;
        let root = db.tree.root();
        drop(db);
        let opened = Arc::new(crash.crash(0));
        let db = go("open", || Db::recover(opened.clone(), root, 1))?;
        let owed = db.db.checkpointer().retired().lists().owed();
        let first = free(&db, owed, 3)?;
        checkpoint(&db)?;
        let news = [VALUE, VALUE, VALUE, chunked()];
        for (k, &len) in news.iter().enumerate() {
            db.put(&key(10 + k), &vec![k as u8 + 51; len]);
        }
        checkpoint(&db)?;
        // Left to free: the chunked value's directory and second chunk, and the last value.
        let left: Vec<PageId> = std::iter::once(values[2][0])
            .chain(values[2][2..].iter().copied())
            .chain(values[3].iter().copied())
            .collect();
        let expected = less(in_use_of(&db, &classes)?, &left);
        drop(db);
        let db = reopen(&opened, root)?;
        let owed = db.db.checkpointer().retired().lists().owed();
        let rest = free(&db, owed, 1 << 20)?;
        let after = in_use_of(&db, &classes)?;
        let mut rows = Vec::new();
        for (k, &len) in lens.iter().enumerate() {
            rows.push(db.read(&key(k))? == Some(vec![k as u8 + 101; len]));
        }
        for (k, &len) in news.iter().enumerate() {
            rows.push(db.read(&key(10 + k))? == Some(vec![k as u8 + 51; len]));
        }
        let checked = tree_check(&db);
        drop(db);
        ensure!(first == 3, "the first pass freed {first} pages");
        ensure!(
            first + rest == pages.len() as u64,
            "the passes freed {first} and {rest} pages, where {} were retired",
            pages.len()
        );
        ensure!(rows.iter().all(|&r| r), "rows read back: {rows:?}");
        checked?;
        ensure!(
            after == expected,
            "pages in use {after:?}, where {expected:?}"
        );
        Ok(())
    })
}

/// #6194 (b): a transaction open across a checkpoint, with new values out of line and a write
/// that retires a value, its item in its slot's list; then its rollback, its item cut off the list
/// first, and a checkpoint, killed before every write and every third operation. Each image opens
/// with every row as it was before the transaction, no item of it in the owed list, no page of the
/// values' class leaked, and the tree checking.
#[test]
fn a_rollback_of_a_transaction_open_across_a_checkpoint_killed_anywhere_leaves_the_rows_before() {
    words(|| {
        for seed in 0..2 {
            let crash = Arc::new(CrashIO::new(75 + seed));
            let db = Db::create(crash.clone());
            for k in 0..3 {
                db.put(&key(k), &vec![k as u8 + 1; VALUE]);
            }
            checkpoint(&db)?;
            let old = stored(&db, &key(0))?.ok_or("key 0 not out of line")?;
            let classes = classes_of(&value_pages(&db, old)?);
            let expected = in_use_of(&db, &classes)?;
            let txns = db.db.transactions();
            let (mut txn, _) = retiring(&db, &key(0), vec![9; VALUE])?;
            for k in 20..22 {
                let mut op = WriteOp::put(key(k).as_bytes(), vec![k as u8; VALUE], Mode::Upsert);
                go("insert", || {
                    drive(&db.io, || {
                        db.tree.write(txns, &db.versions, &mut txn, &mut op)
                    })
                })?;
            }
            checkpoint(&db)?;
            crash.capture(|_| true, 3, seed);
            roll_back(&db, txn, 1)?;
            checkpoint(&db)?;
            let images = crash.captured();
            let root = db.tree.root();
            drop(db);
            ensure!(images.len() > 2, "seed {seed}: {} images", images.len());
            for (i, image) in images.into_iter().enumerate() {
                let db = go("open", || Db::recover(Arc::new(image), root, 1))?;
                let owed = owed(&db)?;
                let after = in_use_of(&db, &classes)?;
                let mut rows = Vec::new();
                for k in 0..3 {
                    rows.push(db.read(&key(k))? == Some(vec![k as u8 + 1; VALUE]));
                }
                for k in 20..22 {
                    rows.push(db.read(&key(k))?.is_none());
                }
                let checked = tree_check(&db);
                drop(db);
                ensure!(
                    owed.is_empty(),
                    "seed {seed}, image {i}: the owed list {owed:?}"
                );
                ensure!(
                    rows.iter().all(|&r| r),
                    "seed {seed}, image {i}: rows {rows:?}"
                );
                ensure!(
                    after == expected,
                    "seed {seed}, image {i}: pages in use {after:?}, where {expected:?}"
                );
                checked.map_err(|e| format!("seed {seed}, image {i}: {e}"))?;
            }
        }
        Ok(())
    })
}

/// Every dirty page written back, to swap, and every clean one evicted.
fn to_swap(db: &Db) -> Words<()> {
    let pool = db.db.pool();
    let mut sweep = pool.sweep();
    go("sweep", || drive(&db.io, || sweep.step(pool)))?;
    pool.evict_all_clean();
    Ok(())
}

/// The page ids of class `class` in use.
fn in_use(db: &Db, class: u64) -> Words<u64> {
    let class = SizeClass::new(class as usize).ok_or("a class")?;
    go("in use", || {
        drive(&db.io, || db.db.store().pages_in_use(class))
    })
}

/// Runs a test's body, its databases dropped before its words fail it.
fn words(body: impl FnOnce() -> Words<()>) {
    if let Err(words) = body() {
        panic!("{words}");
    }
}

/// Writes `keys` out of line, checkpoints, then writes each again in a transaction of its own, as
/// S5's write will: its item appended first through the lists' own append, with the writer's slot and start,
/// then published once its put is done. The references the second writes retired.
fn retire(db: &Db, keys: std::ops::Range<usize>) -> Words<Vec<ValueRef>> {
    for k in keys.clone() {
        db.put(&key(k), &vec![k as u8 + 1; VALUE]);
    }
    // Imaged before they are replaced (case d): the image keeps their pages, so their items are
    // written (DESIGN.md §6.5).
    checkpoint(db)?;
    let mut retired = Vec::new();
    for k in keys {
        let (txn, old) = retiring(db, &key(k), vec![k as u8 + 101; VALUE])?;
        commit(db, txn)?;
        let old = old.ok_or_else(|| format!("key {k} not out of line"))?;
        ensure!(
            db.db.store().retired(old.pid),
            "key {k}: its page not retired"
        );
        retired.push(old);
    }
    Ok(retired)
}

/// A transaction writing `value` at `key`, the item of the value it replaces appended and
/// published around its write as S5's will be, not yet committed: it and the reference it retired.
fn retiring(db: &Db, key: &str, value: Vec<u8>) -> Words<(Txn, Option<ValueRef>)> {
    let txns = db.db.transactions();
    let lists = db.db.checkpointer().retired().lists();
    let old = stored(db, key)?;
    let mut txn = db.begin();
    if let Some(reference) = old {
        go("append", || {
            drive(&db.io, || {
                lists.append(txn.slot(), txn.start(), reference, txns)
            })
        })?;
    }
    let mut op = WriteOp::put(key.as_bytes(), value, Mode::Upsert);
    let wrote = go("write", || {
        drive(&db.io, || {
            db.tree.write(txns, &db.versions, &mut txn, &mut op)
        })
    })?;
    ensure!(matches!(wrote, Write::Done(_)), "{key}: {wrote:?}");
    if old.is_some() {
        lists.publish(txn.slot(), txn.start());
    }
    Ok((txn, old))
}

fn commit(db: &Db, txn: Txn) -> Words<()> {
    let txns = db.db.transactions();
    let mut commit = Commit::new(txn);
    go("commit", || drive(&db.io, || txns.commit(&mut commit))).map(|_| ())
}

/// The reference of the out-of-line value `key` holds now, read as the tree stores it.
fn stored(db: &Db, key: &str) -> Words<Option<ValueRef>> {
    let tree = Tree::open(
        db.io.clone(),
        db.db.pool().clone(),
        db.db.store().clone(),
        db.tree.root(),
    )
    .with_head(HEADER)
    .with_reserves();
    let stored = go("stored", || drive(&db.io, || tree.stored(key.as_bytes())))?;
    Ok(match stored.map(|s| s.value) {
        Some(StoredValue::OutOfLine(reference)) => Some(reference),
        _ => None,
    })
}

/// Every item of the owed list, in order.
fn owed(db: &Db) -> Words<Vec<ValueRef>> {
    let lists = db.db.checkpointer().retired().lists();
    let mut reader = crate::storage::atoll::gc::Reader::from(0);
    let mut out = Vec::new();
    while let Some(item) = go("owed", || {
        drive(&db.io, || lists.read(lists.owed(), &mut reader))
    })? {
        out.push(item.reference);
        reader.advance();
    }
    Ok(out)
}

/// How many items the record in charge holds, read back from the file.
fn owed_items(db: &Db) -> Words<usize> {
    let retired = db.db.checkpointer().retired();
    let Some((place, checkpoint)) = db.db.checkpointer().superblock().retired else {
        return Ok(0);
    };
    let items = go("record", || retired.read_record(&db.io, place, checkpoint))?;
    Ok(items.len())
}

/// Every key 0 to `n` reads the value its second write wrote.
fn rows_read_back(db: &Db, n: usize) -> Words<()> {
    for k in 0..n {
        let read = db.read(&key(k))?;
        ensure!(
            read == Some(vec![k as u8 + 101; VALUE]),
            "key {k} reads {:?}",
            read.map(|v| (v.len(), v.first().copied()))
        );
    }
    Ok(())
}

/// A full checkpoint of `db`, stepped to its end.
fn checkpoint(db: &Db) -> Words<()> {
    let checkpointer = db.db.checkpointer();
    let mut ticket = checkpointer.request_full();
    go("checkpoint", || {
        drive(&db.io, || {
            checkpointer.step_full(&mut ticket, db.db.transactions())
        })
    })
}

/// The database a crash now would leave, opened.
fn reopen(crash: &CrashIO, root: PageId) -> Words<Db> {
    go("reopen", || Db::recover(Arc::new(crash.crash(0)), root, 1))
}

/// Steps the checkpoint asked for a unit at a time until it has made its cut.
fn until_cut(db: &Db) -> Words<()> {
    let checkpointer = db.db.checkpointer();
    while checkpointer.cut_taken().is_none() {
        if let Some(c) = go("unit", || checkpointer.one_unit(db.db.transactions()))? {
            go("wait", || {
                crate::storage::atoll::park::wait_for_completion(db.io.as_ref(), c)
            })?;
        }
    }
    Ok(())
}

/// The retired file's steps of a checkpoint, made by hand until they are done or fail: a cut
/// numbered past every checkpoint, so the page table is read as it stands.
fn file_steps(db: &Db, run: &mut crate::storage::atoll::gc::RetiredRun, c: Ts) -> Result<()> {
    let retired = db.db.checkpointer().retired();
    let heights = db.db.store().heights();
    let store = db.db.store();
    let txns = db.db.transactions();
    while !drive(&db.io, || retired.step(run, txns, c, 99, &heights, store))? {}
    Ok(())
}

/// A step of the pass over `slot`'s list, `budget` pages at most: the pages it freed.
fn free(db: &Db, slot: u16, budget: u64) -> Words<u64> {
    let retired = db.db.checkpointer().retired();
    let txns = db.db.transactions();
    go("free", || {
        drive(&db.io, || {
            retired.collector().step(
                slot,
                txns.collect_cutoff(),
                budget,
                retired.lists(),
                db.db.pool(),
                db.db.store(),
                txns,
            )
        })
    })
}

/// The pages of the value `reference` names, its directory first.
fn value_pages(db: &Db, reference: ValueRef) -> Words<Vec<PageId>> {
    let pages = go("pages", || {
        drive(&db.io, || {
            crate::storage::atoll::btree::value::pages(db.db.pool(), reference, &|| true)
        })
    })?;
    pages.map_err(|_| "its pages not read".into())
}

fn faults(write: f64) -> Faults {
    Faults {
        read: 0.0,
        write,
        sync: write,
        short_write: 0.0,
    }
}

fn go<T>(what: &str, op: impl FnOnce() -> Result<T>) -> Words<T> {
    op().map_err(|e| format!("{what}: {e}"))
}

fn key(k: usize) -> String {
    format!("key{k:04}")
}

fn path() -> String {
    format!("{BASE}.retired")
}

/// Bytes of a value of a directory and two chunks.
fn chunked() -> usize {
    crate::storage::atoll::btree::value::chunk() + 12_345
}

/// Steps the checkpoint under way a unit at a time while `go_on` holds for its phase, `None` before
/// one begins and once it has ended.
fn step_while(db: &Db, go_on: impl Fn(Option<&str>) -> bool) -> Words<()> {
    let checkpointer = db.db.checkpointer();
    for _ in 0..1 << 16 {
        if !go_on(checkpointer.phase_for_test().as_deref()) {
            return Ok(());
        }
        if let Some(c) = go("unit", || checkpointer.one_unit(db.db.transactions()))? {
            go("wait", || {
                crate::storage::atoll::park::wait_for_completion(db.io.as_ref(), c)
            })?;
        }
    }
    Err("the checkpoint went on for 65536 units".into())
}

/// What `txn` reads of `key`.
fn read_as(db: &Db, txn: &mut Txn, key: &str) -> Words<Option<Vec<u8>>> {
    let mut op = Get::new(key.as_bytes());
    go("read", || {
        drive(&db.io, || {
            db.tree
                .get(db.db.transactions(), &db.versions, txn, &mut op)
        })
    })
}

/// Rolls `txn` back as S5's rollback will: its last `items` items cut off its slot's list first.
fn roll_back(db: &Db, txn: Txn, items: u64) -> Words<()> {
    let txns = db.db.transactions();
    let lists = db.db.checkpointer().retired().lists();
    let slot = txn.slot();
    let to = lists.end(slot) - items;
    go("cut back", || drive(&db.io, || lists.cut_back(slot, to)))?;
    let mut abort = crate::storage::atoll::txn::Abort::new(txn);
    go("rollback", || {
        drive(&db.io, || txns.rollback(&db.versions, &mut abort))
    })
}

/// The tree's check, every node's structure and fences.
fn tree_check(db: &Db) -> Words<()> {
    let tree = Tree::open(
        db.io.clone(),
        db.db.pool().clone(),
        db.db.store().clone(),
        db.tree.root(),
    )
    .with_head(HEADER)
    .with_reserves();
    let mut check = crate::storage::atoll::btree::Check::default();
    go("the tree's check", || {
        drive(&db.io, || tree.check(&mut check))
    })
    .map(|_| ())
}

/// The classes of `pages`, each once.
fn classes_of(pages: &[PageId]) -> Vec<u64> {
    let classes: std::collections::BTreeSet<u64> =
        pages.iter().map(|p| p.class().index() as u64).collect();
    classes.into_iter().collect()
}

/// The pages in use of each of `classes`.
fn in_use_of(db: &Db, classes: &[u64]) -> Words<BTreeMap<u64, u64>> {
    classes
        .iter()
        .map(|&class| Ok((class, in_use(db, class)?)))
        .collect()
}

/// `counts` less `pages`, by class.
fn less(mut counts: BTreeMap<u64, u64>, pages: &[PageId]) -> BTreeMap<u64, u64> {
    for p in pages {
        if let Some(n) = counts.get_mut(&(p.class().index() as u64)) {
            *n -= 1;
        }
    }
    counts
}

/// The number of the checkpoint whose superblock is in charge on `io`, read from its control file.
fn in_charge(io: &Arc<dyn IO>) -> Words<u64> {
    let (_, sb) = go("the control file", || {
        Control::open(
            crate::storage::atoll::requests::counted_for_test(io.clone()),
            BASE,
        )
    })?;
    Ok(sb.ok_or("no superblock in charge")?.checkpoint)
}

/// The retired file's cost probes 1 and 2 (the auditor's #6328, 4), measurement only, each
/// `#[ignore]`d and run alone, `--exact --test-threads 1`, in a release build. Each counts this
/// thread's user-space instructions with a perf counter where the kernel offers one, and prints
/// exact totals; under callgrind, where it counts none, its counted calls are functions of their
/// own (`gc_cost_*`) for `--toggle-collect` and a split by function. The base's copy, for E1b's
/// commit without the packet, is this module less its regions marked packet only and with its
/// lines marked base only; probe 2 is packet only, as the base has no retired file.
#[cfg(target_os = "linux")]
mod cost {
    use super::*;
    use crate::storage::atoll::checkpoint::Checkpointer;
    use crate::storage::atoll::txn::Transactions;

    type Words<T> = std::result::Result<T, String>;

    /// Runs a probe's body, its databases dropped before its words fail it.
    fn words(body: impl FnOnce() -> Words<()>) {
        if let Err(words) = body() {
            panic!("{words}");
        }
    }

    fn ensure(holds: bool, words: impl FnOnce() -> String) -> Words<()> {
        if holds {
            Ok(())
        } else {
            Err(words())
        }
    }

    fn go<T>(what: &str, op: impl FnOnce() -> Result<T>) -> Words<T> {
        op().map_err(|e| format!("{what}: {e}"))
    }

    /// A user-space instruction counter over this thread, opened disabled; its descriptor closes
    /// as it drops.
    struct Instructions(std::os::fd::OwnedFd);

    impl Instructions {
        /// The counter, or none under valgrind, which counts its own instructions, or where the
        /// kernel offers none.
        fn open() -> Option<Self> {
            if std::env::var("LD_PRELOAD").is_ok_and(|p| p.contains("vgpreload")) {
                return None;
            }
            let mut attr = [0u64; 8];
            // type PERF_TYPE_HARDWARE (0), size 64 (VER0); config PERF_COUNT_HW_INSTRUCTIONS, on
            // the PMU of this CPU's core type where the machine has two.
            attr[0] = 64 << 32;
            attr[1] = (core_pmu() << 32) | 1;
            // read_format: TOTAL_TIME_ENABLED | TOTAL_TIME_RUNNING.
            attr[4] = 1 | 2;
            // disabled, exclude_kernel, exclude_hv.
            attr[5] = 1 | (1 << 5) | (1 << 6);
            // SAFETY: a perf_event_attr of VER0's 64 bytes, for this thread, any CPU.
            let fd =
                unsafe { libc::syscall(libc::SYS_perf_event_open, attr.as_ptr(), 0, -1, -1, 0) }
                    as i32;
            // SAFETY: a descriptor perf_event_open just opened, owned by nothing else.
            (fd >= 0).then(|| Self(unsafe { std::os::fd::FromRawFd::from_raw_fd(fd) }))
        }

        /// Counts from now until [`pause`](Self::pause).
        fn go(&self) -> Words<()> {
            self.ioctl(0x2400, "ENABLE")
        }

        fn pause(&self) -> Words<()> {
            self.ioctl(0x2401, "DISABLE")
        }

        fn ioctl(&self, op: libc::c_ulong, what: &str) -> Words<()> {
            // SAFETY: an ioctl on the counter's own descriptor.
            let r = unsafe { libc::ioctl(std::os::fd::AsRawFd::as_raw_fd(&self.0), op, 0) };
            ensure(r == 0, || {
                format!("{what}: {}", std::io::Error::last_os_error())
            })
        }

        /// The instructions counted so far, the counter never multiplexed.
        fn read(&self) -> Words<u64> {
            let mut v = [0u64; 3];
            // SAFETY: reads 24 bytes into `v`, which holds 24.
            let n = unsafe {
                libc::read(
                    std::os::fd::AsRawFd::as_raw_fd(&self.0),
                    v.as_mut_ptr() as *mut libc::c_void,
                    24,
                )
            };
            ensure(n == 24, || {
                format!("read: {n}, {}", std::io::Error::last_os_error())
            })?;
            ensure(v[1] == v[2], || {
                format!("the counter multiplexed: enabled {} running {}", v[1], v[2])
            })?;
            Ok(v[0])
        }
    }

    /// The type of the PMU that counts this thread's CPU on a machine of two core types (Linux's
    /// `cpu_core` and `cpu_atom`), or 0 where there is one: a hardware event with no PMU named
    /// counts on the first core type only, and on the other runs never.
    fn core_pmu() -> u64 {
        // SAFETY: no arguments; returns this thread's CPU or -1.
        let cpu = unsafe { libc::sched_getcpu() };
        let read = |pmu: &str, file: &str| {
            std::fs::read_to_string(format!("/sys/bus/event_source/devices/{pmu}/{file}"))
        };
        for pmu in ["cpu_core", "cpu_atom"] {
            let (Ok(cpus), Ok(kind)) = (read(pmu, "cpus"), read(pmu, "type")) else {
                continue;
            };
            let holds = cpus.trim().split(',').any(|range| {
                let mut ends = range.split('-').map(|n| n.parse::<i32>().unwrap_or(-2));
                let first = ends.next().unwrap_or(-2);
                let last = ends.next().unwrap_or(first);
                (first..=last).contains(&cpu)
            });
            if holds {
                return kind.trim().parse().unwrap_or(0);
            }
        }
        0
    }

    /// Runs `f` counted: its instructions (none without a counter) and its allocations.
    fn counted<T>(
        counter: &Option<Instructions>,
        f: impl FnOnce() -> T,
    ) -> Words<(T, Option<u64>, u64)> {
        let before = counter.as_ref().map(Instructions::read).transpose()?;
        let allocs = crate::storage::atoll::testing::allocations();
        if let Some(c) = counter {
            c.go()?;
        }
        let out = f();
        if let Some(c) = counter {
            c.pause()?;
        }
        let allocs = crate::storage::atoll::testing::allocations() - allocs;
        let after = counter.as_ref().map(Instructions::read).transpose()?;
        Ok((out, after.zip(before).map(|(a, b)| a - b), allocs))
    }

    fn shown(n: Option<u64>) -> String {
        n.map_or("none".into(), |n| n.to_string())
    }

    /// Probe 1: one full checkpoint of a database whose lists are empty, at the default 100
    /// slots, stepped a unit at a time from its ask to its end: the units, instructions and
    /// allocations of each phase a unit began in, and of the whole. Packet only: the Retired
    /// phase's list pages read and items judged, which are every page it can visit (a list page
    /// looked at, a directory page judged; its plan and writes visit none), the retired file's
    /// writes, and its syncs, faulted throughout, so that one would fail the checkpoint.
    #[test]
    #[ignore]
    fn gc_cost_a_checkpoint_with_nothing_retired() {
        words(|| {
            let crash = Arc::new(CrashIO::new(1));
            let db = Db::create(crash.clone());
            for k in 0..20u8 {
                db.put(&format!("cost-{k:02}"), &[k; 100]);
            }
            let measured = nothing_retired(&db, &crash);
            // packet only: begin
            crash.set_faults(no_faults());
            // packet only: end
            drop(db);
            println!("{}", measured?);
            Ok(())
        })
    }

    /// The probe's checkpoint on `db`, which runs on `crash`: its lines.
    fn nothing_retired(db: &Db, crash: &CrashIO) -> Words<String> {
        let checkpointer = db.db.checkpointer();
        let txns = db.db.transactions();
        let slots = txns.slot_count();
        while checkpointer.is_under_way() {
            if let Some(c) = go("an earlier checkpoint", || checkpointer.one_unit(txns))? {
                wait(db, c)?;
            }
        }
        // packet only: begin
        let before = RetiredFile::before(db, crash);
        // packet only: end
        // base only: let _ = crash;
        let counter = Instructions::open();
        let done = checkpointer.fulls_done();
        let mut ticket = checkpointer.request_full();
        let mut phases: BTreeMap<String, (u64, u64, u64)> = BTreeMap::new();
        let mut whole = (0u64, Some(0u64), 0u64);
        while checkpointer.fulls_done() == done {
            ensure(whole.0 < 1 << 20, || "the checkpoint did not end".into())?;
            let phase = phase_of(checkpointer);
            let unit = unit_of(&phase);
            let (wait_on, n, allocs) = counted(&counter, || unit(checkpointer, txns))?;
            let entry = phases.entry(phase).or_default();
            entry.0 += 1;
            entry.1 += n.unwrap_or(0);
            entry.2 += allocs;
            whole.0 += 1;
            whole.1 = whole.1.zip(n).map(|(a, b)| a + b);
            whole.2 += allocs;
            if let Some(c) = go("unit", || wait_on)? {
                wait(db, c)?;
            }
        }
        go("the checkpoint's end", || {
            drive(&db.io, || checkpointer.step_full(&mut ticket, txns))
        })?;
        let whole = (whole.0, whole.1.filter(|_| counter.is_some()), whole.2);
        let mut lines = vec![format!(
            "COST gc-checkpoint-nothing-retired slots={slots} units={} instructions={} allocs={} \
             counter={}",
            whole.0,
            shown(whole.1),
            whole.2,
            if counter.is_some() { "perf" } else { "none" }
        )];
        for (phase, (units, n, allocs)) in &phases {
            lines.push(format!(
                "COST gc-checkpoint-phase {phase} units={units} instructions={} allocs={allocs}",
                shown(whole.1.map(|_| *n))
            ));
        }
        // packet only: begin
        lines.push(before.line(db, crash, &phases, whole.1));
        // packet only: end
        Ok(lines.join("\n"))
    }

    /// The phase the checkpoint's next unit runs in.
    fn phase_of(checkpointer: &Checkpointer) -> String {
        // packet only: begin
        checkpointer.phase_for_test().unwrap_or_else(|| "-".into())
        // packet only: end
        // base only: let _ = checkpointer;
        // base only: "-".into()
    }

    type UnitCall = fn(&Checkpointer, &Transactions) -> Result<Option<Completion>>;

    /// The function a unit of `phase` is counted in.
    fn unit_of(phase: &str) -> UnitCall {
        // packet only: begin
        if phase == "Retired" {
            return gc_cost_unit_retired;
        }
        // packet only: end
        // base only: let _ = phase;
        gc_cost_unit
    }

    /// One unit of the checkpoint, in a function of its own for callgrind: the units of every phase
    /// but the Retired one.
    #[inline(never)]
    fn gc_cost_unit(
        checkpointer: &Checkpointer,
        txns: &Transactions,
    ) -> Result<Option<Completion>> {
        checkpointer.one_unit(std::hint::black_box(txns))
    }

    fn wait(db: &Db, c: Completion) -> Words<()> {
        go("wait", || {
            crate::storage::atoll::park::wait_for_completion(db.io.as_ref(), c)
        })
    }

    // packet only: begin
    /// One unit of the checkpoint's Retired phase, in a function of its own for callgrind. Its
    /// body differs from [`gc_cost_unit`]'s, so the two are not merged.
    #[inline(never)]
    fn gc_cost_unit_retired(
        checkpointer: &Checkpointer,
        txns: &Transactions,
    ) -> Result<Option<Completion>> {
        std::hint::black_box(1u8);
        checkpointer.one_unit(std::hint::black_box(txns))
    }

    /// The retired file's counts before the probe's checkpoint: its writes so far and the items
    /// judged so far, the list pages read taken, its syncs faulted.
    struct RetiredFile {
        writes: usize,
        judged: usize,
    }

    impl RetiredFile {
        fn before(db: &Db, crash: &CrashIO) -> Self {
            let writes = crash.writes_to(&path()).len();
            crash.set_faults_on(Some(&path()));
            crash.set_faults(Faults {
                sync: 1.0,
                ..no_faults()
            });
            crate::storage::atoll::gc::take_list_reads();
            let judged = db.db.checkpointer().retired().judged_in_all();
            Self { writes, judged }
        }

        /// The Retired phase's line: its units and their share of the whole's instructions, and
        /// what it read and wrote.
        fn line(
            &self,
            db: &Db,
            crash: &CrashIO,
            phases: &BTreeMap<String, (u64, u64, u64)>,
            whole: Option<u64>,
        ) -> String {
            let list_reads = crate::storage::atoll::gc::take_list_reads();
            let judged = db.db.checkpointer().retired().judged_in_all() - self.judged;
            let writes = crash.writes_to(&path()).len() - self.writes;
            let (units, n, _) = phases.get("Retired").copied().unwrap_or_default();
            let share = whole.map_or("none".into(), |w| {
                format!("{n} of {w}, {:.4}%", 100.0 * n as f64 / w as f64)
            });
            format!(
                "COST gc-retired-phase units={units} list-pages-read={list_reads} \
                 items-judged={judged} retired-file-writes={writes} retired-file-syncs=0 \
                 instructions-share={share}"
            )
        }
    }

    fn no_faults() -> Faults {
        Faults {
            read: 0.0,
            write: 0.0,
            sync: 0.0,
            short_write: 0.0,
        }
    }

    /// Probe 2: the cut's read of every list's head, `Retired::cut`, N times on a
    /// database of 100 slots; with [`gc_cost_the_cuts_head_read_at_1000_slots`], the cost a slot.
    #[test]
    #[ignore]
    fn gc_cost_the_cuts_head_read_at_100_slots() {
        cut_heads(100)
    }

    /// Probe 2 at 1,000 slots.
    #[test]
    #[ignore]
    fn gc_cost_the_cuts_head_read_at_1000_slots() {
        cut_heads(1_000)
    }

    const CUTS: u64 = 10_000;

    fn cut_heads(slots: usize) {
        words(|| {
            let io: Arc<dyn IO> = Arc::new(CrashIO::new(2));
            let mut config = super::super::config();
            config.txn.slots = slots;
            let db = go("open", || super::super::try_open_with(io, 1, config))?;
            let retired = db.checkpointer().retired();
            let mut run = crate::storage::atoll::gc::RetiredRun::default();
            retired.cut(&mut run);
            let counter = Instructions::open();
            let measured = counted(&counter, || gc_cost_cuts(retired, &mut run, CUTS));
            retired.failed(&mut run);
            drop(db);
            let (_, n, allocs) = measured?;
            println!(
                "COST gc-cut-heads slots={slots} cuts={CUTS} instructions={} allocs={allocs} \
                 counter={}",
                shown(n),
                if counter.is_some() { "perf" } else { "none" }
            );
            Ok(())
        })
    }

    /// `n` cuts of `retired` into `run`, in a function of its own for callgrind.
    #[inline(never)]
    fn gc_cost_cuts(
        retired: &crate::storage::atoll::gc::Retired,
        run: &mut crate::storage::atoll::gc::RetiredRun,
        n: u64,
    ) {
        for _ in 0..n {
            retired.cut(std::hint::black_box(&mut *run));
        }
    }
    // packet only: end
}
