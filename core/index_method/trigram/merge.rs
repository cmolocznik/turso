//! Merges of segments (`OPTIMIZE INDEX`), done in steps that each write a bounded share, so that
//! no transaction of one is large: a replicated database refuses a commit larger than one entry of
//! its log.
//!
//! Which segments to merge is Tantivy's `LogMergePolicy`, with its defaults: the segments, largest
//! first, fall into levels a factor of 2^0.75 apart in size, those of fewer than 10,000 texts
//! counted as of 10,000, and a level of eight or more is merged, smallest first. A segment of more
//! than 10,000,000 texts is merged no more, and no merge makes one larger.
//!
//! A merge of segments into a new one goes through these steps, each in a transaction of its own,
//! its progress in its rows of `merges`:
//!
//! 1. It claims its segments, a `claims` entry each: a second merge claiming one meets a write
//!    conflict.
//! 2. It copies each text of its segments to the new one, numbered after the texts of the
//!    segments before it, except a text removed by then: that one it notes in `dropped`, by its
//!    number and identity.
//! 3. It writes the lists of the new segment, a trigram at a time in order: each segment's
//!    numbers moved up as its texts were, without the dropped ones.
//! 4. It makes the new segment visible and its segments no more, in one transaction.
//! 5. It removes its segments' rows, the tombstones of the texts it dropped, and its own rows.
//!
//! Until step 4 a query reads the old segments, after it the new one. A text removed while the
//! merge runs keeps its identity in the new segment, so its tombstone hides it there too.
//!
//! On a connection told to (`Connection::set_index_merges_whole`), a merge takes every segment
//! instead, as Lucene's `forceMerge(1)`, and a lone segment that holds a removed text too: so no
//! list holds a number of a text removed before it (atoll-forge's decision 103).

use std::collections::HashSet;

use super::{
    blob, blob_at, corrupt, decode_block, encode_blocks, integer_at, key, segment_id, Cursors,
    Reader, Tree, WriteOp, Writes,
};
use crate::index_method::{IndexMethodContext, OptimizeProgress};
use crate::types::{IOResult, IOResultOr};
use crate::{return_if_io, Result, Value};

/// Tantivy's `LogMergePolicy` defaults (`DEFAULT_MIN_NUM_SEGMENTS_IN_MERGE` and the rest).
const MIN_SEGMENTS: usize = 8;
const MAX_TEXTS: u64 = 10_000_000;
const MIN_LAYER: u32 = 10_000;
const LEVEL_LOG_SIZE: f64 = 0.75;

/// The most a step writes, in bytes of the entries it adds and removes: a mebibyte, as the forge's
/// object store writes a large object a mebibyte at a time, each in a transaction of its own.
const STEP_BYTES: usize = 1 << 20;

crate::thread::thread_local! {
    static STEP_BYTES_OVERRIDE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// Sets how much a step of a merge writes on this thread, so that tests can make a merge of a
/// few texts take many steps. `None` restores the default.
#[cfg(any(test, feature = "test_helper"))]
pub fn set_trigram_step_bytes_for_test(bytes: Option<usize>) {
    STEP_BYTES_OVERRIDE.with(|cell| cell.set(bytes));
}

fn step_bytes() -> usize {
    STEP_BYTES_OVERRIDE
        .with(|cell| cell.get())
        .unwrap_or(STEP_BYTES)
}

/// The segments `LogMergePolicy` would merge next, of `segments` with their sizes: the level of
/// the smallest that has enough, as many of it as keep the merged segment within the largest.
pub(super) fn choose(segments: &[(Vec<u8>, u32)]) -> Option<Vec<(Vec<u8>, u32)>> {
    let mut sorted: Vec<&(Vec<u8>, u32)> = segments
        .iter()
        .filter(|(_, texts)| u64::from(*texts) <= MAX_TEXTS)
        .collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let mut levels: Vec<Vec<&(Vec<u8>, u32)>> = Vec::new();
    let mut current = f64::MAX;
    for segment in sorted {
        let size = f64::from(segment.1.max(MIN_LAYER)).log2();
        if size < current - LEVEL_LOG_SIZE {
            current = size;
            levels.push(Vec::new());
        }
        if levels.is_empty() {
            levels.push(Vec::new());
        }
        levels.last_mut().expect("a level").push(segment);
    }
    for level in levels.iter().rev() {
        if level.len() < MIN_SEGMENTS {
            continue;
        }
        // Smallest first, as many as the merged segment holds.
        let mut chosen = Vec::new();
        let mut total = 0u64;
        for segment in level.iter().rev() {
            if total + u64::from(segment.1) > MAX_TEXTS {
                break;
            }
            total += u64::from(segment.1);
            chosen.push((*segment).clone());
        }
        if chosen.len() >= 2 {
            return Some(chosen);
        }
    }
    None
}

/// Every segment, smallest first, as many as one merged segment holds: a whole merge's, of two
/// or more; a lone one is merged only if it holds a removed text, which is read apart.
fn choose_whole(segments: &[(Vec<u8>, u32)]) -> Option<Vec<(Vec<u8>, u32)>> {
    let mut sorted: Vec<&(Vec<u8>, u32)> = segments.iter().collect();
    sorted.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    let mut chosen = Vec::new();
    let mut total = 0u64;
    for segment in sorted {
        if !chosen.is_empty() && total + u64::from(segment.1) > MAX_TEXTS {
            break;
        }
        total += u64::from(segment.1);
        chosen.push(segment.clone());
    }
    (chosen.len() >= 2).then_some(chosen)
}

/// What `merges` holds of a merge: its segments, in order, with their sizes, and how far it is.
#[derive(Debug, Clone)]
struct Merge {
    id: Vec<u8>,
    sources: Vec<(Vec<u8>, u32)>,
    progress: Progress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Progress {
    /// Copying texts: the next is `number` of source `source`.
    Docs {
        source: usize,
        number: u32,
    },
    /// Writing lists: the next is trigram `trigram`'s, from source `source`'s block at or above
    /// number `first`: a step may stop within a trigram whose list is larger than it.
    Postings {
        trigram: i64,
        source: usize,
        first: i64,
    },
    Swap,
    Cleanup,
}

/// `merges` rows: a source is (merge, 0, its place, segment, size); the progress is
/// (merge, 1, phase, empty, cursor).
const SOURCE: i64 = 0;
const PROGRESS: i64 = 1;

impl Progress {
    /// The progress row's phase, place and cursor.
    fn encode(self) -> (i64, Vec<u8>, i64) {
        match self {
            Self::Docs { source, number } => {
                (0, Vec::new(), ((source as i64) << 32) | i64::from(number))
            }
            Self::Postings {
                trigram,
                source,
                first,
            } => {
                let mut place = (source as u64).to_be_bytes().to_vec();
                place.extend_from_slice(&first.to_be_bytes());
                (1, place, trigram)
            }
            Self::Swap => (2, Vec::new(), 0),
            Self::Cleanup => (3, Vec::new(), 0),
        }
    }

    fn decode(phase: i64, place: &[u8], cursor: i64) -> Result<Self> {
        let bad = || *corrupt("a merge's progress");
        Ok(match phase {
            0 => Self::Docs {
                source: usize::try_from(cursor >> 32).map_err(|_| bad())?,
                number: u32::try_from(cursor & 0xffff_ffff).map_err(|_| bad())?,
            },
            1 => {
                let (source, first) = place.split_at_checked(8).ok_or_else(bad)?;
                Self::Postings {
                    trigram: cursor,
                    source: usize::try_from(u64::from_be_bytes(
                        source.try_into().map_err(|_| bad())?,
                    ))
                    .map_err(|_| bad())?,
                    first: i64::from_be_bytes(first.try_into().map_err(|_| bad())?),
                }
            }
            2 => Self::Swap,
            3 => Self::Cleanup,
            _ => return Err(*corrupt("a merge's phase")),
        })
    }
}

impl Merge {
    fn offset(&self, source: usize) -> u32 {
        self.sources[..source].iter().map(|(_, texts)| texts).sum()
    }

    fn texts(&self) -> u32 {
        self.sources.iter().map(|(_, texts)| texts).sum()
    }

    fn progress_key(&self, progress: Progress) -> Result<crate::types::ImmutableRecord> {
        let (phase, place, cursor) = progress.encode();
        key(&[
            blob(&self.id),
            Value::from_i64(PROGRESS),
            Value::from_i64(phase),
            Value::from_blob(place),
            Value::from_i64(cursor),
        ])
    }

    fn source_key(&self, place: usize) -> Result<crate::types::ImmutableRecord> {
        let (segment, texts) = &self.sources[place];
        key(&[
            blob(&self.id),
            Value::from_i64(SOURCE),
            Value::from_i64(place as i64),
            blob(segment),
            Value::from_i64(i64::from(*texts)),
        ])
    }
}

/// One step of `OPTIMIZE INDEX` in progress.
pub(super) struct Step {
    /// The first step of a run: it finds whether there is work, and does none.
    first: bool,
    state: State,
    writes: Writes,
    bytes: usize,
    outcome: OptimizeProgress,
}

/// A merge's rows as `FindMerge` reads them: its id, its sources with their sizes, and its
/// progress once read.
type FoundMerge = (Vec<u8>, Vec<(Vec<u8>, u32)>, Option<Progress>);

enum State {
    /// Reading `merges` for a merge under way.
    FindMerge {
        reader: Reader,
        merge: Option<FoundMerge>,
    },
    /// No merge is under way: every segment, to choose from.
    Segments {
        reader: Reader,
        segments: Vec<(Vec<u8>, u32)>,
    },
    /// A whole merge's lone segment, merged on its own if any text was removed: `reader` stands
    /// at the first tombstone.
    Lone {
        reader: Reader,
        segment: (Vec<u8>, u32),
    },
    /// Copying texts: `reader` stands in `docs` at the next of the current source.
    Docs {
        merge: Merge,
        source: usize,
        reader: Reader,
        /// The text read, whose tombstone is being looked for.
        text: Option<(u32, i64, Vec<u8>, Reader)>,
    },
    LoadDropped {
        merge: Merge,
        resume: (i64, usize, i64),
        reader: Reader,
        dropped: HashSet<u32>,
    },
    Postings(Box<Postings>),
    Cleanup {
        merge: Merge,
        part: Cleanup,
        reader: Reader,
    },
    Write,
    Finished,
}

struct Postings {
    merge: Merge,
    dropped: HashSet<u32>,
    trigram: i64,
    source: usize,
    reader: Reader,
    numbers: Vec<u32>,
    /// The lowest trigram above `trigram` some source holds.
    next: Option<i64>,
    /// The sources before this one a step resumed after, in a trigram it had begun: the next
    /// trigram of each is looked for at its first entry above `trigram`. None once looked for.
    probing: Option<usize>,
    /// Where in the current source's list of `trigram` to start: its block at or above it.
    resume_first: i64,
    /// The bytes of the source entries read into `numbers`, about what writing them takes.
    pending_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
enum Cleanup {
    Postings(usize),
    Docs(usize),
    Dropped,
}

fn source_reader(tree_prefix: &[Value]) -> Result<Reader> {
    Ok(Reader::at(key(tree_prefix)?))
}

impl Step {
    pub(super) fn new(first: bool) -> Self {
        Self {
            first,
            state: State::FindMerge {
                reader: Reader::from_first(),
                merge: None,
            },
            writes: Writes::default(),
            bytes: 0,
            outcome: OptimizeProgress::Done,
        }
    }

    fn insert(&mut self, tree: Tree, record: crate::types::ImmutableRecord) {
        self.bytes += record.get_payload().len();
        self.writes.ops.push_back(WriteOp::Insert(tree, record));
    }

    fn delete(&mut self, tree: Tree, record: crate::types::ImmutableRecord) {
        self.bytes += record.get_payload().len();
        self.writes.ops.push_back(WriteOp::Delete(tree, record));
    }

    fn full(&self) -> bool {
        self.bytes >= step_bytes()
    }

    /// Writes `numbers`, part or all of the merged list of `trigram`, as rows of segment
    /// `merge`.
    fn write_list(&mut self, merge: &[u8], trigram: i64, numbers: &[u32]) -> Result<()> {
        for (first, rest) in encode_blocks(numbers) {
            self.insert(
                Tree::Postings,
                key(&[
                    blob(merge),
                    Value::from_i64(trigram),
                    Value::from_i64(first),
                    Value::from_blob(rest),
                ])?,
            );
        }
        Ok(())
    }

    /// Records the merge's progress, replacing what it was, and ends the step.
    fn advance_to(&mut self, merge: &Merge, from: Progress, to: Progress) -> Result<()> {
        self.delete(Tree::Merges, merge.progress_key(from)?);
        self.insert(Tree::Merges, merge.progress_key(to)?);
        self.outcome = OptimizeProgress::Moved;
        self.state = State::Write;
        Ok(())
    }

    pub(super) fn run(
        &mut self,
        cursors: &mut Cursors,
        context: &IndexMethodContext,
    ) -> IOResultOr<OptimizeProgress> {
        loop {
            match &mut self.state {
                State::FindMerge { reader, merge } => {
                    let entry = return_if_io!(reader.step(cursors.get(Tree::Merges)?));
                    let Some(entry) = entry else {
                        if let Some((id, sources, Some(progress))) = merge.take() {
                            self.start(Merge {
                                id,
                                sources,
                                progress,
                            })?;
                        } else if merge.is_some() {
                            return Err(corrupt("a merge without its progress"));
                        } else {
                            self.state = State::Segments {
                                reader: Reader::from_first(),
                                segments: Vec::new(),
                            };
                        }
                        continue;
                    };
                    let id = blob_at(&entry, 0)?;
                    if merge.as_ref().is_some_and(|(found, _, _)| *found != id) {
                        // The rows of the first merge are read: carry it on.
                        let (id, sources, progress) = merge.take().expect("a merge");
                        let progress =
                            progress.ok_or_else(|| *corrupt("a merge without its progress"))?;
                        self.start(Merge {
                            id,
                            sources,
                            progress,
                        })?;
                        continue;
                    }
                    reader.advance();
                    let (_, sources, progress) =
                        merge.get_or_insert_with(|| (id, Vec::new(), None));
                    match integer_at(&entry, 1)? {
                        SOURCE => {
                            let texts = u32::try_from(integer_at(&entry, 4)?)
                                .map_err(|_| *corrupt("a source's size"))?;
                            sources.push((blob_at(&entry, 3)?, texts));
                        }
                        PROGRESS => {
                            *progress = Some(Progress::decode(
                                integer_at(&entry, 2)?,
                                &blob_at(&entry, 3)?,
                                integer_at(&entry, 4)?,
                            )?);
                        }
                        _ => return Err(corrupt("a merge's row")),
                    }
                }
                State::Segments { reader, segments } => {
                    let entry = return_if_io!(reader.step(cursors.get(Tree::Segments)?));
                    if let Some(entry) = entry {
                        reader.advance();
                        let texts = u32::try_from(integer_at(&entry, 1)?)
                            .map_err(|_| *corrupt("a segment's size"))?;
                        segments.push((blob_at(&entry, 0)?, texts));
                        continue;
                    }
                    let chosen = if context.connection()?.index_merges_whole() {
                        if let [segment] = segments.as_slice() {
                            self.state = State::Lone {
                                reader: Reader::from_first(),
                                segment: segment.clone(),
                            };
                            continue;
                        }
                        choose_whole(segments)
                    } else {
                        choose(segments)
                    };
                    let Some(sources) = chosen else {
                        self.outcome = OptimizeProgress::Done;
                        self.state = State::Finished;
                        continue;
                    };
                    self.begin(sources, context)?;
                }
                State::Lone { reader, segment } => {
                    let entry = return_if_io!(reader.step(cursors.get(Tree::Tombs)?));
                    let Some(_) = entry else {
                        self.outcome = OptimizeProgress::Done;
                        self.state = State::Finished;
                        continue;
                    };
                    let sources = vec![segment.clone()];
                    self.begin(sources, context)?;
                }
                State::Docs {
                    merge,
                    source,
                    reader,
                    text,
                } => {
                    if let Some((number, row, origin, tomb)) = text {
                        let entry = return_if_io!(tomb.step(cursors.get(Tree::Tombs)?));
                        let removed = match &entry {
                            Some(entry) => {
                                blob_at(entry, 0)? == *origin && integer_at(entry, 1)? == *row
                            }
                            None => false,
                        };
                        let renumbered = merge.offset(*source) + *number;
                        let (number, row, origin) = (*number, *row, origin.clone());
                        *text = None;
                        reader.advance();
                        let id = merge.id.clone();
                        let record = if removed {
                            (
                                Tree::Dropped,
                                key(&[
                                    blob(&id),
                                    Value::from_i64(i64::from(renumbered)),
                                    blob(&origin),
                                    Value::from_i64(row),
                                ])?,
                            )
                        } else {
                            (
                                Tree::Docs,
                                key(&[
                                    blob(&id),
                                    Value::from_i64(i64::from(renumbered)),
                                    Value::from_i64(row),
                                    blob(&origin),
                                ])?,
                            )
                        };
                        self.insert(record.0, record.1);
                        if self.full() {
                            let State::Docs { merge, source, .. } = &self.state else {
                                unreachable!("in Docs");
                            };
                            let (merge, from) = (merge.clone(), merge.progress);
                            let to = Progress::Docs {
                                source: *source,
                                number: number + 1,
                            };
                            self.advance_to(&merge, from, to)?;
                        }
                        continue;
                    }
                    let entry = return_if_io!(reader.step(cursors.get(Tree::Docs)?));
                    let segment = &merge.sources[*source].0;
                    let ours = entry
                        .as_ref()
                        .map(|entry| blob_at(entry, 0).map(|s| s == *segment))
                        .transpose()?
                        .unwrap_or(false);
                    if ours {
                        let entry = entry.expect("ours");
                        let number = u32::try_from(integer_at(&entry, 1)?)
                            .map_err(|_| *corrupt("a text's number"))?;
                        let (row, origin) = (integer_at(&entry, 2)?, blob_at(&entry, 3)?);
                        let tomb = Reader::at(key(&[blob(&origin), Value::from_i64(row)])?);
                        *text = Some((number, row, origin, tomb));
                        continue;
                    }
                    // The source's texts are copied: the next source, or the lists.
                    let next = *source + 1;
                    if next < merge.sources.len() {
                        *source = next;
                        *reader = source_reader(&[blob(&merge.sources[next].0)])?;
                        continue;
                    }
                    let merge = merge.clone();
                    let from = merge.progress;
                    let to = Progress::Postings {
                        trigram: 0,
                        source: 0,
                        first: 0,
                    };
                    self.advance_to(&merge, from, to)?;
                }
                State::LoadDropped {
                    merge,
                    resume,
                    reader,
                    dropped,
                } => {
                    let entry = return_if_io!(reader.step(cursors.get(Tree::Dropped)?));
                    if let Some(entry) =
                        entry.filter(|entry| blob_at(entry, 0).is_ok_and(|m| m == merge.id))
                    {
                        reader.advance();
                        dropped.insert(
                            u32::try_from(integer_at(&entry, 1)?)
                                .map_err(|_| *corrupt("a dropped text's number"))?,
                        );
                        continue;
                    }
                    let merge = merge.clone();
                    let ((trigram, source, first), dropped) = (*resume, std::mem::take(dropped));
                    let (probing, reader) = if source > 0 {
                        (
                            Some(0),
                            source_reader(&[
                                blob(&merge.sources[0].0),
                                Value::from_i64(trigram + 1),
                            ])?,
                        )
                    } else {
                        (
                            None,
                            source_reader(&[
                                blob(&merge.sources[0].0),
                                Value::from_i64(trigram),
                                Value::from_i64(first),
                            ])?,
                        )
                    };
                    self.state = State::Postings(Box::new(Postings {
                        merge,
                        dropped,
                        trigram,
                        source,
                        reader,
                        numbers: Vec::new(),
                        next: None,
                        probing,
                        resume_first: 0,
                        pending_bytes: 0,
                    }));
                    let State::Postings(postings) = &mut self.state else {
                        unreachable!("made above");
                    };
                    postings.resume_first = first;
                }
                State::Postings(postings) => {
                    let entry = return_if_io!(postings.reader.step(cursors.get(Tree::Postings)?));
                    // A probe of a source the step resumed after: only its next trigram counts.
                    if let Some(probed) = postings.probing {
                        let segment = &postings.merge.sources[probed].0;
                        if let Some(entry) = &entry {
                            if blob_at(entry, 0)? == *segment {
                                let trigram = integer_at(entry, 1)?;
                                postings.next =
                                    Some(postings.next.map_or(trigram, |next| next.min(trigram)));
                            }
                        }
                        let following = probed + 1;
                        if following < postings.source {
                            postings.probing = Some(following);
                            postings.reader = source_reader(&[
                                blob(&postings.merge.sources[following].0),
                                Value::from_i64(postings.trigram + 1),
                            ])?;
                        } else {
                            postings.probing = None;
                            postings.reader = source_reader(&[
                                blob(&postings.merge.sources[postings.source].0),
                                Value::from_i64(postings.trigram),
                                Value::from_i64(postings.resume_first),
                            ])?;
                        }
                        continue;
                    }
                    let segment = &postings.merge.sources[postings.source].0;
                    let (ours, trigram) = match &entry {
                        Some(entry) if blob_at(entry, 0)? == *segment => {
                            (true, integer_at(entry, 1)?)
                        }
                        _ => (false, 0),
                    };
                    if ours && trigram == postings.trigram {
                        let entry = entry.expect("ours");
                        let first = integer_at(&entry, 2)?;
                        let rest = blob_at(&entry, 3)?;
                        let mut numbers = Vec::new();
                        decode_block(first, &rest, &mut numbers)?;
                        let offset = postings.merge.offset(postings.source);
                        for number in numbers {
                            let number = offset + number;
                            if !postings.dropped.contains(&number) {
                                postings.numbers.push(number);
                            }
                        }
                        postings.pending_bytes += rest.len() + 40;
                        postings.reader.advance();
                        if self.bytes + postings.pending_bytes >= step_bytes() {
                            // A list larger than a step: stop after this block, and resume at the
                            // next.
                            let (merge, trigram, source) =
                                (postings.merge.clone(), postings.trigram, postings.source);
                            let numbers = std::mem::take(&mut postings.numbers);
                            self.write_list(&merge.id, trigram, &numbers)?;
                            let from = merge.progress;
                            let to = Progress::Postings {
                                trigram,
                                source,
                                first: first + 1,
                            };
                            self.advance_to(&merge, from, to)?;
                        }
                        continue;
                    }
                    if ours {
                        postings.next =
                            Some(postings.next.map_or(trigram, |next| next.min(trigram)));
                    }
                    let next_source = postings.source + 1;
                    if next_source < postings.merge.sources.len() {
                        postings.source = next_source;
                        postings.reader = source_reader(&[
                            blob(&postings.merge.sources[next_source].0),
                            Value::from_i64(postings.trigram),
                        ])?;
                        continue;
                    }
                    // Every source's list of this trigram is read: write the merged one.
                    let numbers = std::mem::take(&mut postings.numbers);
                    postings.pending_bytes = 0;
                    let (merge, trigram, next) =
                        (postings.merge.clone(), postings.trigram, postings.next);
                    self.write_list(&merge.id, trigram, &numbers)?;
                    let from = merge.progress;
                    match next {
                        None => self.advance_to(&merge, from, Progress::Swap)?,
                        Some(next) if self.full() => {
                            let to = Progress::Postings {
                                trigram: next,
                                source: 0,
                                first: 0,
                            };
                            self.advance_to(&merge, from, to)?;
                        }
                        Some(next) => {
                            let State::Postings(postings) = &mut self.state else {
                                unreachable!("in Postings");
                            };
                            postings.trigram = next;
                            postings.source = 0;
                            postings.next = None;
                            postings.reader = source_reader(&[
                                blob(&postings.merge.sources[0].0),
                                Value::from_i64(next),
                            ])?;
                        }
                    }
                }
                State::Cleanup {
                    merge,
                    part,
                    reader,
                } => {
                    let tree = match part {
                        Cleanup::Postings(_) => Tree::Postings,
                        Cleanup::Docs(_) => Tree::Docs,
                        Cleanup::Dropped => Tree::Dropped,
                    };
                    let entry = return_if_io!(reader.step(cursors.get(tree)?));
                    let owner = match part {
                        Cleanup::Postings(source) | Cleanup::Docs(source) => {
                            merge.sources[*source].0.clone()
                        }
                        Cleanup::Dropped => merge.id.clone(),
                    };
                    if let Some(entry) =
                        entry.filter(|entry| blob_at(entry, 0).is_ok_and(|o| o == owner))
                    {
                        reader.advance();
                        let part = *part;
                        self.delete(tree, key(&entry)?);
                        if matches!(part, Cleanup::Dropped) {
                            self.delete(
                                Tree::Tombs,
                                key(&[
                                    Value::from_blob(blob_at(&entry, 2)?),
                                    Value::from_i64(integer_at(&entry, 3)?),
                                ])?,
                            );
                        }
                        if self.full() {
                            self.outcome = OptimizeProgress::Moved;
                            self.state = State::Write;
                        }
                        continue;
                    }
                    let next = match *part {
                        Cleanup::Postings(source) if source + 1 < merge.sources.len() => {
                            Some(Cleanup::Postings(source + 1))
                        }
                        Cleanup::Postings(_) => Some(Cleanup::Docs(0)),
                        Cleanup::Docs(source) if source + 1 < merge.sources.len() => {
                            Some(Cleanup::Docs(source + 1))
                        }
                        Cleanup::Docs(_) => Some(Cleanup::Dropped),
                        Cleanup::Dropped => None,
                    };
                    match next {
                        Some(next) => {
                            let owner = match next {
                                Cleanup::Postings(source) | Cleanup::Docs(source) => {
                                    merge.sources[source].0.clone()
                                }
                                Cleanup::Dropped => merge.id.clone(),
                            };
                            *part = next;
                            *reader = source_reader(&[blob(&owner)])?;
                        }
                        None => {
                            // Everything of the merge is gone but its own rows: they go last.
                            let merge = merge.clone();
                            for (segment, _) in &merge.sources {
                                self.delete(Tree::Claims, key(&[blob(segment)])?);
                            }
                            for place in 0..merge.sources.len() {
                                let record = merge.source_key(place)?;
                                self.delete(Tree::Merges, record);
                            }
                            self.delete(Tree::Merges, merge.progress_key(Progress::Cleanup)?);
                            self.outcome = OptimizeProgress::Moved;
                            self.state = State::Write;
                        }
                    }
                }
                State::Write => {
                    return_if_io!(self.writes.drain(cursors));
                    self.state = State::Finished;
                }
                State::Finished => return Ok(IOResult::Done(self.outcome)),
            }
        }
    }

    /// Carries on a merge from its progress: the first step of a run only says there is work.
    /// Begins a merge of `sources`: claims them and writes its rows, unless this step only says
    /// whether there is one.
    fn begin(&mut self, sources: Vec<(Vec<u8>, u32)>, context: &IndexMethodContext) -> Result<()> {
        self.outcome = OptimizeProgress::Moved;
        if self.first {
            self.state = State::Finished;
            return Ok(());
        }
        let merge = Merge {
            id: segment_id(context)?,
            sources,
            progress: Progress::Docs {
                source: 0,
                number: 0,
            },
        };
        // A claim is never read: one exists only beside its merge's rows, which a run finds and
        // carries on before it chooses. It is written so that two runs that chose a segment in
        // transactions side by side write one key, and one loses its commit.
        for (segment, _) in &merge.sources {
            self.insert(Tree::Claims, key(&[blob(segment)])?);
        }
        for place in 0..merge.sources.len() {
            let record = merge.source_key(place)?;
            self.insert(Tree::Merges, record);
        }
        self.insert(Tree::Merges, merge.progress_key(merge.progress)?);
        self.state = State::Write;
        Ok(())
    }

    fn start(&mut self, merge: Merge) -> Result<()> {
        if merge.sources.is_empty() {
            return Err(*corrupt("a merge of no segment"));
        }
        self.outcome = OptimizeProgress::Moved;
        if self.first {
            self.state = State::Finished;
            return Ok(());
        }
        self.state = match merge.progress {
            Progress::Docs { source, number } => {
                let segment = merge
                    .sources
                    .get(source)
                    .ok_or_else(|| *corrupt("a merge's progress"))?
                    .0
                    .clone();
                State::Docs {
                    reader: source_reader(&[blob(&segment), Value::from_i64(i64::from(number))])?,
                    merge,
                    source,
                    text: None,
                }
            }
            Progress::Postings {
                trigram,
                source,
                first,
            } => {
                if source >= merge.sources.len() {
                    return Err(*corrupt("a merge's progress"));
                }
                State::LoadDropped {
                    reader: source_reader(&[blob(&merge.id)])?,
                    merge,
                    resume: (trigram, source, first),
                    dropped: HashSet::new(),
                }
            }
            Progress::Swap => {
                self.insert(
                    Tree::Segments,
                    key(&[blob(&merge.id), Value::from_i64(i64::from(merge.texts()))])?,
                );
                for (segment, texts) in &merge.sources {
                    self.delete(
                        Tree::Segments,
                        key(&[blob(segment), Value::from_i64(i64::from(*texts))])?,
                    );
                }
                self.delete(Tree::Merges, merge.progress_key(Progress::Swap)?);
                self.insert(Tree::Merges, merge.progress_key(Progress::Cleanup)?);
                State::Write
            }
            Progress::Cleanup => State::Cleanup {
                reader: source_reader(&[blob(&merge.sources[0].0)])?,
                merge,
                part: Cleanup::Postings(0),
            },
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segments(sizes: &[u32]) -> Vec<(Vec<u8>, u32)> {
        sizes
            .iter()
            .enumerate()
            .map(|(i, size)| (vec![i as u8], *size))
            .collect()
    }

    /// Tantivy's policy: eight segments of a level are merged, smallest level first; seven are
    /// not; a segment over the largest size is never merged again.
    #[test]
    fn eight_segments_of_a_level_are_merged() {
        assert_eq!(choose(&segments(&[5; 7])), None);
        let chosen = choose(&segments(&[5, 6, 7, 8, 9, 10, 11, 12])).unwrap();
        assert_eq!(
            chosen.len(),
            8,
            "below 10,000 texts, every segment is of one level"
        );
        // A level of large segments and one of small: the small one first.
        let mut both = vec![100_000u32; 8];
        both.extend([3u32; 8]);
        let chosen = choose(&segments(&both)).unwrap();
        assert!(chosen.iter().all(|(_, size)| *size == 3), "{chosen:?}");
        // Sizes within a factor of 2^0.75 are one level, however they differ.
        let close = [
            100_000, 90_000, 80_000, 70_000, 100_000, 95_000, 85_000, 75_000,
        ];
        assert_eq!(choose(&segments(&close)).map(|c| c.len()), Some(8));
        // Sizes a factor of more than 2^0.75 apart are levels apart.
        assert_eq!(
            choose(&segments(&[
                100_000, 100_000, 100_000, 100_000, 20_000, 20_000, 20_000, 20_000
            ])),
            None
        );
        // No merge makes a segment above the largest.
        assert_eq!(choose(&segments(&[3_000_000; 8])).map(|c| c.len()), Some(3));
        assert_eq!(choose(&segments(&[10_000_001; 8])), None);
    }
}
