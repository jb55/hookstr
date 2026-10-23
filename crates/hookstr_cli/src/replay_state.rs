//! Client-local replay bookkeeping. Deliberately NOT nostr events — acks in
//! the sync set would pollute negentropy reconciliation forever.

use redb::{ReadableTable, TableDefinition};

/// event id (32 bytes) -> (attempts, next_retry_unix_s); attempts ==
/// [`REPLAYED`] means done.
const REPLAYS: TableDefinition<&[u8; 32], (u32, u64)> = TableDefinition::new("replays");

/// Sentinel in the attempts column marking a successful replay.
const REPLAYED: u32 = u32::MAX;

/// Default base of the exponential backoff after a failed replay attempt.
pub const BACKOFF_BASE_SECS: u64 = 30;
/// Backoff ceiling: base * 2^7, about an hour at the default base.
const BACKOFF_MAX_DOUBLINGS: u32 = 7;

/// When an event is next owed a delivery attempt.
pub enum Due {
    /// Never tried, or failed and past its backoff.
    Now,
    /// Failed; retry at this unix second.
    Later(u64),
    /// Replayed.
    Never,
}

pub struct ReplayState {
    db: redb::Database,
    backoff_base_secs: u64,
}

impl ReplayState {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        Self::open_with_backoff(path, BACKOFF_BASE_SECS)
    }

    /// Like [`open`](Self::open), with failures backing off from
    /// `backoff_base_secs` instead of the default 30s.
    pub fn open_with_backoff(path: &str, backoff_base_secs: u64) -> anyhow::Result<Self> {
        let db = redb::Database::create(path)?;
        // Materialize the table so reads before the first write don't hit
        // TableDoesNotExist.
        let write = db.begin_write()?;
        write.open_table(REPLAYS)?;
        write.commit()?;
        Ok(Self {
            db,
            backoff_base_secs,
        })
    }

    fn row(&self, id: &[u8; 32]) -> anyhow::Result<Option<(u32, u64)>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(REPLAYS)?;
        Ok(table.get(id)?.map(|row| row.value()))
    }

    /// Whether this event still needs a replay attempt now: never tried, or
    /// failed and past its backoff.
    pub fn is_due(&self, id: &[u8; 32], now: u64) -> anyhow::Result<bool> {
        Ok(matches!(self.due(id, now)?, Due::Now))
    }

    pub fn due(&self, id: &[u8; 32], now: u64) -> anyhow::Result<Due> {
        Ok(match self.row(id)? {
            None => Due::Now,
            Some((REPLAYED, _)) => Due::Never,
            Some((_, next_retry)) if next_retry <= now => Due::Now,
            Some((_, next_retry)) => Due::Later(next_retry),
        })
    }

    pub fn mark_replayed(&self, id: &[u8; 32]) -> anyhow::Result<()> {
        self.mark_replayed_all(std::slice::from_ref(id))
    }

    /// Mark every event replayed in one transaction (one fsync, not one per
    /// event), so acking a batch is atomic.
    pub fn mark_replayed_all(&self, ids: &[[u8; 32]]) -> anyhow::Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut table = write.open_table(REPLAYS)?;
            for id in ids {
                table.insert(id, (REPLAYED, 0))?;
            }
        }
        write.commit()?;
        Ok(())
    }

    /// Record a failed attempt and schedule the next one with exponential
    /// backoff (30s doubling to a ~1h ceiling at the default base).
    pub fn mark_failed(&self, id: &[u8; 32], now: u64) -> anyhow::Result<()> {
        self.mark_failed_all(std::slice::from_ref(id), now)?;
        Ok(())
    }

    /// [`mark_failed`](Self::mark_failed) for a whole batch in one
    /// transaction. Returns the earliest retry time it scheduled.
    pub fn mark_failed_all(&self, ids: &[[u8; 32]], now: u64) -> anyhow::Result<Option<u64>> {
        let write = self.db.begin_write()?;
        let mut earliest = None::<u64>;
        {
            let mut table = write.open_table(REPLAYS)?;
            for id in ids {
                let attempts = table.get(id)?.map(|row| row.value().0).unwrap_or(0) + 1;
                let backoff = self.backoff_base_secs << attempts.min(BACKOFF_MAX_DOUBLINGS);
                let retry_at = now + backoff;
                table.insert(id, (attempts, retry_at))?;
                earliest = Some(earliest.map_or(retry_at, |at| at.min(retry_at)));
            }
        }
        write.commit()?;
        Ok(earliest)
    }
}
