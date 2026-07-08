//! Client-local replay bookkeeping. Deliberately NOT nostr events — acks in
//! the sync set would pollute negentropy reconciliation forever.

use redb::TableDefinition;

/// event id (32 bytes) -> (attempts, next_retry_unix_s); attempts ==
/// [`REPLAYED`] means done.
const REPLAYS: TableDefinition<&[u8; 32], (u32, u64)> = TableDefinition::new("replays");

/// Sentinel in the attempts column marking a successful replay.
const REPLAYED: u32 = u32::MAX;

/// Base of the exponential backoff after a failed replay attempt.
const BACKOFF_BASE_SECS: u64 = 30;
/// Backoff ceiling: 30s * 2^7 ≈ one hour.
const BACKOFF_MAX_DOUBLINGS: u32 = 7;

pub struct ReplayState {
    db: redb::Database,
}

impl ReplayState {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let db = redb::Database::create(path)?;
        // Materialize the table so reads before the first write don't hit
        // TableDoesNotExist.
        let write = db.begin_write()?;
        write.open_table(REPLAYS)?;
        write.commit()?;
        Ok(Self { db })
    }

    fn row(&self, id: &[u8; 32]) -> anyhow::Result<Option<(u32, u64)>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(REPLAYS)?;
        Ok(table.get(id)?.map(|row| row.value()))
    }

    /// Whether this event still needs a replay attempt now: never tried, or
    /// failed and past its backoff.
    pub fn is_due(&self, id: &[u8; 32], now: u64) -> anyhow::Result<bool> {
        Ok(match self.row(id)? {
            None => true,
            Some((REPLAYED, _)) => false,
            Some((_, next_retry)) => next_retry <= now,
        })
    }

    pub fn mark_replayed(&self, id: &[u8; 32]) -> anyhow::Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut table = write.open_table(REPLAYS)?;
            table.insert(id, (REPLAYED, 0))?;
        }
        write.commit()?;
        Ok(())
    }

    /// Record a failed attempt and schedule the next one with exponential
    /// backoff (30s doubling to a ~1h ceiling).
    pub fn mark_failed(&self, id: &[u8; 32], now: u64) -> anyhow::Result<()> {
        let attempts = self.row(id)?.map(|(attempts, _)| attempts).unwrap_or(0) + 1;
        let backoff = BACKOFF_BASE_SECS << attempts.min(BACKOFF_MAX_DOUBLINGS);
        let write = self.db.begin_write()?;
        {
            let mut table = write.open_table(REPLAYS)?;
            table.insert(id, (attempts, now + backoff))?;
        }
        write.commit()?;
        Ok(())
    }
}
