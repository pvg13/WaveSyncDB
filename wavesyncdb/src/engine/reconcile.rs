//! Convergence verification + range reconciliation via shadow digests (#82).
//!
//! Two peers hold identical group data iff every shadow-table cell agrees on
//! both its CRDT version *and* its value. We summarise a cell as a BLAKE3 hash
//! over `(table, pk, cid, col_version, site_id, canonical value bytes)` and
//! combine cells with XOR — order-independent, so scan order is irrelevant and
//! two peers fold to the same value iff their data matches.
//!
//! - **Convergence proof (foundation):** XOR *all* cells into one 32-byte
//!   digest ([`compute_group_digest`]). Equal digests ⇒ proven identical.
//! - **Range reconciliation (this step):** partition cells into
//!   [`BUCKET_COUNT`] buckets by `BLAKE3(table, pk, cid)` and XOR each cell into
//!   its bucket ([`compute_group_buckets`]). Exchanging per-bucket digests lets
//!   peers transfer only the cells in *mismatching* buckets
//!   ([`changes_in_buckets`]) instead of the whole range — so after a long
//!   offline period / partition merge the relay carries ~the diff, not
//!   everything since the last shared cursor.
//!
//! Value bytes come from [`sync_handler::get_local_value_bytes`] — the same
//! serialization conflict resolution uses for its tiebreak — so converged peers
//! produce byte-identical input (no false divergence from value encoding). The
//! primary-key column's clock cell is excluded everywhere (the writer records
//! one but the receiver rejects PK-column changes, so it's asymmetric; the PK
//! value is already implicit in every cell's `pk`).

use super::*;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use std::collections::HashSet;

/// Shadow `cid` sentinel for a row tombstone (deleted row). Tombstone cells
/// have no user-table value, so their value bytes are empty.
const TOMBSTONE_CID: &str = "__deleted";

/// Number of reconciliation buckets. A mismatching bucket transfers all of its
/// cells, so larger = closer to "only the diff" but more digest bytes per
/// exchange (BUCKET_COUNT × 32). 64 keeps the digest payload ~2 KiB while, at
/// the typical scale (≪ thousands of cells), putting most cells in distinct
/// buckets so a small diff transfers ~the diff.
pub(super) const BUCKET_COUNT: usize = 64;

/// Which reconciliation bucket a cell belongs to. Identity only (table, pk,
/// cid) — never the version/value — so the *same* cell always lands in the same
/// bucket on both peers regardless of who has the newer value.
pub(super) fn bucket_index(table: &str, pk: &str, cid: &str, n: usize) -> usize {
    let mut h = blake3::Hasher::new();
    h.update(table.as_bytes());
    h.update(&[0]);
    h.update(pk.as_bytes());
    h.update(&[0]);
    h.update(cid.as_bytes());
    let d = h.finalize();
    let v = u64::from_le_bytes(d.as_bytes()[..8].try_into().unwrap());
    (v % n as u64) as usize
}

/// Compute per-bucket value-inclusive digests for one group's database. The XOR
/// of all returned buckets equals [`compute_group_digest`]'s output.
pub(super) async fn compute_group_buckets(
    db: &DatabaseConnection,
    registry: &TableRegistry,
    n: usize,
) -> Vec<[u8; 32]> {
    let mut buckets = vec![[0u8; 32]; n];
    for meta in registry.all_tables() {
        let table = &meta.table_name;
        let shadow = format!("_wavesync_{}_clock", table.replace('"', "\"\""));
        let rows = match db
            .query_all_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                format!("SELECT pk, cid, col_version, site_id FROM \"{shadow}\""),
            ))
            .await
        {
            Ok(rows) => rows,
            // Shadow table absent (no writes yet) — nothing to fold in.
            Err(_) => continue,
        };
        for row in rows {
            let pk: String = row.try_get("", "pk").unwrap_or_default();
            let cid: String = row.try_get("", "cid").unwrap_or_default();
            let cv: i64 = row.try_get("", "col_version").unwrap_or(0);
            let site: Vec<u8> = row.try_get("", "site_id").unwrap_or_default();

            // See module docs: the PK column's clock cell is asymmetric across
            // writer/receiver, so it's excluded from every digest.
            if cid == meta.primary_key_column {
                continue;
            }
            let value_bytes = if cid == TOMBSTONE_CID {
                Vec::new()
            } else {
                sync_handler::get_local_value_bytes(db, table, &meta.primary_key_column, &pk, &cid)
                    .await
            };

            let mut h = blake3::Hasher::new();
            h.update(table.as_bytes());
            h.update(&[0]);
            h.update(pk.as_bytes());
            h.update(&[0]);
            h.update(cid.as_bytes());
            h.update(&[0]);
            h.update(&cv.to_le_bytes());
            h.update(&site);
            h.update(&value_bytes);
            let cell = h.finalize();

            let idx = bucket_index(table, &pk, &cid, n);
            for (a, b) in buckets[idx].iter_mut().zip(cell.as_bytes()) {
                *a ^= b;
            }
        }
    }
    buckets
}

/// Compute the single value-inclusive convergence digest for one group (the
/// `n = 1` case of [`compute_group_buckets`]). Equal across two peers iff their
/// data is byte-identical.
pub(super) async fn compute_group_digest(
    db: &DatabaseConnection,
    registry: &TableRegistry,
) -> [u8; 32] {
    compute_group_buckets(db, registry, 1).await[0]
}

/// Gather this peer's `ColumnChange`s whose cell falls in one of `mismatch`'s
/// buckets — the data the requester needs from us to reconcile those buckets.
/// Reuses `get_changes_since(0)` (all changes, with values) and filters by
/// bucket; the PK column is excluded (the receiver rejects PK-column changes).
pub(super) async fn changes_in_buckets(
    db: &DatabaseConnection,
    registry: &TableRegistry,
    n: usize,
    mismatch: &HashSet<usize>,
) -> Vec<crate::messages::ColumnChange> {
    let all = shadow::get_changes_since(db, registry, 0)
        .await
        .unwrap_or_default();
    all.into_iter()
        .filter(|c| {
            let pk_is_id = registry
                .get(&c.table.0)
                .is_some_and(|m| m.primary_key_column == c.cid.0);
            !pk_is_id && mismatch.contains(&bucket_index(&c.table.0, &c.pk.0, &c.cid.0, n))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WaveSyncDbBuilder;
    use sea_orm::{ActiveModelTrait, Set};
    use std::collections::HashSet;

    mod titem {
        use sea_orm::entity::prelude::*;
        #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
        #[sea_orm(table_name = "titems")]
        pub struct Model {
            #[sea_orm(primary_key, auto_increment = false)]
            pub id: String,
            pub val: String,
        }
        #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
        pub enum Relation {}
        impl ActiveModelBehavior for ActiveModel {}
    }

    fn tmp_db() -> String {
        let unique = uuid::Uuid::new_v4().simple().to_string();
        let path = std::env::temp_dir().join(format!("wavesync_reconcile_{unique}.db"));
        format!("sqlite:{}?mode=rwc", path.display())
    }

    #[test]
    fn bucket_index_is_deterministic_and_in_range() {
        let n = BUCKET_COUNT;
        let a = bucket_index("tasks", "pk1", "title", n);
        let b = bucket_index("tasks", "pk1", "title", n);
        assert_eq!(a, b, "same cell must map to the same bucket");
        assert!(a < n);
        // Identity drives the bucket, not version/value — different cids spread.
        let mut seen = HashSet::new();
        for cid in ["title", "completed", "notes", "due", "owner", "tags"] {
            seen.insert(bucket_index("tasks", "pk1", cid, n));
        }
        assert!(seen.len() > 1, "distinct cells should not all collide");
    }

    #[tokio::test]
    async fn buckets_xor_to_global_digest_and_changes_in_buckets_match() {
        let db = WaveSyncDbBuilder::new(&tmp_db(), "reconcile-unit")
            .build()
            .await
            .unwrap();
        db.schema().register(titem::Entity).sync().await.unwrap();

        for i in 0..8 {
            titem::ActiveModel {
                id: Set(format!("r{i}")),
                val: Set(format!("v{i}")),
            }
            .insert(&db)
            .await
            .unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let n = BUCKET_COUNT;
        let buckets = compute_group_buckets(db.inner(), db.registry(), n).await;
        assert_eq!(buckets.len(), n);

        // The XOR of all buckets must equal the single global digest.
        let mut xor = [0u8; 32];
        for b in &buckets {
            for (a, x) in xor.iter_mut().zip(b) {
                *a ^= x;
            }
        }
        let global = compute_group_digest(db.inner(), db.registry()).await;
        assert_eq!(
            xor, global,
            "XOR of bucket digests must equal the global digest"
        );
        assert_ne!(
            global, [0u8; 32],
            "non-empty group must have a non-zero digest"
        );

        // changes_in_buckets over every non-empty bucket returns exactly the
        // synced (non-PK) cells, and never the PK column.
        let nonempty: HashSet<usize> = (0..n).filter(|&i| buckets[i] != [0u8; 32]).collect();
        let changes = changes_in_buckets(db.inner(), db.registry(), n, &nonempty).await;
        assert!(!changes.is_empty());
        assert!(
            changes.iter().all(|c| c.cid.0 != "id"),
            "PK column must be excluded from reconciliation changes"
        );
        // Each returned change's bucket is in the requested set.
        assert!(
            changes
                .iter()
                .all(|c| nonempty.contains(&bucket_index(&c.table.0, &c.pk.0, &c.cid.0, n))),
            "every returned change must fall in a requested bucket"
        );
    }
}
