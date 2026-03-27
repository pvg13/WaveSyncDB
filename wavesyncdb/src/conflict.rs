//! Column-level CRDT conflict resolution.
//!
//! Each column has its own Lamport clock (`col_version`). When two nodes edit
//! different columns of the same row concurrently, both changes survive.
//! When they edit the same column, the higher `col_version` wins; ties are
//! broken by comparing the serialized value bytes, then `site_id`.
//!
//! Delete operations use a `__deleted` sentinel column with a `causal_length`
//! that must exceed the maximum `col_version` across all columns for the row.

use crate::messages::{DeletePolicy, NodeId};

/// Determine whether a remote column change should be applied over local state.
///
/// Returns `true` if:
/// - There is no local clock entry for this column (first write), or
/// - The remote `col_version` is strictly greater, or
/// - On `col_version` tie: compare serialized values, then `site_id`.
pub fn should_apply_column(
    remote_col_version: u64,
    remote_val: &[u8],
    remote_site_id: &NodeId,
    local_col_version: u64,
    local_val: &[u8],
    local_site_id: &NodeId,
) -> bool {
    match remote_col_version.cmp(&local_col_version) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => {
            // Tiebreak: compare value bytes, then site_id
            match remote_val.cmp(local_val) {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Less => false,
                std::cmp::Ordering::Equal => remote_site_id.0 > local_site_id.0,
            }
        }
    }
}

/// Determine whether a remote delete should be applied.
///
/// A delete's `causal_length` must be ≥ the local maximum `col_version`
/// for the row. The `DeletePolicy` determines whether ties go to delete
/// or to the existing data.
pub fn should_apply_delete(
    remote_cl: u64,
    local_max_col_version: u64,
    policy: &DeletePolicy,
) -> bool {
    match remote_cl.cmp(&local_max_col_version) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => {
            // Tie: policy decides
            matches!(policy, DeletePolicy::DeleteWins)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SITE_A: NodeId = NodeId([1u8; 16]);
    const SITE_B: NodeId = NodeId([2u8; 16]);

    // ── should_apply_column ──

    #[test]
    fn test_higher_col_version_wins() {
        assert!(should_apply_column(5, b"a", &SITE_A, 3, b"b", &SITE_B));
    }

    #[test]
    fn test_lower_col_version_loses() {
        assert!(!should_apply_column(3, b"a", &SITE_A, 5, b"b", &SITE_B));
    }

    #[test]
    fn test_same_version_higher_value_wins() {
        assert!(should_apply_column(5, b"z", &SITE_A, 5, b"a", &SITE_B));
    }

    #[test]
    fn test_same_version_lower_value_loses() {
        assert!(!should_apply_column(5, b"a", &SITE_A, 5, b"z", &SITE_B));
    }

    #[test]
    fn test_same_version_same_value_higher_site_wins() {
        assert!(should_apply_column(5, b"x", &SITE_B, 5, b"x", &SITE_A));
    }

    #[test]
    fn test_same_version_same_value_lower_site_loses() {
        assert!(!should_apply_column(5, b"x", &SITE_A, 5, b"x", &SITE_B));
    }

    #[test]
    fn test_same_everything_does_not_apply() {
        assert!(!should_apply_column(5, b"x", &SITE_A, 5, b"x", &SITE_A));
    }

    #[test]
    fn test_first_write_no_local() {
        // If local col_version is 0 (no entry), remote with version > 0 always wins
        assert!(should_apply_column(1, b"v", &SITE_A, 0, b"", &NodeId([0u8; 16])));
    }

    #[test]
    fn test_empty_values_tiebreak_to_site_id() {
        assert!(should_apply_column(1, b"", &SITE_B, 1, b"", &SITE_A));
        assert!(!should_apply_column(1, b"", &SITE_A, 1, b"", &SITE_B));
    }

    // ── should_apply_delete ──

    #[test]
    fn test_delete_higher_cl_always_applies() {
        assert!(should_apply_delete(10, 5, &DeletePolicy::DeleteWins));
        assert!(should_apply_delete(10, 5, &DeletePolicy::AddWins));
    }

    #[test]
    fn test_delete_lower_cl_never_applies() {
        assert!(!should_apply_delete(3, 5, &DeletePolicy::DeleteWins));
        assert!(!should_apply_delete(3, 5, &DeletePolicy::AddWins));
    }

    #[test]
    fn test_delete_tie_delete_wins() {
        assert!(should_apply_delete(5, 5, &DeletePolicy::DeleteWins));
    }

    #[test]
    fn test_delete_tie_add_wins() {
        assert!(!should_apply_delete(5, 5, &DeletePolicy::AddWins));
    }

    #[test]
    fn test_delete_zero_cl_zero_local() {
        // Both at 0 — policy decides
        assert!(should_apply_delete(0, 0, &DeletePolicy::DeleteWins));
        assert!(!should_apply_delete(0, 0, &DeletePolicy::AddWins));
    }
}
