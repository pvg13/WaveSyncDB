//! Last-Write-Wins (LWW) conflict resolution.
//!
//! When a remote operation arrives, we compare it against the latest local operation
//! for the same row. The operation with the higher `(hlc_time, hlc_counter, node_id)`
//! tuple wins. This provides a total ordering over all operations, ensuring all nodes
//! converge to the same state.

use crate::messages::SyncOperation;

/// Determine whether a remote operation should be applied over the local state.
///
/// Returns `true` if:
/// - There is no local operation for this row (first write wins by default), or
/// - The remote operation has a strictly greater `(hlc_time, hlc_counter, node_id)` tuple.
///
/// Returns `false` if the local operation wins or both are identical (no-op).
pub fn should_apply(remote: &SyncOperation, local: Option<&SyncOperation>) -> bool {
    match local {
        None => true,
        Some(local) => {
            (remote.hlc_time, remote.hlc_counter, &remote.node_id)
                > (local.hlc_time, local.hlc_counter, &local.node_id)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::WriteKind;

    fn make_op(hlc_time: u64, hlc_counter: u32, node_id: u8) -> SyncOperation {
        SyncOperation {
            op_id: 0,
            hlc_time,
            hlc_counter,
            node_id: [node_id; 16],
            table: "test".into(),
            kind: WriteKind::Update,
            primary_key: "1".into(),
            data: None,
            columns: None,
        }
    }

    #[test]
    fn test_no_local_always_applies() {
        let remote = make_op(100, 0, 1);
        assert!(should_apply(&remote, None));
    }

    #[test]
    fn test_newer_hlc_wins() {
        let remote = make_op(200, 0, 1);
        let local = make_op(100, 0, 2);
        assert!(should_apply(&remote, Some(&local)));
    }

    #[test]
    fn test_older_hlc_loses() {
        let remote = make_op(100, 0, 1);
        let local = make_op(200, 0, 2);
        assert!(!should_apply(&remote, Some(&local)));
    }

    #[test]
    fn test_same_hlc_higher_counter_wins() {
        let remote = make_op(100, 5, 1);
        let local = make_op(100, 3, 2);
        assert!(should_apply(&remote, Some(&local)));
    }

    #[test]
    fn test_same_hlc_same_counter_higher_node_wins() {
        let remote = make_op(100, 0, 5);
        let local = make_op(100, 0, 3);
        assert!(should_apply(&remote, Some(&local)));
    }

    #[test]
    fn test_identical_ops_do_not_apply() {
        let remote = make_op(100, 0, 1);
        let local = make_op(100, 0, 1);
        assert!(!should_apply(&remote, Some(&local)));
    }
}
