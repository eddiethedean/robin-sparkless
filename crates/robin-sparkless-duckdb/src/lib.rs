//! Optional, read-only DuckDB catalog integration for Polars.
//!
//! Sources can be standalone local files or DuckDB owners served over Quack. The catalog never
//! accepts a path, URI, extension, or credential from a table identifier: the server supplies a
//! trusted alias-to-source map. A statement uses one DuckDB read transaction and exports typed
//! Arrow IPC batches before Polars evaluates its relational plan.

/// Maximum number of source rows admitted by one DuckDB-backed Connect operation.
pub const DUCKDB_SOURCE_MAX_ROWS: u64 = 1_000_000;
/// Maximum combined Arrow IPC input bytes admitted by one source operation.
pub const DUCKDB_SOURCE_MAX_IPC_BYTES: usize = 32 * 1024 * 1024;
/// Maximum decoded source data retained across all tables in one statement.
pub const DUCKDB_SOURCE_MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;
/// Maximum size of one independently readable Arrow IPC source batch.
pub const DUCKDB_SOURCE_MAX_BATCH_BYTES: usize = 1024 * 1024;
/// Maximum rows accepted in one DuckDB-produced source batch.
pub const DUCKDB_SOURCE_MAX_BATCH_ROWS: usize = 16_384;
/// Maximum simultaneously open DuckDB source connections per configured catalog.
pub const DUCKDB_SOURCE_MAX_CONNECTIONS: usize = 4;
/// Maximum source operations queued while waiting for a DuckDB connection.
pub const DUCKDB_SOURCE_MAX_QUEUED_CONNECTIONS: usize = 16;

mod catalog;
mod config;
mod connection;
mod connection_pool;
mod scan;
mod snapshot;

pub use catalog::DuckDbCatalog;
pub use config::{DuckDbQuackSourceConfig, DuckDbSourceConfig, DuckDbSourceSpec};

#[cfg(test)]
mod source_pool_tests {
    use super::*;
    use crate::connection_pool::SourceConnectionPool;
    use crate::scan::duckdb_type_to_spark;
    use crate::snapshot::DuckDbReadSnapshot;
    use robin_sparkless_core::CatalogReadSnapshot;
    use robin_sparkless_core::DataType;
    use robin_sparkless_core::engine::CancellationToken;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn decimal_type_mapping_preserves_supported_precision_and_scale_boundaries() {
        for (type_name, precision, scale) in [
            ("DECIMAL(1, 0)", 1, 0),
            ("DECIMAL(9, 0)", 9, 0),
            ("DECIMAL(18, 6)", 18, 6),
            ("DECIMAL(38, 0)", 38, 0),
            ("DECIMAL(38, 38)", 38, 38),
        ] {
            assert!(matches!(
                duckdb_type_to_spark(type_name),
                Ok(DataType::Decimal(actual_precision, actual_scale))
                    if actual_precision == precision && actual_scale == scale
            ));
        }

        for type_name in ["DECIMAL(0, 0)", "DECIMAL(39, 0)", "DECIMAL(4, 5)"] {
            assert!(duckdb_type_to_spark(type_name).is_err(), "{type_name}");
        }
    }

    #[test]
    fn source_snapshot_charges_decoded_bytes_across_all_batches() {
        let snapshot = DuckDbReadSnapshot::new(
            HashMap::new(),
            Arc::new(SourceConnectionPool::default()),
            CancellationToken::new(),
        );
        assert!(
            snapshot
                .charge_decoded_bytes(DUCKDB_SOURCE_MAX_DECODED_BYTES - 1)
                .is_ok()
        );
        assert!(snapshot.charge_decoded_bytes(1).is_ok());
        assert!(matches!(
            snapshot.charge_decoded_bytes(1),
            Err(error) if error.user_message().contains("decoded data exceeds")
        ));
        assert_eq!(
            snapshot.decoded_bytes.load(Ordering::Acquire),
            DUCKDB_SOURCE_MAX_DECODED_BYTES
        );
        assert!(snapshot.failed.load(Ordering::Acquire));
    }

    fn wait_for_waiters(pool: &SourceConnectionPool, expected: usize) {
        for _ in 0..10_000 {
            if pool.state.lock().unwrap().waiting == expected {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("source pool did not reach {expected} waiters");
    }

    #[test]
    fn source_pool_enforces_connection_and_queue_caps() {
        let pool = Arc::new(SourceConnectionPool::default());
        let token = CancellationToken::new();
        let active = (0..DUCKDB_SOURCE_MAX_CONNECTIONS)
            .map(|_| pool.acquire(&token, 1).unwrap())
            .collect::<Vec<_>>();
        let waiters = (0..DUCKDB_SOURCE_MAX_QUEUED_CONNECTIONS)
            .map(|_| {
                let pool = Arc::clone(&pool);
                let token = token.clone();
                thread::spawn(move || {
                    let _permit = pool.acquire(&token, 1).unwrap();
                })
            })
            .collect::<Vec<_>>();
        wait_for_waiters(&pool, DUCKDB_SOURCE_MAX_QUEUED_CONNECTIONS);

        assert!(matches!(
            pool.acquire(&token, 1),
            Err(error) if error.user_message().contains("queue is full")
        ));
        drop(active);
        for waiter in waiters {
            waiter.join().unwrap();
        }
        assert_eq!(pool.state.lock().unwrap().active, 0);
    }

    #[test]
    fn source_pool_cancellation_removes_a_waiter() {
        let pool = Arc::new(SourceConnectionPool::default());
        let token = CancellationToken::new();
        let active = (0..DUCKDB_SOURCE_MAX_CONNECTIONS)
            .map(|_| pool.acquire(&token, 1).unwrap())
            .collect::<Vec<_>>();
        let waiter_token = token.clone();
        let waiter_pool = Arc::clone(&pool);
        let waiter = thread::spawn(move || waiter_pool.acquire(&waiter_token, 1));
        wait_for_waiters(&pool, 1);
        token.cancel();
        assert!(matches!(
            waiter.join().unwrap(),
            Err(error) if error.category() == robin_sparkless_core::ErrorCategory::Cancelled
        ));
        assert_eq!(pool.state.lock().unwrap().waiting, 0);
        drop(active);
    }

    #[test]
    fn source_pool_reserves_quack_data_and_cancel_connections_together() {
        let pool = Arc::new(SourceConnectionPool::default());
        let token = CancellationToken::new();
        let first = pool.acquire(&token, 2).unwrap();
        let second = pool.acquire(&token, 2).unwrap();
        assert_eq!(pool.state.lock().unwrap().active, 4);

        let waiter_pool = Arc::clone(&pool);
        let waiter_token = token.clone();
        let waiter = thread::spawn(move || waiter_pool.acquire(&waiter_token, 2));
        wait_for_waiters(&pool, 1);
        drop(first);
        let third = waiter.join().unwrap().unwrap();
        assert_eq!(pool.state.lock().unwrap().active, 4);

        drop((second, third));
        assert_eq!(pool.state.lock().unwrap().active, 0);
    }

    #[test]
    fn source_pool_wakes_waiters_whose_weight_fits_released_capacity() {
        use std::sync::mpsc;

        let pool = Arc::new(SourceConnectionPool::default());
        let token = CancellationToken::new();
        let pair = pool.acquire(&token, 2).unwrap();
        let first_single = pool.acquire(&token, 1).unwrap();
        let second_single = pool.acquire(&token, 1).unwrap();

        let (large_tx, large_rx) = mpsc::channel();
        let large_pool = Arc::clone(&pool);
        let large_token = token.clone();
        let large = thread::spawn(move || {
            let permit = large_pool.acquire(&large_token, 2).unwrap();
            large_tx.send(()).unwrap();
            permit
        });
        wait_for_waiters(&pool, 1);

        let (small_tx, small_rx) = mpsc::channel();
        let small_pool = Arc::clone(&pool);
        let small_token = token.clone();
        let small = thread::spawn(move || {
            let permit = small_pool.acquire(&small_token, 1).unwrap();
            small_tx.send(()).unwrap();
            permit
        });
        wait_for_waiters(&pool, 2);

        drop(first_single);
        small_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the one-slot waiter fits the released capacity");
        let small_permit = small.join().unwrap();
        drop((pair, second_single, small_permit));
        large_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the two-slot waiter fits after the larger reservation is released");
        let large_permit = large.join().unwrap();
        drop(large_permit);
        assert_eq!(pool.state.lock().unwrap().active, 0);
    }
}
