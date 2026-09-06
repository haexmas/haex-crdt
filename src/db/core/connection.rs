//! Locks the connection wrapped by [`crate::db::DbConnection`] and hands the
//! caller a `&mut Connection`. A poisoned mutex or an unmounted slot are
//! surfaced as typed errors rather than panics so consumers can decide how
//! to recover.

use crate::db::error::DatabaseError;
use crate::db::DbConnection;
use rusqlite::Connection;

pub fn with_connection<T, F>(connection: &DbConnection, f: F) -> Result<T, DatabaseError>
where
    F: FnOnce(&mut Connection) -> Result<T, DatabaseError>,
{
    let mut db_lock = connection
        .0
        .lock()
        .map_err(|e| DatabaseError::MutexPoisoned {
            reason: e.to_string(),
        })?;

    let conn = db_lock.as_mut().ok_or(DatabaseError::ConnectionError {
        reason: "Connection to vault failed".to_string(),
    })?;

    f(conn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbConnection;
    use rusqlite::Connection;

    #[test]
    fn hands_mutable_connection_to_closure() {
        let db = DbConnection::new(Connection::open_in_memory().unwrap());
        let out = with_connection(&db, |c| {
            c.execute("CREATE TABLE t (id INTEGER)", []).unwrap();
            let one: i64 = c
                .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
                .unwrap();
            Ok(one)
        })
        .unwrap();
        assert_eq!(out, 0);
    }

    #[test]
    fn empty_slot_surfaces_connection_error() {
        let db = DbConnection::empty();
        let err = with_connection::<(), _>(&db, |_| Ok(())).unwrap_err();
        assert!(matches!(err, DatabaseError::ConnectionError { .. }));
    }

    #[test]
    fn closure_error_is_propagated_verbatim() {
        let db = DbConnection::new(Connection::open_in_memory().unwrap());
        let err = with_connection::<(), _>(&db, |_| {
            Err(DatabaseError::StatementError {
                reason: "sentinel".to_string(),
            })
        })
        .unwrap_err();
        match err {
            DatabaseError::StatementError { reason } => assert_eq!(reason, "sentinel"),
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
