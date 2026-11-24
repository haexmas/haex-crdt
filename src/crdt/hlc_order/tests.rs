use super::*;
use rusqlite::Connection;

const T19: &str = "9999999999999999999";
const T20: &str = "10000000000000000000";
const T_OVER_U64: &str = "99999999999999999999999";

#[test]
fn rust_order_is_numeric_beyond_u64() {
    assert_eq!(
        compare(&format!("{T19}/1"), &format!("{T20}/1")),
        Ordering::Less
    );
    assert_eq!(
        compare(&format!("{T20}/1"), &format!("{T_OVER_U64}/1")),
        Ordering::Less
    );
    assert_eq!(
        compare(&format!("{T_OVER_U64}/1"), &format!("{T_OVER_U64}/2")),
        Ordering::Less,
        "equal time falls through to the node"
    );
}

#[test]
fn node_order_ignores_leading_zeros_and_case_at_any_width() {
    assert_eq!(compare("5/01", "5/1"), Ordering::Equal);
    assert_eq!(compare("5/AB", "5/ab"), Ordering::Equal);
    assert_eq!(compare("5/02", "5/10"), Ordering::Less);
    let wide = "1".to_string() + &"0".repeat(40);
    assert_eq!(compare(&format!("5/{wide}"), "5/ffff"), Ordering::Greater);
}

#[test]
fn malformed_parts_count_as_zero() {
    assert_eq!(compare("", "1/1"), Ordering::Less);
    assert_eq!(compare("abc/1", "1/1"), Ordering::Less);
    assert_eq!(compare("1/zz", "1/1"), Ordering::Less);
    assert_eq!(compare("0/01", "1/1"), Ordering::Less);
    assert_eq!(compare("", "0/0"), Ordering::Equal);
}

fn sql_bool(conn: &Connection, expr: &str, hlc: &str, param: &str) -> bool {
    conn.query_row(
        &format!("SELECT {expr} FROM (SELECT ?1 AS h)"),
        [hlc, param],
        |r| r.get(0),
    )
    .unwrap()
}

#[test]
fn sql_time_order_matches_rust_for_every_pair() {
    let conn = Connection::open_in_memory().unwrap();
    let values = [
        "", "abc", "0/0", "0/01", "1/1", "99/1", "100/1", "7/ab", "7", "007/1", T19, T20,
        T_OVER_U64,
    ];
    let lt = sql_time_lt("h", "?2");
    let ge = sql_time_ge("h", "?2");
    for a in values {
        for b in values {
            let (ta, tb) = (time_key(a), time_key(b));
            let expected = cmp_keys(ta, tb);
            assert_eq!(
                sql_bool(&conn, &lt, a, tb),
                expected == Ordering::Less,
                "lt {a:?} vs {b:?}"
            );
            assert_eq!(
                sql_bool(&conn, &ge, a, tb),
                expected != Ordering::Less,
                "ge {a:?} vs {b:?}"
            );
        }
    }
}

#[test]
fn sql_order_desc_puts_the_widest_time_first() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t (h TEXT)").unwrap();
    for h in ["5/1", T19, T_OVER_U64, T20, "100/1"] {
        conn.execute("INSERT INTO t VALUES (?1)", [h]).unwrap();
    }
    let first: String = conn
        .query_row(
            &format!(
                "SELECT h FROM t ORDER BY {} LIMIT 1",
                sql_time_order_desc("h")
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(first, T_OVER_U64);
}

#[test]
fn u64_time_key_has_no_leading_zeros_and_zero_is_empty() {
    assert_eq!(u64_time_key(0), "");
    assert_eq!(u64_time_key(u64::MAX), "18446744073709551615");
}
