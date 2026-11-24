use super::*;

#[test]
fn writes_share_timestamp_once_write_pending_is_set() {
    let hlc = HlcService::new_with_uuid(uuid::Uuid::new_v4());
    let ctx = ConnectionContext::new();

    let first = ctx.current_or_new_tx_hlc(&hlc).expect("first hlc");
    ctx.mark_write_pending();
    let second = ctx.current_or_new_tx_hlc(&hlc).expect("second hlc");

    assert_eq!(
        first, second,
        "once write_pending is set, repeated calls must return the cached timestamp"
    );
}

#[test]
fn readonly_calls_do_not_pin_a_stale_slot() {
    // Without a write_pending signal, every call must draw a fresh
    // timestamp — otherwise a bare `SELECT current_hlc()` could dictate
    // the HLC of the next write transaction.
    let hlc = HlcService::new_with_uuid(uuid::Uuid::new_v4());
    let ctx = ConnectionContext::new();

    let a = ctx.current_or_new_tx_hlc(&hlc).expect("first hlc");
    let b = ctx.current_or_new_tx_hlc(&hlc).expect("second hlc");

    assert_ne!(
        a, b,
        "read-only invocations must not reuse a cached slot that no write has claimed"
    );
}

#[test]
fn reset_produces_fresh_timestamp() {
    let hlc = HlcService::new_with_uuid(uuid::Uuid::new_v4());
    let ctx = ConnectionContext::new();

    let first = ctx.current_or_new_tx_hlc(&hlc).expect("first hlc");
    ctx.mark_write_pending();
    let pinned = ctx.current_or_new_tx_hlc(&hlc).expect("pinned hlc");
    assert_eq!(first, pinned, "write_pending must pin the first value");

    ctx.reset_tx_slot();
    let fresh = ctx.current_or_new_tx_hlc(&hlc).expect("post-reset hlc");

    assert_ne!(first, fresh, "after reset a new timestamp must be produced");
}

#[test]
fn context_is_clone_and_shares_state() {
    // The context is passed by clone into commit/rollback/update hooks;
    // both handles must see the same underlying slot.
    let hlc = HlcService::new_with_uuid(uuid::Uuid::new_v4());
    let ctx_a = ConnectionContext::new();
    let ctx_b = ctx_a.clone();

    ctx_a.mark_write_pending();
    let first = ctx_a.current_or_new_tx_hlc(&hlc).expect("first hlc");
    let second = ctx_b
        .current_or_new_tx_hlc(&hlc)
        .expect("second hlc via clone");
    assert_eq!(first, second, "cloned context must share the pinned slot");

    ctx_b.reset_tx_slot();
    let after_reset = ctx_a
        .current_or_new_tx_hlc(&hlc)
        .expect("post-reset hlc via original");
    assert_ne!(
        first, after_reset,
        "reset on either clone must clear the shared slot"
    );
}
