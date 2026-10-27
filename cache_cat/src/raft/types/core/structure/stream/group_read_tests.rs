use super::*;
use crate::raft::types::core::structure::stream::ManualClock;

fn id(ms: u64) -> StreamId {
    StreamId::new(ms, 0)
}

fn stream(now: u64, ids: &[u64]) -> RedisStream {
    let mut stream = RedisStream::with_clock(Arc::new(ManualClock::new(now)));
    for &ms in ids {
        stream
            .xadd(
                AddId::Explicit(id(ms)),
                vec![(b"field".to_vec(), ms.to_string().into_bytes())],
            )
            .unwrap();
    }
    stream
        .xgroup_create(b"group", GroupStart::Id(StreamId::ZERO), None)
        .unwrap();
    stream
}

#[test]
fn replicated_group_read_uses_command_clock_on_replicas_with_different_clocks() {
    let mut leader = stream(1, &[1, 2]);
    let mut follower = stream(u64::MAX, &[1, 2]);
    let options = ReadGroupOptions {
        count: Some(1),
        no_ack: false,
    };
    let first = leader
        .xreadgroup_at(b"group", b"consumer", GroupRead::New, options, 1234)
        .unwrap();
    assert_eq!(
        first,
        follower
            .xreadgroup_at(b"group", b"consumer", GroupRead::New, options, 1234)
            .unwrap()
    );
    let expected = leader.snapshot();
    assert_eq!(expected, follower.snapshot());
    assert_eq!(expected.groups[0].consumers[0].seen_ms, 1234);
    assert_eq!(expected.groups[0].consumers[0].active_ms, Some(1234));
    assert_eq!(expected.groups[0].pending[0].delivery_ms, 1234);
    assert_eq!(expected.groups[0].pending[0].deliveries, 1);
    assert_eq!(expected.groups[0].last_delivered, id(1));
}

#[test]
fn empty_group_read_creates_and_touches_consumer_without_delivering() {
    let mut stream = stream(u64::MAX, &[]);
    assert!(
        stream
            .xreadgroup_at(
                b"group",
                b"consumer",
                GroupRead::New,
                ReadGroupOptions::default(),
                10
            )
            .unwrap()
            .is_empty()
    );
    let first = stream.snapshot();
    assert_eq!(first.groups[0].consumers.len(), 1);
    assert_eq!(first.groups[0].consumers[0].seen_ms, 10);
    assert_eq!(first.groups[0].consumers[0].active_ms, None);
    assert!(first.groups[0].pending.is_empty());
    assert!(
        stream
            .xreadgroup_at(
                b"group",
                b"consumer",
                GroupRead::PendingAfter(StreamId::ZERO),
                ReadGroupOptions::default(),
                20
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(stream.snapshot().groups[0].consumers[0].seen_ms, 20);
}

#[test]
fn noack_advances_group_without_adding_pending_or_refreshing_active_time() {
    let mut stream = stream(0, &[1, 2]);
    let entries = stream
        .xreadgroup_at(
            b"group",
            b"consumer",
            GroupRead::New,
            ReadGroupOptions {
                count: None,
                no_ack: true,
            },
            15,
        )
        .unwrap();
    assert_eq!(
        entries.iter().map(|entry| entry.id).collect::<Vec<_>>(),
        vec![id(1), id(2)]
    );
    let snapshot = stream.snapshot();
    assert_eq!(snapshot.groups[0].last_delivered, id(2));
    assert_eq!(snapshot.groups[0].consumers[0].seen_ms, 15);
    assert_eq!(snapshot.groups[0].consumers[0].active_ms, None);
    assert!(snapshot.groups[0].pending.is_empty());
}

#[test]
fn history_reads_ignore_noack_and_only_update_live_delivery_metadata() {
    let mut stream = stream(0, &[1, 2]);
    stream
        .xreadgroup_at(
            b"group",
            b"consumer",
            GroupRead::New,
            ReadGroupOptions::default(),
            10,
        )
        .unwrap();
    stream.xdel(&[id(1)]);
    let entries = stream
        .xreadgroup_at(
            b"group",
            b"consumer",
            GroupRead::PendingAfter(StreamId::ZERO),
            ReadGroupOptions {
                count: None,
                no_ack: true,
            },
            20,
        )
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(
        entries[0],
        GroupEntry {
            id: id(1),
            fields: None
        }
    );
    assert_eq!(entries[1].id, id(2));
    assert!(entries[1].fields.is_some());
    let snapshot = stream.snapshot();
    assert_eq!(snapshot.groups[0].pending[0].deliveries, 1);
    assert_eq!(snapshot.groups[0].pending[0].delivery_ms, 10);
    assert_eq!(snapshot.groups[0].pending[1].deliveries, 2);
    assert_eq!(snapshot.groups[0].pending[1].delivery_ms, 20);
    assert_eq!(snapshot.groups[0].consumers[0].seen_ms, 20);
    assert_eq!(snapshot.groups[0].consumers[0].active_ms, Some(10));
    assert!(
        stream
            .xreadgroup_at(
                b"group",
                b"another",
                GroupRead::PendingAfter(StreamId::ZERO),
                ReadGroupOptions::default(),
                30
            )
            .unwrap()
            .is_empty()
    );
    stream.check_invariants().unwrap();
}

#[test]
fn readiness_is_read_only_and_group_identity_survives_clone_but_not_recreate() {
    let mut stream = stream(0, &[1, 2]);
    let original = stream.snapshot();
    assert!(stream.group_has_new(b"group").unwrap());
    assert_eq!(stream.group_has_new(b"missing"), Err(StreamError::NoGroup));
    assert_eq!(stream.snapshot(), original);
    let identity = stream.group_identity(b"group").unwrap();
    let frozen = stream.clone();
    assert!(Arc::ptr_eq(
        &identity,
        &frozen.group_identity(b"group").unwrap()
    ));
    stream
        .xreadgroup_at(
            b"group",
            b"consumer",
            GroupRead::New,
            ReadGroupOptions {
                count: Some(1),
                no_ack: false,
            },
            10,
        )
        .unwrap();
    assert!(stream.group_has_new(b"group").unwrap());
    stream.xdel(&[id(2)]);
    assert!(!stream.group_has_new(b"group").unwrap());
    assert_eq!(frozen.snapshot(), original);
    assert!(stream.xgroup_destroy(b"group"));
    stream
        .xgroup_create(b"group", GroupStart::Id(StreamId::ZERO), None)
        .unwrap();
    assert!(!Arc::ptr_eq(
        &identity,
        &stream.group_identity(b"group").unwrap()
    ));
}

#[test]
fn replicated_group_generation_survives_borrowed_serde_and_owned_snapshot() {
    let mut stream = stream(0, &[1]);
    assert_eq!(stream.group_generation(b"group").unwrap(), None);
    stream.xgroup_destroy(b"group");
    let generation = GroupGeneration {
        log_index: 42,
        ordinal: 1,
    };
    stream
        .xgroup_create_at_generation(
            b"group",
            GroupStart::Id(StreamId::ZERO),
            None,
            Some(generation),
        )
        .unwrap();
    let snapshot = stream.snapshot();
    assert_eq!(snapshot.groups[0].generation, Some(generation));
    let encoded = bincode2::serialize(&stream).unwrap();
    assert_eq!(encoded, bincode2::serialize(&snapshot).unwrap());
    let restored: RedisStream = bincode2::deserialize(&encoded).unwrap();
    assert_eq!(
        restored.group_generation(b"group").unwrap(),
        Some(generation)
    );
    assert_eq!(
        snapshot
            .clone()
            .into_stream()
            .unwrap()
            .group_generation(b"group")
            .unwrap(),
        Some(generation)
    );
    let frozen = stream.clone();
    stream.xgroup_destroy(b"group");
    let recreated = GroupGeneration {
        log_index: 42,
        ordinal: 2,
    };
    stream
        .xgroup_create_at_generation(
            b"group",
            GroupStart::Id(StreamId::ZERO),
            None,
            Some(recreated),
        )
        .unwrap();
    assert_eq!(stream.group_generation(b"group").unwrap(), Some(recreated));
    assert_eq!(frozen.group_generation(b"group").unwrap(), Some(generation));
    assert_eq!(
        restored.group_generation(b"group").unwrap(),
        Some(generation)
    );
}
