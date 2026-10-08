mod random;
pub mod sadd;
pub mod scard;
pub mod sdiff;
pub mod sdiffstore;
pub mod sinter;
pub mod sinterstore;
pub mod sismember;
pub mod smembers;
pub mod spop;
pub mod srandmember;
pub mod srem;
pub mod sunion;
pub mod sunionstore;

/// Keep replicated reads independent of each node's hash-table iteration order.
fn canonical_set_reply(
    mut reply: crate::raft::types::core::response_value::Value,
) -> crate::raft::types::core::response_value::Value {
    use crate::raft::types::core::response_value::Value;

    if let Value::Set(members) = &mut reply {
        members.sort_unstable_by(|left, right| match (left, right) {
            (Value::BulkString(Some(left)), Value::BulkString(Some(right))) => left.cmp(right),
            _ => unreachable!("set members are bulk strings"),
        });
    }
    reply
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocha::EntrySnapshot;
    use crate::raft::types::core::mocha::core::MyValue;
    use crate::raft::types::core::mocha::read_command::{MultiReadCommand, ReadCommand};
    use crate::raft::types::core::response_value::Value;
    use crate::raft::types::core::value_object::ValueObject;
    use bytes::Bytes;
    use parking_lot::Mutex;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn set_entry(members: &[&[u8]], capacity: usize, reverse: bool) -> EntrySnapshot<MyValue> {
        let mut set = HashSet::with_capacity(capacity);
        for index in 0..members.len() {
            let index = if reverse {
                members.len() - index - 1
            } else {
                index
            };
            set.insert(Bytes::copy_from_slice(members[index]));
        }
        EntrySnapshot {
            value: MyValue::new(ValueObject::Set(Arc::new(Mutex::new(set)))),
            expire_at: None,
        }
    }

    fn set_reply(members: &[&[u8]]) -> Value {
        Value::Set(
            members
                .iter()
                .map(|member| Value::BulkString(Some(Bytes::copy_from_slice(member))))
                .collect(),
        )
    }

    fn multi_commands() -> [Box<dyn MultiReadCommand>; 3] {
        let keys = vec!["first".into(), "second".into()];
        [
            Box::new(sinter::SInterParams { keys: keys.clone() }),
            Box::new(sunion::SUnionParams { keys: keys.clone() }),
            Box::new(sdiff::SDiffParams { keys }),
        ]
    }

    #[test]
    fn clocked_set_reads_are_identical_across_hash_table_layouts() {
        let first: &[&[u8]] = &[b"\xff", b"a", b"a\0", b"\x80", b"\0", b""];
        let second: &[&[u8]] = &[b"a", b"\x80", b"extra"];
        let expected_members = set_reply(&[b"", b"\0", b"a", b"a\0", b"\x80", b"\xff"]);
        let expected_multi = [
            set_reply(&[b"a", b"\x80"]),
            set_reply(&[b"", b"\0", b"a", b"a\0", b"extra", b"\x80", b"\xff"]),
            set_reply(&[b"", b"\0", b"a\0", b"\xff"]),
        ];
        let members = smembers::SMembersParams {
            key: "first".into(),
        };
        let commands = multi_commands();
        for (capacity, reverse) in [(0, false), (32, true), (1024, false)] {
            let first = set_entry(first, capacity, reverse);
            let second = set_entry(second, capacity + 16, !reverse);
            for clock in [0, 123_456] {
                let reply = members.execute_with_clock(Some(first.clone()), clock);
                for proto in [2, 3] {
                    assert_eq!(
                        reply.encode_proto(proto),
                        expected_members.encode_proto(proto)
                    );
                }
                for (command, expected) in commands.iter().zip(&expected_multi) {
                    let reply = command
                        .execute_with_clock(vec![Some(first.clone()), Some(second.clone())], clock);
                    for proto in [2, 3] {
                        assert_eq!(reply.encode_proto(proto), expected.encode_proto(proto));
                    }
                }
            }
        }
    }

    #[test]
    fn clocked_set_reads_preserve_empty_replies_and_wrong_type_errors() {
        let members = smembers::SMembersParams {
            key: "first".into(),
        };
        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::String("value".into())),
            expire_at: None,
        };
        let empty_set = set_entry(&[], 0, false);
        let wrong_type_reply =
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        for proto in [2, 3] {
            for entry in [None, Some(empty_set.clone())] {
                assert_eq!(
                    members.execute_with_clock(entry, 17).encode_proto(proto),
                    set_reply(&[]).encode_proto(proto)
                );
            }
            assert_eq!(
                members
                    .execute_with_clock(Some(wrong_type.clone()), 17)
                    .encode_proto(proto),
                wrong_type_reply
            );
            for command in multi_commands() {
                for values in [vec![None, None], vec![None, Some(empty_set.clone())]] {
                    assert_eq!(
                        command.execute_with_clock(values, 17).encode_proto(proto),
                        set_reply(&[]).encode_proto(proto)
                    );
                }
                for values in [
                    vec![None, Some(wrong_type.clone())],
                    vec![Some(wrong_type.clone()), None],
                ] {
                    assert_eq!(
                        command.execute_with_clock(values, 17).encode_proto(proto),
                        wrong_type_reply
                    );
                }
            }
        }
    }
}
