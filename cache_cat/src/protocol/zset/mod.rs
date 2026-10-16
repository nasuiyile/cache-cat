pub mod zadd;
pub mod zcard;
pub mod zcount;
pub mod zincrby;
pub mod zpopmin;
pub mod zrange;
pub mod zrangegetscore;
pub mod zrank;
pub mod zrem;
pub mod zrevrank;
pub mod zscore;

#[cfg(test)]
mod rank_tests {
    use super::{zrank::ZRankCommand, zrevrank::ZRevRankCommand};
    use crate::error::ProtocolError;
    use crate::mocha::EntrySnapshot;
    use crate::protocol::raft_command::RaftCommand;
    use crate::raft::types::core::mocha::core::MyValue;
    use crate::raft::types::core::mocha::read_command::ReadCommand;
    use crate::raft::types::core::response_value::Value;
    use crate::raft::types::core::structure::sorted_set::SortedSet;
    use crate::raft::types::core::value_object::ValueObject;
    use crate::raft::types::entry::read_operation::ReadOperation;
    use crate::raft::types::entry::request::Operation;
    use bytes::Bytes;
    use parking_lot::Mutex;
    use std::sync::Arc;

    fn args(values: &[&str]) -> Vec<Value> {
        values
            .iter()
            .map(|value| Value::BulkString(Some(Bytes::copy_from_slice(value.as_bytes()))))
            .collect()
    }

    fn parse(command: &dyn RaftCommand, values: &[&str]) -> Box<dyn ReadCommand> {
        match command.raft_request(&args(values)).unwrap() {
            Operation::Read(ReadOperation::ZRank(params)) => Box::new(params),
            Operation::Read(ReadOperation::ZRevRank(params)) => Box::new(params),
            _ => panic!("expected rank command"),
        }
    }

    #[test]
    fn withscore_preserves_rank_and_uses_protocol_specific_scores() {
        let mut set = SortedSet::new();
        set.zincrby("a".into(), 1.5);
        set.zincrby("b".into(), 2.0);
        set.zincrby("c".into(), 3.0);
        let snapshot = EntrySnapshot {
            value: MyValue::new(ValueObject::ZSet(Arc::new(Mutex::new(set)))),
            expire_at: Some(100),
        };
        let commands: [(&dyn RaftCommand, &str, i64); 2] = [
            (&ZRankCommand, "ZRANK", 0),
            (&ZRevRankCommand, "ZREVRANK", 2),
        ];
        for (command, name, rank) in commands {
            let plain = parse(command, &[name, "key", "a"]);
            assert!(matches!(
                plain.execute(Some(snapshot.clone())),
                Value::Integer(actual) if actual == rank
            ));
            let scored = parse(command, &[name, "key", "a", "withscore"]);
            let response = scored.execute(Some(snapshot.clone()));
            assert_eq!(
                response.encode_proto(2),
                format!("*2\r\n:{rank}\r\n$3\r\n1.5\r\n").as_bytes()
            );
            assert_eq!(
                response.encode_proto(3),
                format!("*2\r\n:{rank}\r\n,1.5\r\n").as_bytes()
            );
            for entry in [None, Some(snapshot.clone())] {
                let missing = parse(command, &[name, "key", "absent", "WITHSCORE"]);
                let reply = missing.execute(entry.clone());
                assert_eq!(reply.encode_proto(2), b"*-1\r\n");
                assert_eq!(reply.encode_proto(3), b"_\r\n");
                let missing = parse(command, &[name, "key", "absent"]);
                assert_eq!(missing.execute(entry).encode(), b"$-1\r\n");
            }
            assert_eq!(
                command
                    .raft_request(&args(&[name, "key", "a", "WITHSCORES"]))
                    .unwrap_err(),
                ProtocolError::SyntaxError
            );
        }
    }

    #[test]
    fn withscore_option_follows_redis_nul_termination() {
        let commands: [(&dyn RaftCommand, &str); 2] =
            [(&ZRankCommand, "ZRANK"), (&ZRevRankCommand, "ZREVRANK")];
        for (command, name) in commands {
            for option in ["WITHSCORE\0", "wItHsCoRe\0suffix"] {
                let params = parse(command, &[name, "key", "member", option]);
                assert_eq!(params.execute(None).encode_proto(2), b"*-1\r\n");
            }
            for option in [
                "",
                "WITH",
                "WITH\0SCORE",
                "WITHSCORES\0",
                "WITHSCOREsuffix",
                "\0WITHSCORE",
            ] {
                assert_eq!(
                    command
                        .raft_request(&args(&[name, "key", "member", option]))
                        .unwrap_err(),
                    ProtocolError::SyntaxError
                );
            }
            let mut option = vec![0xff; 1024 * 1024];
            option[..10].copy_from_slice(b"WITHSCORE\0");
            let mut items = args(&[name, "key", "member", ""]);
            items[3] = Value::BulkString(Some(Bytes::from(option)));
            let operation = command.raft_request(&items).unwrap();
            match operation {
                Operation::Read(ReadOperation::ZRank(params)) => assert!(params.with_score),
                Operation::Read(ReadOperation::ZRevRank(params)) => assert!(params.with_score),
                _ => panic!("expected rank command"),
            }
        }
    }
}
