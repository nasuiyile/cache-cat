use crate::protocol::common::resp::{RespParseError, take_request_from_bytes_stream};
use crate::raft::types::core::response_value::Value;
use bytes::BytesMut;

fn assert_arguments(value: Value, expected: &[&[u8]]) {
    let Value::Array(Some(arguments)) = value else {
        panic!("request did not decode to an array: {value:?}");
    };
    assert_eq!(arguments.len(), expected.len());
    for (argument, expected) in arguments.into_iter().zip(expected) {
        let Value::BulkString(Some(bytes)) = argument else {
            panic!("request argument was not a bulk string: {argument:?}");
        };
        assert_eq!(bytes.as_ref(), *expected);
    }
}

#[test]
fn requests_decode_flat_bulk_string_arguments() {
    for (wire, arguments) in [
        (b"*1\r\n$4\r\nPING\r\n".as_slice(), vec![b"PING".as_slice()]),
        (
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n".as_slice(),
            vec![b"SET".as_slice(), b"key".as_slice(), b"".as_slice()],
        ),
    ] {
        let mut buffer = BytesMut::from(wire);
        assert_arguments(
            take_request_from_bytes_stream(&mut buffer)
                .unwrap()
                .unwrap(),
            &arguments,
        );
        assert!(buffer.is_empty());
    }
}

#[test]
fn requests_preserve_incomplete_frames_at_every_split_boundary() {
    let wire = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$12\r\nhello\r\nworld\r\n";
    for split in 0..wire.len() {
        let mut buffer = BytesMut::from(&wire[..split]);
        assert!(
            take_request_from_bytes_stream(&mut buffer)
                .unwrap()
                .is_none()
        );
        assert_eq!(buffer.as_ref(), &wire[..split], "split at byte {split}");
        buffer.extend_from_slice(&wire[split..]);
        assert_arguments(
            take_request_from_bytes_stream(&mut buffer)
                .unwrap()
                .unwrap(),
            &[b"SET", b"key", b"hello\r\nworld"],
        );
        assert!(buffer.is_empty());
    }
}

#[test]
fn requests_accept_one_byte_at_a_time() {
    let wire = b"*2\r\n$4\r\nECHO\r\n$0\r\n\r\n";
    let mut buffer = BytesMut::new();
    for (index, byte) in wire.iter().enumerate() {
        buffer.extend_from_slice(&[*byte]);
        let result = take_request_from_bytes_stream(&mut buffer).unwrap();
        if index + 1 == wire.len() {
            assert_arguments(result.unwrap(), &[b"ECHO", b""]);
            assert!(buffer.is_empty());
        } else {
            assert!(result.is_none(), "frame completed at byte {index}");
            assert_eq!(buffer.as_ref(), &wire[..=index]);
        }
    }
}

#[test]
fn requests_leave_pipelined_frames_and_partial_suffixes_in_order() {
    let first = b"*1\r\n$4\r\nPING\r\n";
    let second = b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
    let partial = b"*2\r\n$3\r\nGET\r\n$3\r\nke";
    let mut buffer = BytesMut::new();
    buffer.extend_from_slice(first);
    buffer.extend_from_slice(second);
    buffer.extend_from_slice(partial);

    assert_arguments(
        take_request_from_bytes_stream(&mut buffer)
            .unwrap()
            .unwrap(),
        &[b"PING"],
    );
    assert_eq!(&buffer[..second.len()], second);
    assert_arguments(
        take_request_from_bytes_stream(&mut buffer)
            .unwrap()
            .unwrap(),
        &[b"ECHO", b"hello"],
    );
    assert_eq!(buffer.as_ref(), partial);
    assert!(
        take_request_from_bytes_stream(&mut buffer)
            .unwrap()
            .is_none()
    );
    assert_eq!(buffer.as_ref(), partial);
    buffer.extend_from_slice(b"y\r\n");
    assert_arguments(
        take_request_from_bytes_stream(&mut buffer)
            .unwrap()
            .unwrap(),
        &[b"GET", b"key"],
    );
    assert!(buffer.is_empty());
}

#[test]
fn requests_skip_empty_arrays_without_stalling_the_pipeline() {
    let empty = b"*0\r\n*-1\r\n*-2\r\n";
    let mut buffer = BytesMut::from(empty.as_slice());
    assert!(
        take_request_from_bytes_stream(&mut buffer)
            .unwrap()
            .is_none()
    );
    assert!(buffer.is_empty());

    buffer.extend_from_slice(empty);
    buffer.extend_from_slice(b"*1\r\n$4\r\nPING\r\n*0\r\n");
    assert_arguments(
        take_request_from_bytes_stream(&mut buffer)
            .unwrap()
            .unwrap(),
        &[b"PING"],
    );
    assert_eq!(buffer.as_ref(), b"*0\r\n");
    assert!(
        take_request_from_bytes_stream(&mut buffer)
            .unwrap()
            .is_none()
    );
    assert!(buffer.is_empty());
}

#[test]
fn requests_reject_non_bulk_arguments_as_soon_as_the_marker_arrives() {
    for marker in b"*%~>|+-:_#,(!=" {
        let mut buffer = BytesMut::from(b"*1\r\n".as_slice());
        buffer.extend_from_slice(&[*marker]);
        let original = buffer.clone();
        assert!(matches!(
            take_request_from_bytes_stream(&mut buffer),
            Err(RespParseError::ExpectedBulk(actual)) if actual == *marker
        ));
        assert_eq!(buffer, original);
    }
}

#[test]
fn requests_reject_non_array_top_level_markers_immediately() {
    for marker in b"$%~>|+-:_#,(!=X" {
        let mut buffer = BytesMut::from(&[*marker][..]);
        assert!(matches!(
            take_request_from_bytes_stream(&mut buffer),
            Err(RespParseError::ExpectedArray(actual)) if actual == *marker
        ));
        assert_eq!(buffer.as_ref(), &[*marker]);
    }
}

#[test]
fn requests_reject_deep_aggregates_without_recursing() {
    // The array form is the original approximately 80 KB stack-overflow probe.
    // Include RESP3 attributes with no metadata: they can nest without children.
    for header in [
        b"*1\r\n".as_slice(),
        b"%1\r\n+key\r\n".as_slice(),
        b"~1\r\n".as_slice(),
        b">1\r\n".as_slice(),
        b"|0\r\n".as_slice(),
        b"|1\r\n+key\r\n:1\r\n".as_slice(),
    ] {
        for complete in [false, true] {
            let mut buffer = BytesMut::from(b"*1\r\n".as_slice());
            for _ in 0..20_000 {
                buffer.extend_from_slice(header);
            }
            if complete {
                buffer.extend_from_slice(b"$4\r\nPING\r\n");
            }
            let original = buffer.clone();
            assert!(matches!(
                take_request_from_bytes_stream(&mut buffer),
                Err(RespParseError::ExpectedBulk(actual)) if actual == header[0]
            ));
            assert_eq!(buffer, original);
        }
    }
}

#[test]
fn requests_do_not_consume_arguments_before_a_protocol_error() {
    let wire = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n*1\r\n$5\r\nvalue\r\n";
    let mut buffer = BytesMut::from(wire.as_slice());
    assert!(matches!(
        take_request_from_bytes_stream(&mut buffer),
        Err(RespParseError::ExpectedBulk(b'*'))
    ));
    assert_eq!(buffer.as_ref(), wire);

    let mut buffer = BytesMut::from(b"*1\r\n$4\r\nPING\r\n".as_slice());
    buffer.extend_from_slice(wire);
    assert_arguments(
        take_request_from_bytes_stream(&mut buffer)
            .unwrap()
            .unwrap(),
        &[b"PING"],
    );
    assert!(matches!(
        take_request_from_bytes_stream(&mut buffer),
        Err(RespParseError::ExpectedBulk(b'*'))
    ));
    assert_eq!(buffer.as_ref(), wire);
}

#[test]
fn requests_reject_null_and_other_negative_bulk_lengths() {
    for wire in [b"*1\r\n$-1\r\n".as_slice(), b"*1\r\n$-2\r\n".as_slice()] {
        let mut buffer = BytesMut::from(wire);
        assert!(matches!(
            take_request_from_bytes_stream(&mut buffer),
            Err(RespParseError::InvalidBulkLength)
        ));
        assert_eq!(buffer.as_ref(), wire);
    }
}

#[test]
fn requests_reject_invalid_and_oversized_lengths_before_reading_payloads() {
    for line in [
        "",
        "+1",
        "01",
        "-0",
        "x",
        "2147483648",
        "9223372036854775808",
    ] {
        let wire = format!("*{line}\r\n");
        let mut buffer = BytesMut::from(wire.as_bytes());
        assert!(matches!(
            take_request_from_bytes_stream(&mut buffer),
            Err(RespParseError::InvalidArrayLength)
        ));
        assert_eq!(buffer.as_ref(), wire.as_bytes());
    }
    for line in [
        "",
        "+1",
        "01",
        "-0",
        "x",
        "536870913",
        "9223372036854775808",
    ] {
        let wire = format!("*1\r\n${line}\r\n");
        let mut buffer = BytesMut::from(wire.as_bytes());
        assert!(matches!(
            take_request_from_bytes_stream(&mut buffer),
            Err(RespParseError::InvalidBulkLength)
        ));
        assert_eq!(buffer.as_ref(), wire.as_bytes());
    }
    for wire in [
        b"*2147483647\r\n".as_slice(),
        b"*1\r\n$536870912\r\n".as_slice(),
    ] {
        let mut buffer = BytesMut::from(wire);
        assert!(
            take_request_from_bytes_stream(&mut buffer)
                .unwrap()
                .is_none()
        );
        assert_eq!(buffer.as_ref(), wire);
    }
}

#[test]
fn requests_require_crlf_after_bulk_payloads() {
    for terminator in [b"!!", b"\r!", b"!\n", b"\n\r"] {
        let mut buffer = BytesMut::from(b"*1\r\n$1\r\nx".as_slice());
        buffer.extend_from_slice(terminator);
        let original = buffer.clone();
        assert!(matches!(
            take_request_from_bytes_stream(&mut buffer),
            Err(RespParseError::InvalidBulkTerminator)
        ));
        assert_eq!(buffer, original);
    }
}

#[test]
fn requests_treat_nested_frame_markers_inside_bulk_data_as_binary() {
    let mut payload = b"\0\xff\r\n".to_vec();
    for _ in 0..20_000 {
        payload.extend_from_slice(b"*1\r\n");
    }
    payload.extend_from_slice(b"$4\r\nPING\r\n|0\r\n%1\r\n~1\r\n>1\r\n");
    let mut buffer = BytesMut::from(b"*2\r\n$4\r\nECHO\r\n".as_slice());
    buffer.extend_from_slice(format!("${}\r\n", payload.len()).as_bytes());
    buffer.extend_from_slice(&payload);
    buffer.extend_from_slice(b"\r\n");
    assert_arguments(
        take_request_from_bytes_stream(&mut buffer)
            .unwrap()
            .unwrap(),
        &[b"ECHO", &payload],
    );
    assert!(buffer.is_empty());
}

#[test]
fn requests_allow_wide_flat_arrays() {
    let mut buffer = BytesMut::from(b"*4097\r\n$4\r\nMGET\r\n".as_slice());
    for _ in 0..4096 {
        buffer.extend_from_slice(b"$1\r\nk\r\n");
    }
    let mut expected: Vec<&[u8]> = vec![b"MGET"];
    expected.extend(std::iter::repeat_n(b"k".as_slice(), 4096));
    assert_arguments(
        take_request_from_bytes_stream(&mut buffer)
            .unwrap()
            .unwrap(),
        &expected,
    );
    assert!(buffer.is_empty());
}
