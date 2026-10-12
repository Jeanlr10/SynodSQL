//! Pinned test vectors for the canonical encoding and row hash.
//!
//! These bytes and hashes are part of format v0.1. CI runs this file on
//! x86-64 and ARM64, so a platform that encodes a row differently fails
//! here. Do not update a vector unless the specification changes, and then
//! change the format version too.

use synod_core::*;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn stamp(hlc_ms: u64, node: u32) -> Stamp {
    Stamp { hlc: Hlc::new(hlc_ms, 0).unwrap(), node: NodeId(node) }
}

fn dec(s: &str) -> Value {
    Value::Decimal(s.parse().unwrap())
}

/// Row r1 of the worked example in the white paper (Section 8), after the
/// merge: sku A-100, name "Bolt, M6 x 20", price 0.11, qty 500.
fn worked_example_r1() -> Row {
    Row {
        row_id: RowId::new(NodeId(0xA), 1),
        tombstone: false,
        cells: vec![
            Cell { stamp: stamp(1, 0xA), value: Value::Text("A-100".into()) },
            Cell { stamp: stamp(11, 0xB), value: Value::Text("Bolt, M6 x 20".into()) },
            Cell { stamp: stamp(17, 0xC), value: dec("0.11") },
            Cell { stamp: stamp(1, 0xA), value: Value::Integer(500) },
        ],
    }
}

#[test]
fn value_vectors() {
    let set = ValueSet::new([Value::Text("b".into()), Value::Text("a".into())]).unwrap();
    let cases: &[(Value, &str)] = &[
        (Value::Null, "00"),
        (Value::Integer(1), "010000000000000001"),
        (Value::Integer(-1), "01ffffffffffffffff"),
        (Value::Real(1.5), "023ff8000000000000"),
        (Value::Real(-0.0), "028000000000000000"),
        (Value::Real(f64::NAN), "027ff8000000000000"),
        (dec("0.10"), "030100000000000000000000000000000001"),
        (dec("-12.5"), "0301ffffffffffffffffffffffffffffff83"),
        (Value::Text("é".into()), "0400000002c3a9"),
        (Value::Blob(vec![0xde, 0xad]), "0500000002dead"),
        (Value::Set(set), "0600000002040000000161040000000162"),
    ];
    for (value, expected) in cases {
        assert_eq!(&hex(&value.to_canonical_bytes()), expected, "{value:?}");
    }
}

#[test]
fn row_vector() {
    let expected = [
        "01",                                   // format version
        "0000000a000000000000000100000000",     // row_id: node 0xA, counter 1
        "00",                                   // flags: live
        "0004",                                 // 4 columns
        "00000000000100000000000a",             // stamp (1 ms, node A)
        "0400000005412d313030",                 // "A-100"
        "00000000000b00000000000b",             // stamp (11 ms, node B)
        "040000000d426f6c742c204d362078203230", // "Bolt, M6 x 20"
        "00000000001100000000000c",             // stamp (17 ms, node C)
        "03020000000000000000000000000000000b", // 0.11
        "00000000000100000000000a",             // stamp (1 ms, node A)
        "0100000000000001f4",                   // 500
    ]
    .concat();
    assert_eq!(hex(&worked_example_r1().to_canonical_bytes()), expected);
}

#[test]
fn row_hash_vectors() {
    let live = worked_example_r1();
    let mut tombstone = live.clone();
    tombstone.tombstone = true;
    let empty = Row { row_id: RowId::new(NodeId(0), 0), tombstone: false, cells: vec![] };
    // Computed independently with the Python `blake3` package from the
    // bytes in `row_vector`.
    for (name, row, expected) in [
        ("worked example r1", &live, "64a5aae2a8e1bf8e32673a95713078ae9558dbe0b2e3767030565b714889f6e9"),
        ("tombstone r1", &tombstone, "b36bea63f2c82b8ace1b8ab4b16bc1bc01201db40a6fec737bacce4964aee611"),
        ("empty row", &empty, "b58fd0ce6979f11331788fc81038eedd0197aa9123b27c90c269318e63559d22"),
    ] {
        assert_eq!(row.hash().to_string(), expected, "{name}");
    }
}

/// Prolly tree roots. Items: row ID (node i mod 3, counter i), row hash
/// BLAKE3(i as u64 big-endian), for i in 0..count. Computed independently
/// with a Python build of specification Section 8.
#[test]
fn prolly_root_vectors() {
    for (count, expected) in [
        (0u64, "b781189e5bd7ace7f05f9a989290cd7108d714087a69ace91793215c50866679"),
        (1, "2d8e105a6d69e84e12a13eefc864a6cdee51dc71c57a0f1d011daa626c0172b3"),
        (1_000, "88a4dc5cadb474b40228847a267c48966c6dd7c07bf618e9f74992f4733f5ce8"),
        (20_000, "56b7f3fadff3548968a4662b12ad9522496306da51e517bcd28e9e21056827a8"),
    ] {
        let mut items: Vec<(RowId, RowHash)> = (0..count)
            .map(|i| (RowId::new(NodeId((i % 3) as u32), i), RowHash(*blake3::hash(&i.to_be_bytes()).as_bytes())))
            .collect();
        items.sort();
        assert_eq!(hex(&synod_core::prolly::root_hash(items)), expected, "{count} items");
    }
}
