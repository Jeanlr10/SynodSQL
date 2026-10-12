//! Property tests for HLC order, row ID uniqueness, and canonical encoding.

use proptest::prelude::*;
use synod_core::hlc::MAX_PHYSICAL_MS;
use synod_core::row_id::MemoryCounterStore;
use synod_core::*;

// ---------------------------------------------------------------- HLC

#[derive(Clone, Debug)]
enum ClockOp {
    Now { physical_ms: u64 },
    Update { physical_ms: u64, remote: Hlc },
}

fn any_hlc() -> impl Strategy<Value = Hlc> {
    (0..=MAX_PHYSICAL_MS, any::<u16>()).prop_map(|(p, c)| Hlc::new(p, c).unwrap())
}

fn clock_op() -> impl Strategy<Value = ClockOp> {
    // Small physical range, so values collide often and the logical
    // counter is exercised.
    let pt = 1_000u64..1_050;
    prop_oneof![
        pt.clone().prop_map(|physical_ms| ClockOp::Now { physical_ms }),
        (pt, 990u64..1_100, any::<u16>())
            .prop_map(|(physical_ms, rp, rc)| ClockOp::Update { physical_ms, remote: Hlc::new(rp, rc).unwrap() }),
    ]
}

proptest! {
    /// Every value a clock gives out is larger than the one before, also
    /// when the physical clock goes backward.
    #[test]
    fn hlc_is_strictly_monotonic(ops in prop::collection::vec(clock_op(), 1..300)) {
        let mut clock = HlcClock::new(20);
        let mut prev = Hlc::ZERO;
        for op in ops {
            let before = clock.last();
            let result = match op {
                ClockOp::Now { physical_ms } => clock.now(physical_ms),
                ClockOp::Update { physical_ms, remote } => clock.update(remote, physical_ms),
            };
            match result {
                Ok(h) => {
                    prop_assert!(h > prev, "{h:?} not after {prev:?}");
                    prop_assert_eq!(clock.last(), h);
                    prev = h;
                }
                Err(_) => prop_assert_eq!(clock.last(), before),
            }
        }
    }

    /// A receive gives a value after the remote value (causality).
    #[test]
    fn hlc_receive_is_after_send(start in any_hlc(), remote in any_hlc(), pt in 0..=MAX_PHYSICAL_MS) {
        let mut clock = HlcClock::resume(start, u64::MAX);
        if let Ok(h) = clock.update(remote, pt) {
            prop_assert!(h > remote);
            prop_assert!(h > start);
            prop_assert!(h.physical_ms() >= pt);
        }
    }

    /// A remote value too far ahead is rejected, and the clock stays the
    /// same. A value inside the limit is accepted.
    #[test]
    fn hlc_drift_limit(pt in 0u64..1 << 40, max_drift in 0u64..10_000, ahead in 0u64..20_000, c in any::<u16>()) {
        let mut clock = HlcClock::new(max_drift);
        let before = clock.now(pt).unwrap();
        let remote = Hlc::new(pt + ahead, c).unwrap();
        let result = clock.update(remote, pt);
        if ahead > max_drift {
            let rejected = matches!(result, Err(HlcError::DriftExceeded { .. }));
            prop_assert!(rejected);
            prop_assert_eq!(clock.last(), before);
        } else {
            prop_assert!(result.is_ok());
        }
    }

    /// With a physical clock that does not go backward, and peers inside
    /// the drift limit, the HLC stays within the limit of the physical
    /// clock (plus 1 ms for a logical counter that overflows).
    #[test]
    fn hlc_stays_near_physical_time(
        steps in prop::collection::vec((0u64..5, prop::option::of((0u64..=100, any::<u16>()))), 1..500),
    ) {
        const MAX_DRIFT: u64 = 100;
        let mut clock = HlcClock::new(MAX_DRIFT);
        let mut pt = 1_000_000u64;
        for (advance, remote) in steps {
            pt += advance;
            let h = match remote {
                Some((ahead, c)) => clock.update(Hlc::new(pt + ahead, c).unwrap(), pt).unwrap(),
                None => clock.now(pt).unwrap(),
            };
            prop_assert!(h.physical_ms() >= pt);
            prop_assert!(h.physical_ms() <= pt + MAX_DRIFT + 1, "{h:?} too far ahead of {pt}");
        }
    }

    #[test]
    fn hlc_bytes_round_trip(h in any_hlc()) {
        prop_assert_eq!(Hlc::from_be_bytes(h.to_be_bytes()), h);
        prop_assert_eq!(Hlc::new(h.physical_ms(), h.logical()), Some(h));
    }

    #[test]
    fn stamp_byte_order_is_stamp_order(a in any_hlc(), an in any::<u32>(), b in any_hlc(), bn in any::<u32>()) {
        let a = Stamp { hlc: a, node: NodeId(an) };
        let b = Stamp { hlc: b, node: NodeId(bn) };
        prop_assert_eq!(a.cmp(&b), a.to_be_bytes().cmp(&b.to_be_bytes()));
    }
}

// ---------------------------------------------------------------- Row IDs

proptest! {
    /// Over any sequence of allocations and restarts, one node never gives
    /// out the same row ID twice, and its IDs always increase.
    #[test]
    fn row_ids_unique_across_restarts(
        node in any::<u32>(),
        block in 1u64..64,
        runs in prop::collection::vec(0usize..200, 1..20),
    ) {
        let mut store = MemoryCounterStore::default();
        let mut last: Option<RowId> = None;
        for count in runs {
            let mut alloc = RowIdAllocator::open(NodeId(node), store, block).unwrap();
            for _ in 0..count {
                let id = alloc.allocate().unwrap();
                prop_assert_eq!(id.node(), NodeId(node));
                prop_assert!(last.is_none_or(|l| id > l), "{id} after {last:?}");
                // The persisted mark covers every ID given out.
                prop_assert!(alloc.store().high_water > id.counter());
                last = Some(id);
            }
            store = alloc.into_store(); // restart
        }
    }

    #[test]
    fn row_id_bytes_round_trip(node in any::<u32>(), counter in any::<u64>()) {
        let id = RowId::new(NodeId(node), counter);
        prop_assert_eq!(id.node(), NodeId(node));
        prop_assert_eq!(id.counter(), counter);
        prop_assert_eq!(RowId::from_be_bytes(id.to_be_bytes()), Some(id));
    }

    #[test]
    fn row_id_byte_order_is_id_order(a in any::<u128>(), b in any::<u128>()) {
        let a = RowId::new(NodeId((a >> 64) as u32), a as u64);
        let b = RowId::new(NodeId((b >> 64) as u32), b as u64);
        prop_assert_eq!(a.cmp(&b), a.to_be_bytes().cmp(&b.to_be_bytes()));
        prop_assert_eq!(a.cmp(&b), (a.node(), a.counter()).cmp(&(b.node(), b.counter())));
    }
}

// ---------------------------------------------------------------- Encoding

fn scalar() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<i64>().prop_map(Value::Integer),
        any::<u64>().prop_map(|bits| Value::Real(f64::from_bits(bits))),
        prop_oneof![Just(f64::NAN), Just(-0.0), Just(0.0), Just(f64::INFINITY)].prop_map(Value::Real),
        (any::<i128>(), 0u8..=38).prop_map(|(u, s)| Value::Decimal(Decimal::new(u, s).unwrap())),
        ".{0,20}".prop_map(Value::Text),
        prop::collection::vec(any::<u8>(), 0..20).prop_map(Value::Blob),
    ]
}

fn value() -> impl Strategy<Value = Value> {
    prop_oneof![
        4 => scalar(),
        1 => prop::collection::vec(scalar(), 0..8).prop_map(|items| Value::Set(ValueSet::new(items).unwrap())),
    ]
}

fn row() -> impl Strategy<Value = Row> {
    let cell = (any::<u64>(), any::<u32>(), value())
        .prop_map(|(h, n, value)| Cell { stamp: Stamp { hlc: Hlc::from_u64(h), node: NodeId(n) }, value });
    (any::<u32>(), any::<u64>(), any::<bool>(), prop::collection::vec(cell, 0..8)).prop_map(
        |(node, counter, tombstone, cells)| Row { row_id: RowId::new(NodeId(node), counter), tombstone, cells },
    )
}

proptest! {
    /// Decoding gives back an equal value, and encoding it again gives the
    /// same bytes.
    #[test]
    fn value_round_trip(v in value()) {
        let bytes = v.to_canonical_bytes();
        let back = Value::from_canonical_bytes(&bytes).unwrap();
        prop_assert_eq!(&back, &v);
        prop_assert_eq!(back.to_canonical_bytes(), bytes);
    }

    /// The decoder accepts only canonical bytes: any input that decodes
    /// encodes back to exactly the same bytes.
    #[test]
    fn decoder_accepts_only_canonical_bytes(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        if let Ok(v) = Value::from_canonical_bytes(&bytes) {
            prop_assert_eq!(v.to_canonical_bytes(), bytes);
        }
    }

    /// A mutated encoding is either rejected or decodes to a value whose
    /// encoding is the mutated bytes. It never decodes to a second form of
    /// some value.
    #[test]
    fn mutated_encoding_is_rejected_or_canonical(v in value(), index in any::<prop::sample::Index>(), byte in any::<u8>()) {
        let mut bytes = v.to_canonical_bytes();
        let i = index.index(bytes.len());
        bytes[i] = byte;
        if let Ok(w) = Value::from_canonical_bytes(&bytes) {
            prop_assert_eq!(w.to_canonical_bytes(), bytes);
        }
    }

    #[test]
    fn row_round_trip(r in row()) {
        let bytes = r.to_canonical_bytes();
        let back = Row::from_canonical_bytes(&bytes).unwrap();
        prop_assert_eq!(&back, &r);
        prop_assert_eq!(back.to_canonical_bytes(), bytes);
        prop_assert_eq!(back.hash(), r.hash());
    }

    /// Equal values give equal row hashes, also when they were built in
    /// different ways (unnormalized decimal, other NaN bits, set order).
    #[test]
    fn equal_values_hash_equal(units in any::<i64>(), scale in 0u8..10, items in prop::collection::vec(scalar(), 0..6)) {
        let stamp = Stamp::default();
        let id = RowId::new(NodeId(1), 1);
        let a = Decimal::new(units as i128, scale).unwrap();
        let b = Decimal::new(units as i128 * 1000, scale + 3).unwrap();
        let mut reversed = items.clone();
        reversed.reverse();
        let make = |d, set_items: Vec<Value>, nan_bits| Row {
            row_id: id,
            tombstone: false,
            cells: vec![
                Cell { stamp, value: Value::Decimal(d) },
                Cell { stamp, value: Value::Set(ValueSet::new(set_items).unwrap()) },
                Cell { stamp, value: Value::Real(f64::from_bits(nan_bits)) },
            ],
        };
        prop_assert_eq!(make(a, items, 0x7ff8_0000_0000_0000).hash(), make(b, reversed, 0xfff0_0000_dead_beef).hash());
    }

    #[test]
    fn decimal_text_round_trip(units in any::<i128>(), scale in 0u8..=38) {
        let d = Decimal::new(units, scale).unwrap();
        prop_assert_eq!(d.to_string().parse::<Decimal>(), Ok(d));
    }
}

// ---------------------------------------------------------------- Prolly tree

/// A plain reference build: split each level into nodes at the
/// boundaries, then build the level above, until one node is left.
fn reference_root(items: &[(RowId, RowHash)]) -> [u8; 32] {
    use synod_core::prolly::{empty_root, is_boundary, node_hash};
    if items.is_empty() {
        return empty_root();
    }
    let mut level_items: Vec<(RowId, [u8; 32])> = items.iter().map(|(k, h)| (*k, h.0)).collect();
    for level in 0u8.. {
        let mut nodes: Vec<Vec<(RowId, [u8; 32])>> = vec![vec![]];
        for (k, h) in &level_items {
            nodes.last_mut().unwrap().push((*k, *h));
            if is_boundary(level, *k) {
                nodes.push(vec![]);
            }
        }
        if nodes.last().unwrap().is_empty() {
            nodes.pop();
        }
        let hashed: Vec<(RowId, [u8; 32])> = nodes
            .iter()
            .map(|node| {
                let mut bytes = vec![level];
                bytes.extend_from_slice(&(node.len() as u32).to_be_bytes());
                for (k, h) in node {
                    bytes.extend_from_slice(&k.to_be_bytes());
                    bytes.extend_from_slice(h);
                }
                (node.last().unwrap().0, node_hash(&bytes))
            })
            .collect();
        if hashed.len() == 1 {
            return hashed[0].1;
        }
        level_items = hashed;
    }
    unreachable!()
}

proptest! {
    /// The streaming builder gives the same root as the plain reference
    /// build, for any set of rows, and the input order does not matter.
    #[test]
    fn prolly_matches_reference(keys in prop::collection::btree_set(any::<(u32, u64)>(), 0..3_000), seed in any::<u64>()) {
        let items: Vec<(RowId, RowHash)> = keys
            .iter()
            .map(|&(n, c)| {
                let id = RowId::new(NodeId(n % 4), c);
                (id, RowHash(*blake3::hash(&[c.to_be_bytes(), seed.to_be_bytes()].concat()).as_bytes()))
            })
            .collect::<std::collections::BTreeMap<_, _>>()
            .into_iter()
            .collect();
        prop_assert_eq!(synod_core::prolly::root_hash(items.clone()), reference_root(&items));
    }
}
