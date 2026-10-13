//! Prolly tree over (row ID, row hash) (specification v0.1, Section 8).
//!
//! A Prolly tree is a Merkle B-tree whose node boundaries come from
//! hashes of the keys. The shape depends only on the set of keys, so the
//! same rows always give the same tree and the same root hash, whatever
//! the order of inserts and whatever the history of the database.
//!
//! [`TreeBuilder`] takes the items in key order and returns the root hash.
//! It gives each finished node to a callback, so a caller can store the
//! nodes (for the catch-up diff in Phase 3) or ignore them.

use crate::{RowHash, RowId};

/// Key-derivation context for node hashes.
pub const NODE_HASH_CONTEXT: &str = "SynodSQL 2026-10-12 tree node v1";

/// Key-derivation context for node boundaries.
pub const BOUNDARY_CONTEXT: &str = "SynodSQL 2026-10-12 tree boundary v1";

/// Target mean number of items per node.
pub const TARGET_FANOUT: u32 = 64;

/// Length of one encoded item: key (16) and hash (32).
pub const ITEM_LEN: usize = 48;

/// A node hash, which is also the root hash of its subtree.
pub type NodeHash = [u8; 32];

/// Returns true if the item with `key` is the last item of its node at
/// `level`.
pub fn is_boundary(level: u8, key: RowId) -> bool {
    let mut h = blake3::Hasher::new_derive_key(BOUNDARY_CONTEXT);
    h.update(&[level]);
    h.update(&key.to_be_bytes());
    let bytes = h.finalize();
    let x = u32::from_be_bytes(bytes.as_bytes()[..4].try_into().unwrap());
    x < u32::MAX / TARGET_FANOUT
}

/// Returns the hash of an encoded node.
pub fn node_hash(node: &[u8]) -> NodeHash {
    let mut h = blake3::Hasher::new_derive_key(NODE_HASH_CONTEXT);
    h.update(node);
    *h.finalize().as_bytes()
}

/// The root hash of a tree with no rows.
pub fn empty_root() -> NodeHash {
    node_hash(&[0, 0, 0, 0, 0])
}

/// Encodes a node: level, item count, then each item's key and hash. It is
/// the encoding that [`TreeBuilder`] gives to its callback.
pub fn encode_node(level: u8, items: &[(RowId, [u8; 32])]) -> Vec<u8> {
    let mut node = Vec::with_capacity(5 + ITEM_LEN * items.len());
    node.push(level);
    node.extend_from_slice(&(items.len() as u32).to_be_bytes());
    for (key, hash) in items {
        node.extend_from_slice(&key.to_be_bytes());
        node.extend_from_slice(hash);
    }
    node
}

struct Level {
    // Encoded node in progress: level, count placeholder, items.
    node: Vec<u8>,
    count: u32,
    last_key: Option<RowId>,
}

impl Level {
    fn new(level: u8) -> Level {
        let mut node = Vec::with_capacity(5 + ITEM_LEN * TARGET_FANOUT as usize * 2);
        node.push(level);
        node.extend_from_slice(&[0; 4]);
        Level { node, count: 0, last_key: None }
    }
}

/// Builds a Prolly tree from items in strictly ascending key order.
pub struct TreeBuilder<F> {
    levels: Vec<Level>,
    // Number of nodes finished at each level, and the hash of the last one.
    finished: Vec<u64>,
    last_hash: Vec<NodeHash>,
    on_node: F,
}

impl TreeBuilder<fn(NodeHash, &[u8])> {
    /// Makes a builder that does not keep the nodes.
    pub fn new() -> Self {
        TreeBuilder::with_sink(|_, _| {})
    }
}

impl Default for TreeBuilder<fn(NodeHash, &[u8])> {
    fn default() -> Self {
        TreeBuilder::new()
    }
}

impl<F: FnMut(NodeHash, &[u8])> TreeBuilder<F> {
    /// Makes a builder that gives each finished node (hash, encoding) to
    /// `on_node`.
    pub fn with_sink(on_node: F) -> Self {
        TreeBuilder { levels: vec![Level::new(0)], finished: vec![0], last_hash: vec![[0; 32]], on_node }
    }

    /// Adds a row. Keys must be strictly ascending.
    ///
    /// # Panics
    ///
    /// Panics if `key` is not larger than the previous key.
    pub fn push(&mut self, key: RowId, hash: RowHash) {
        self.push_at(0, key, hash.0);
    }

    fn push_at(&mut self, level: usize, key: RowId, hash: NodeHash) {
        let l = &mut self.levels[level];
        if let Some(last) = l.last_key {
            assert!(key > last, "keys out of order at level {level}: {key} after {last}");
        }
        l.node.extend_from_slice(&key.to_be_bytes());
        l.node.extend_from_slice(&hash);
        l.count += 1;
        l.last_key = Some(key);
        if is_boundary(level as u8, key) {
            self.close(level);
        }
    }

    // Finishes the node in progress at `level` and adds it to the level
    // above.
    fn close(&mut self, level: usize) {
        let l = &mut self.levels[level];
        let key = l.last_key.expect("close of an empty node");
        l.node[1..5].copy_from_slice(&l.count.to_be_bytes());
        let node = std::mem::replace(l, Level::new(level as u8));
        let hash = node_hash(&node.node);
        (self.on_node)(hash, &node.node);
        self.finished[level] += 1;
        self.last_hash[level] = hash;
        if self.levels.len() == level + 1 {
            self.levels.push(Level::new(level as u8 + 1));
            self.finished.push(0);
            self.last_hash.push([0; 32]);
        }
        self.push_at(level + 1, key, hash);
    }

    /// Finishes the tree and returns the root hash.
    ///
    /// From the bottom up, each level closes its open node. The first
    /// level that made exactly one node holds the root.
    pub fn finish(mut self) -> NodeHash {
        for level in 0.. {
            if self.levels[level].count > 0 {
                self.close(level);
            }
            match self.finished[level] {
                0 => return empty_root(), // only at level 0, with no rows
                1 => return self.last_hash[level],
                _ => {}
            }
        }
        unreachable!()
    }
}

/// Returns the root hash of a set of rows given in key order.
pub fn root_hash(items: impl IntoIterator<Item = (RowId, RowHash)>) -> NodeHash {
    let mut b = TreeBuilder::new();
    for (k, h) in items {
        b.push(k, h);
    }
    b.finish()
}

/// The items of a node: (key, row hash or child node hash).
pub type NodeItems = Vec<(RowId, [u8; 32])>;

/// Decodes a node: its level and its items (key, hash). At level 0 the
/// hashes are row hashes; above, they are child node hashes. Returns
/// `None` if the encoding is not valid.
pub fn decode_node(node: &[u8]) -> Option<(u8, NodeItems)> {
    let (&level, rest) = node.split_first()?;
    let count = u32::from_be_bytes(rest.get(..4)?.try_into().ok()?) as usize;
    let body = &rest[4..];
    if body.len() != count.checked_mul(ITEM_LEN)? {
        return None;
    }
    let items: NodeItems = body
        .as_chunks::<ITEM_LEN>()
        .0
        .iter()
        .map(|c| Some((RowId::from_be_bytes(c[..16].try_into().unwrap())?, c[16..].try_into().unwrap())))
        .collect::<Option<_>>()?;
    if !items.windows(2).all(|w| w[0].0 < w[1].0) {
        return None;
    }
    Some((level, items))
}

/// Errors of a [`TreeWalk`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WalkError {
    /// A node that was not asked for, or that does not have its hash.
    UnexpectedNode,
    /// A node that cannot be decoded, or has the wrong level.
    BadNode,
}

/// Reads all items of a peer's tree, and fetches from the peer only the
/// nodes that this side does not have (catch-up and repair, specification
/// Section 6). It does no I/O: [`step`](TreeWalk::step) expands what the
/// local lookup has and returns the hashes to fetch; give each fetched
/// node to [`receive`](TreeWalk::receive) and step again. The same
/// subtree hash means the same items, so nodes that both sides have
/// never cross the network.
pub struct TreeWalk {
    // Nodes to expand: (hash, encoding).
    ready: Vec<(NodeHash, Vec<u8>)>,
    // Nodes to look up, then fetch.
    unknown: Vec<NodeHash>,
    wanted: std::collections::BTreeSet<NodeHash>,
    items: Vec<(RowId, RowHash)>,
    fetched: u64,
}

impl TreeWalk {
    /// Starts a walk of the tree with this root hash.
    pub fn new(root: NodeHash) -> TreeWalk {
        TreeWalk { ready: Vec::new(), unknown: vec![root], wanted: Default::default(), items: Vec::new(), fetched: 0 }
    }

    /// Expands every node that `local` has, and returns the hashes of the
    /// nodes to fetch from the peer. When it returns no hashes and no
    /// fetch is open, the walk is done.
    pub fn step(&mut self, mut local: impl FnMut(&NodeHash) -> Option<Vec<u8>>) -> Result<Vec<NodeHash>, WalkError> {
        let mut fetch = Vec::new();
        loop {
            while let Some(h) = self.unknown.pop() {
                match local(&h).filter(|n| node_hash(n) == h) {
                    Some(n) => self.ready.push((h, n)),
                    None => {
                        if self.wanted.insert(h) {
                            fetch.push(h);
                        }
                    }
                }
            }
            let Some((_, node)) = self.ready.pop() else { break };
            let (level, items) = decode_node(&node).ok_or(WalkError::BadNode)?;
            if level == 0 {
                self.items.extend(items.into_iter().map(|(k, h)| (k, RowHash(h))));
            } else {
                self.unknown.extend(items.into_iter().map(|(_, h)| h));
            }
        }
        Ok(fetch)
    }

    /// Takes a node fetched from the peer.
    pub fn receive(&mut self, hash: NodeHash, node: Vec<u8>) -> Result<(), WalkError> {
        if !self.wanted.remove(&hash) || node_hash(&node) != hash {
            return Err(WalkError::UnexpectedNode);
        }
        self.fetched += 1;
        self.ready.push((hash, node));
        Ok(())
    }

    /// Returns the number of nodes that are asked for and not received.
    pub fn open_fetches(&self) -> usize {
        self.wanted.len()
    }

    /// Returns the number of nodes received from the peer.
    pub fn fetched(&self) -> u64 {
        self.fetched
    }

    /// Returns all items of the peer's tree, in key order, and checks that
    /// they have the root hash.
    pub fn finish(mut self, root: NodeHash) -> Result<Vec<(RowId, RowHash)>, WalkError> {
        if !self.wanted.is_empty() || !self.unknown.is_empty() || !self.ready.is_empty() {
            return Err(WalkError::UnexpectedNode);
        }
        self.items.sort_by_key(|(k, _)| *k);
        if root_hash(self.items.iter().copied()) != root {
            return Err(WalkError::BadNode);
        }
        Ok(self.items)
    }
}

/// Compares two sorted item lists. Returns the keys whose hash differs or
/// that only `theirs` has, and the keys that only `ours` has.
pub fn diff_hashes(ours: &[(RowId, RowHash)], theirs: &[(RowId, RowHash)]) -> (Vec<RowId>, Vec<RowId>) {
    let (mut changed, mut removed) = (Vec::new(), Vec::new());
    let (mut i, mut j) = (0, 0);
    while i < ours.len() || j < theirs.len() {
        match (ours.get(i), theirs.get(j)) {
            (Some(a), Some(b)) if a.0 == b.0 => {
                if a.1 != b.1 {
                    changed.push(b.0);
                }
                i += 1;
                j += 1;
            }
            (Some(a), Some(b)) if a.0 < b.0 => {
                removed.push(a.0);
                i += 1;
            }
            (Some(a), None) => {
                removed.push(a.0);
                i += 1;
            }
            (_, Some(b)) => {
                changed.push(b.0);
                j += 1;
            }
            (None, None) => unreachable!(),
        }
    }
    (changed, removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeId;

    fn item(i: u64) -> (RowId, RowHash) {
        let id = RowId::new(NodeId((i % 3) as u32), i);
        (id, RowHash(*blake3::hash(&i.to_be_bytes()).as_bytes()))
    }

    fn sorted(range: impl Iterator<Item = u64>) -> Vec<(RowId, RowHash)> {
        let mut v: Vec<_> = range.map(item).collect();
        v.sort();
        v
    }

    #[test]
    fn empty_and_single() {
        assert_eq!(root_hash([]), empty_root());
        let (k, h) = item(1);
        let mut node = vec![0, 0, 0, 0, 1];
        node.extend_from_slice(&k.to_be_bytes());
        node.extend_from_slice(&h.0);
        assert_eq!(root_hash([(k, h)]), node_hash(&node));
    }

    #[test]
    fn a_change_changes_the_root() {
        let a = sorted(0..5_000);
        let mut b = a.clone();
        b[2_500].1.0[0] ^= 1;
        let mut c = a.clone();
        c.remove(100);
        let roots = [root_hash(a.clone()), root_hash(b), root_hash(c)];
        assert_ne!(roots[0], roots[1]);
        assert_ne!(roots[0], roots[2]);
        assert_ne!(roots[1], roots[2]);
        assert_eq!(roots[0], root_hash(a));
    }

    #[test]
    fn nodes_have_the_target_size_and_one_root() {
        let mut nodes = Vec::new();
        let mut b = TreeBuilder::with_sink(|h, n: &[u8]| nodes.push((h, n[0], n.len())));
        for (k, h) in sorted(0..50_000) {
            b.push(k, h);
        }
        let root = b.finish();
        let leaves = nodes.iter().filter(|n| n.1 == 0).count();
        let mean = 50_000 / leaves;
        assert!((32..=128).contains(&mean), "mean leaf fanout {mean}");
        let top = nodes.iter().map(|n| n.1).max().unwrap();
        let tops: Vec<_> = nodes.iter().filter(|n| n.1 == top).collect();
        assert_eq!(tops.len(), 1);
        assert_eq!(tops[0].0, root);
    }

    #[test]
    fn encode_node_matches_the_builder() {
        let mut nodes = Vec::new();
        let mut b = TreeBuilder::with_sink(|h, n: &[u8]| nodes.push((h, n.to_vec())));
        for (k, h) in sorted(0..5_000) {
            b.push(k, h);
        }
        b.finish();
        for (h, n) in nodes {
            let (level, items) = decode_node(&n).unwrap();
            assert_eq!(encode_node(level, &items), n);
            assert_eq!(node_hash(&encode_node(level, &items)), h);
        }
        assert_eq!(encode_node(0, &[]), vec![0; 5]);
    }

    #[test]
    #[should_panic(expected = "out of order")]
    fn rejects_unsorted_input() {
        let mut v = sorted(0..10);
        v.swap(3, 4);
        root_hash(v);
    }

    #[test]
    fn walk_fetches_only_differing_nodes() {
        use std::collections::HashMap;
        let items = |n: u64, skip: u64| -> Vec<(RowId, RowHash)> {
            (0..n)
                .filter(|i| i % 1000 != skip)
                .map(|i| (RowId::new(NodeId(1), i), RowHash([(i % 251) as u8; 32])))
                .collect()
        };
        let build = |it: &[(RowId, RowHash)]| {
            let mut nodes = HashMap::new();
            let mut b = TreeBuilder::with_sink(|h, n: &[u8]| {
                nodes.insert(h, n.to_vec());
            });
            for (k, h) in it {
                b.push(*k, *h);
            }
            let root = b.finish();
            nodes.insert(empty_root(), vec![0; 5]);
            (root, nodes)
        };
        let theirs = items(20_000, 7);
        let mut ours = items(20_000, 3);
        ours[100].1 = RowHash([9; 32]);
        let (their_root, their_nodes) = build(&theirs);
        let (_, our_nodes) = build(&ours);
        let mut walk = TreeWalk::new(their_root);
        loop {
            let fetch = walk.step(|h| our_nodes.get(h).cloned()).unwrap();
            if fetch.is_empty() {
                break;
            }
            for h in fetch {
                walk.receive(h, their_nodes[&h].clone()).unwrap();
            }
        }
        let fetched = walk.fetched();
        assert!(fetched < their_nodes.len() as u64 / 5, "{fetched} of {}", their_nodes.len());
        let got = walk.finish(their_root).unwrap();
        assert_eq!(got, theirs);
        let (changed, removed) = diff_hashes(&ours, &got);
        assert_eq!(removed.len(), 20);
        assert_eq!(changed.len(), 21);
        assert!(TreeWalk::new(their_root).receive([1; 32], vec![]).is_err());
        let (empty, _) = build(&[]);
        let mut w = TreeWalk::new(empty);
        assert!(w.step(|_| Some(vec![0; 5])).unwrap().is_empty());
        assert!(w.finish(empty).unwrap().is_empty());
    }
}
