//! Messages between nodes, and their binary encoding (specification
//! Section 6.4).
//!
//! A message is one version byte, one tag byte, and the fields. Integers
//! are big-endian; byte strings have a u32 length first. The transport
//! names the sender; a message does not carry it.

use synod_core::manifest::{LogRef, Manifest};
use synod_core::prolly::NodeHash;
use synod_core::{NodeId, Row, RowId};
use synod_store::{BaseImage, BaseRow, MergedLog, MergedRecord};

/// Version of the message encoding.
pub const MSG_VERSION: u8 = 1;

/// One merge of a node's chain, as [`Status`] shows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Link {
    /// Epoch.
    pub epoch: u64,
    /// Manifest hash.
    pub hash: [u8; 32],
    /// The manifest's parent.
    pub parent: [u8; 32],
    /// The manifest's rank.
    pub rank: u32,
}

/// A node's position: its chain of merges for the last L epochs.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Status {
    /// The open epoch (the next to seal).
    pub open_epoch: u64,
    /// The merges that the node keeps, by epoch; the last is its head.
    pub chain: Vec<Link>,
    /// The node's own sealed logs that its chain has not merged, within
    /// the lateness bound: a peer that lacks one asks for it.
    pub own_logs: Vec<LogRef>,
}

impl Status {
    /// The last merged epoch.
    pub fn head_epoch(&self) -> Option<u64> {
        self.chain.last().map(|l| l.epoch)
    }
}

/// A message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Msg {
    /// A sealed log.
    Log(Vec<u8>),
    /// A manifest, from its leader or passed on.
    Manifest(Manifest),
    /// The root hash of M(e) after a merge, for the verify step.
    Root {
        /// Epoch.
        epoch: u64,
        /// The merged manifest.
        manifest: [u8; 32],
        /// Root hash of M(e).
        root: NodeHash,
    },
    /// Asks for the receiver's [`Status`].
    GetStatus,
    /// A node's status.
    Status(Status),
    /// Asks for logs by hash.
    GetLogs(Vec<[u8; 32]>),
    /// Logs, and the hashes asked for that the sender does not keep.
    Logs {
        /// Sealed logs.
        logs: Vec<Vec<u8>>,
        /// Hashes not kept.
        missing: Vec<[u8; 32]>,
    },
    /// Asks for the manifests of the receiver's chain from this epoch on.
    GetChain(u64),
    /// Manifests of the sender's chain, by epoch.
    Chain(Vec<Manifest>),
    /// Asks for the receiver's [`BaseImage`].
    GetImage,
    /// A base image.
    Image(BaseImage),
    /// Asks for nodes of the receiver's base tree.
    GetTree(Vec<NodeHash>),
    /// Nodes of the sender's base tree (`None`: not in it), with its root.
    Tree {
        /// The sender's base root.
        root: NodeHash,
        /// Nodes.
        nodes: Vec<(NodeHash, Option<Vec<u8>>)>,
    },
    /// Asks for rows of the receiver's base state.
    GetRows(Vec<RowId>),
    /// Rows of the sender's base state, with its root.
    Rows {
        /// The sender's base root.
        root: NodeHash,
        /// Rows.
        rows: Vec<(RowId, BaseRow)>,
    },
}

/// A message that cannot be decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BadMessage(pub &'static str);

impl std::fmt::Display for BadMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bad message: {}", self.0)
    }
}

impl std::error::Error for BadMessage {}

struct W(Vec<u8>);

impl W {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn h(&mut self, v: &[u8; 32]) {
        self.0.extend_from_slice(v);
    }
    fn bytes(&mut self, v: &[u8]) {
        self.u32(v.len() as u32);
        self.0.extend_from_slice(v);
    }
    fn len(&mut self, n: usize) {
        self.u32(n as u32);
    }
}

struct R<'a>(&'a [u8]);

impl<'a> R<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], BadMessage> {
        if self.0.len() < n {
            return Err(BadMessage("unexpected end"));
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn u8(&mut self) -> Result<u8, BadMessage> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, BadMessage> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, BadMessage> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn h(&mut self) -> Result<[u8; 32], BadMessage> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn bytes(&mut self) -> Result<Vec<u8>, BadMessage> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    // A count, checked against the bytes left so that a bad count cannot
    // make a huge allocation.
    fn len(&mut self, min_item: usize) -> Result<usize, BadMessage> {
        let n = self.u32()? as usize;
        if n.saturating_mul(min_item) > self.0.len() {
            return Err(BadMessage("count too large"));
        }
        Ok(n)
    }
    fn row_id(&mut self) -> Result<RowId, BadMessage> {
        RowId::from_be_bytes(self.take(16)?.try_into().unwrap()).ok_or(BadMessage("row ID"))
    }
    fn manifest(&mut self) -> Result<Manifest, BadMessage> {
        Manifest::decode(&self.bytes()?).map_err(|_| BadMessage("manifest"))
    }
    fn opt_u64(&mut self) -> Result<Option<u64>, BadMessage> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            _ => Err(BadMessage("option")),
        }
    }
}

const LOG: u8 = 1;
const MANIFEST: u8 = 2;
const ROOT: u8 = 3;
const GET_STATUS: u8 = 4;
const STATUS: u8 = 5;
const GET_LOGS: u8 = 6;
const LOGS: u8 = 7;
const GET_CHAIN: u8 = 8;
const CHAIN: u8 = 9;
const GET_IMAGE: u8 = 10;
const IMAGE: u8 = 11;
const GET_TREE: u8 = 12;
const TREE: u8 = 13;
const GET_ROWS: u8 = 14;
const ROWS: u8 = 15;

impl Msg {
    /// A short name, for traces and metrics.
    pub fn name(&self) -> &'static str {
        match self {
            Msg::Log(_) => "log",
            Msg::Manifest(_) => "manifest",
            Msg::Root { .. } => "root",
            Msg::GetStatus => "get_status",
            Msg::Status(_) => "status",
            Msg::GetLogs(_) => "get_logs",
            Msg::Logs { .. } => "logs",
            Msg::GetChain(_) => "get_chain",
            Msg::Chain(_) => "chain",
            Msg::GetImage => "get_image",
            Msg::Image(_) => "image",
            Msg::GetTree(_) => "get_tree",
            Msg::Tree { .. } => "tree",
            Msg::GetRows(_) => "get_rows",
            Msg::Rows { .. } => "rows",
        }
    }

    /// Returns the encoding.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W(vec![MSG_VERSION]);
        match self {
            Msg::Log(b) => {
                w.u8(LOG);
                w.bytes(b);
            }
            Msg::Manifest(m) => {
                w.u8(MANIFEST);
                w.bytes(&m.encode());
            }
            Msg::Root { epoch, manifest, root } => {
                w.u8(ROOT);
                w.u64(*epoch);
                w.h(manifest);
                w.h(root);
            }
            Msg::GetStatus => w.u8(GET_STATUS),
            Msg::Status(s) => {
                w.u8(STATUS);
                w.u64(s.open_epoch);
                w.len(s.chain.len());
                for l in &s.chain {
                    w.u64(l.epoch);
                    w.h(&l.hash);
                    w.h(&l.parent);
                    w.u32(l.rank);
                }
                w.len(s.own_logs.len());
                for l in &s.own_logs {
                    w.u32(l.node.0);
                    w.u64(l.epoch);
                    w.h(&l.hash);
                }
            }
            Msg::GetLogs(hs) => {
                w.u8(GET_LOGS);
                w.len(hs.len());
                hs.iter().for_each(|h| w.h(h));
            }
            Msg::Logs { logs, missing } => {
                w.u8(LOGS);
                w.len(logs.len());
                logs.iter().for_each(|l| w.bytes(l));
                w.len(missing.len());
                missing.iter().for_each(|h| w.h(h));
            }
            Msg::GetChain(from) => {
                w.u8(GET_CHAIN);
                w.u64(*from);
            }
            Msg::Chain(ms) => {
                w.u8(CHAIN);
                w.len(ms.len());
                ms.iter().for_each(|m| w.bytes(&m.encode()));
            }
            Msg::GetImage => w.u8(GET_IMAGE),
            Msg::Image(img) => {
                w.u8(IMAGE);
                match img.epoch {
                    None => w.u8(0),
                    Some(e) => {
                        w.u8(1);
                        w.u64(e);
                    }
                }
                w.h(&img.root);
                w.len(img.merged.len());
                for r in &img.merged {
                    w.bytes(&r.manifest.encode());
                    w.h(&r.base_before);
                    w.h(&r.root);
                }
                w.len(img.merged_logs.len());
                for l in &img.merged_logs {
                    w.h(&l.hash);
                    w.u64(l.merged_in);
                    w.u32(l.node.0);
                    w.u64(l.log_epoch);
                }
            }
            Msg::GetTree(hs) => {
                w.u8(GET_TREE);
                w.len(hs.len());
                hs.iter().for_each(|h| w.h(h));
            }
            Msg::Tree { root, nodes } => {
                w.u8(TREE);
                w.h(root);
                w.len(nodes.len());
                for (h, n) in nodes {
                    w.h(h);
                    match n {
                        None => w.u8(0),
                        Some(n) => {
                            w.u8(1);
                            w.bytes(n);
                        }
                    }
                }
            }
            Msg::GetRows(ids) => {
                w.u8(GET_ROWS);
                w.len(ids.len());
                ids.iter().for_each(|id| w.0.extend_from_slice(&id.to_be_bytes()));
            }
            Msg::Rows { root, rows } => {
                w.u8(ROWS);
                w.h(root);
                w.len(rows.len());
                for (id, row) in rows {
                    w.0.extend_from_slice(&id.to_be_bytes());
                    match row {
                        None => w.u8(0),
                        Some((t, r)) => {
                            w.u8(1);
                            w.u32(*t);
                            w.bytes(&r.to_canonical_bytes());
                        }
                    }
                }
            }
        }
        w.0
    }

    /// Decodes a message.
    pub fn decode(bytes: &[u8]) -> Result<Msg, BadMessage> {
        let mut r = R(bytes);
        if r.u8()? != MSG_VERSION {
            return Err(BadMessage("version"));
        }
        let msg = match r.u8()? {
            LOG => Msg::Log(r.bytes()?),
            MANIFEST => Msg::Manifest(r.manifest()?),
            ROOT => Msg::Root { epoch: r.u64()?, manifest: r.h()?, root: r.h()? },
            GET_STATUS => Msg::GetStatus,
            STATUS => {
                let open_epoch = r.u64()?;
                let n = r.len(76)?;
                let chain = (0..n)
                    .map(|_| Ok(Link { epoch: r.u64()?, hash: r.h()?, parent: r.h()?, rank: r.u32()? }))
                    .collect::<Result<_, BadMessage>>()?;
                let n = r.len(44)?;
                let own_logs = (0..n)
                    .map(|_| Ok(LogRef { node: NodeId(r.u32()?), epoch: r.u64()?, hash: r.h()? }))
                    .collect::<Result<_, BadMessage>>()?;
                Msg::Status(Status { open_epoch, chain, own_logs })
            }
            GET_LOGS => {
                let n = r.len(32)?;
                Msg::GetLogs((0..n).map(|_| r.h()).collect::<Result<_, _>>()?)
            }
            LOGS => {
                let n = r.len(4)?;
                let logs = (0..n).map(|_| r.bytes()).collect::<Result<_, _>>()?;
                let n = r.len(32)?;
                let missing = (0..n).map(|_| r.h()).collect::<Result<_, _>>()?;
                Msg::Logs { logs, missing }
            }
            GET_CHAIN => Msg::GetChain(r.u64()?),
            CHAIN => {
                let n = r.len(4)?;
                Msg::Chain((0..n).map(|_| r.manifest()).collect::<Result<_, _>>()?)
            }
            GET_IMAGE => Msg::GetImage,
            IMAGE => {
                let epoch = r.opt_u64()?;
                let root = r.h()?;
                let n = r.len(68)?;
                let merged = (0..n)
                    .map(|_| {
                        let manifest = r.manifest()?;
                        Ok(MergedRecord {
                            epoch: manifest.epoch,
                            hash: manifest.hash(),
                            manifest,
                            base_before: r.h()?,
                            root: r.h()?,
                            undoable: false,
                        })
                    })
                    .collect::<Result<_, BadMessage>>()?;
                let n = r.len(52)?;
                let merged_logs = (0..n)
                    .map(|_| {
                        Ok(MergedLog { hash: r.h()?, merged_in: r.u64()?, node: NodeId(r.u32()?), log_epoch: r.u64()? })
                    })
                    .collect::<Result<_, BadMessage>>()?;
                Msg::Image(BaseImage { epoch, root, merged, merged_logs })
            }
            GET_TREE => {
                let n = r.len(32)?;
                Msg::GetTree((0..n).map(|_| r.h()).collect::<Result<_, _>>()?)
            }
            TREE => {
                let root = r.h()?;
                let n = r.len(33)?;
                let nodes = (0..n)
                    .map(|_| {
                        let h = r.h()?;
                        let node = match r.u8()? {
                            0 => None,
                            1 => Some(r.bytes()?),
                            _ => return Err(BadMessage("option")),
                        };
                        Ok((h, node))
                    })
                    .collect::<Result<_, BadMessage>>()?;
                Msg::Tree { root, nodes }
            }
            GET_ROWS => {
                let n = r.len(16)?;
                Msg::GetRows((0..n).map(|_| r.row_id()).collect::<Result<_, _>>()?)
            }
            ROWS => {
                let root = r.h()?;
                let n = r.len(17)?;
                let rows = (0..n)
                    .map(|_| {
                        let id = r.row_id()?;
                        let row = match r.u8()? {
                            0 => None,
                            1 => {
                                let t = r.u32()?;
                                let row = Row::from_canonical_bytes(&r.bytes()?).map_err(|_| BadMessage("row"))?;
                                if row.row_id != id {
                                    return Err(BadMessage("row ID does not match"));
                                }
                                Some((t, row))
                            }
                            _ => return Err(BadMessage("option")),
                        };
                        Ok((id, row))
                    })
                    .collect::<Result<_, BadMessage>>()?;
                Msg::Rows { root, rows }
            }
            _ => return Err(BadMessage("tag")),
        };
        if !r.0.is_empty() {
            return Err(BadMessage("trailing bytes"));
        }
        Ok(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use synod_core::{Cell, Hlc, Stamp, Value};

    #[test]
    fn every_message_round_trips() {
        let m =
            Manifest::new(4, 1, NodeId(2), [3; 32], Some(1), vec![LogRef { node: NodeId(1), epoch: 4, hash: [5; 32] }]);
        let row = Row {
            row_id: RowId::new(NodeId(1), 7),
            deleted: Some(3),
            cells: vec![Cell {
                stamp: Stamp { hlc: Hlc::new(5, 1).unwrap(), node: NodeId(1) },
                value: Value::Integer(9),
            }],
        };
        let msgs = vec![
            Msg::Log(vec![1, 2, 3]),
            Msg::Manifest(m.clone()),
            Msg::Root { epoch: 4, manifest: [1; 32], root: [2; 32] },
            Msg::GetStatus,
            Msg::Status(Status {
                open_epoch: 9,
                chain: vec![Link { epoch: 3, hash: [1; 32], parent: [0; 32], rank: 2 }],
                own_logs: vec![LogRef { node: NodeId(2), epoch: 3, hash: [4; 32] }],
            }),
            Msg::GetLogs(vec![[7; 32]]),
            Msg::Logs { logs: vec![vec![9; 10]], missing: vec![[8; 32]] },
            Msg::GetChain(3),
            Msg::Chain(vec![m.clone()]),
            Msg::GetImage,
            Msg::Image(BaseImage {
                epoch: Some(4),
                root: [6; 32],
                merged: vec![MergedRecord {
                    epoch: 4,
                    hash: m.hash(),
                    manifest: m,
                    base_before: [1; 32],
                    root: [6; 32],
                    undoable: false,
                }],
                merged_logs: vec![MergedLog { hash: [5; 32], merged_in: 4, node: NodeId(1), log_epoch: 4 }],
            }),
            Msg::GetTree(vec![[1; 32]]),
            Msg::Tree { root: [1; 32], nodes: vec![([2; 32], Some(vec![0; 5])), ([3; 32], None)] },
            Msg::GetRows(vec![row.row_id]),
            Msg::Rows {
                root: [1; 32],
                rows: vec![(row.row_id, Some((1, row.clone()))), (RowId::new(NodeId(2), 0), None)],
            },
        ];
        for msg in msgs {
            let bytes = msg.encode();
            assert_eq!(Msg::decode(&bytes).unwrap(), msg, "{}", msg.name());
            for cut in 0..bytes.len() {
                assert!(Msg::decode(&bytes[..cut]).is_err(), "{} cut at {cut}", msg.name());
            }
        }
        assert!(Msg::decode(&[MSG_VERSION, GET_LOGS, 0xFF, 0xFF, 0xFF, 0xFF]).is_err());
    }
}
