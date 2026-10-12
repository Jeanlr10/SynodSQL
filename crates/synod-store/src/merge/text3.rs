//! Line-level three-way text merge (the `text3` policy).
//!
//! `merge3(base, ours, theirs)` keeps each region that only one side
//! changed, and keeps `ours` where both sides changed the same region in
//! different ways. Lines keep their line ends, so text without a final
//! newline round-trips.

/// Longest-common-subsequence size above which the merge does not align
/// lines and treats the whole text as one region.
const MAX_CELLS: usize = 4_000_000;

fn lines(s: &str) -> Vec<&str> {
    s.split_inclusive('\n').collect()
}

// For each line of `a`, the index of its match in `b`, from a longest
// common subsequence. Ties go to the earliest match, so the result is
// deterministic.
fn matches(a: &[&str], b: &[&str]) -> Option<Vec<Option<usize>>> {
    if a.len().saturating_mul(b.len()) > MAX_CELLS {
        return None;
    }
    let (n, m) = (a.len(), b.len());
    let mut len = vec![0u32; (n + 1) * (m + 1)];
    let at = |i: usize, j: usize| i * (m + 1) + j;
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            len[at(i, j)] =
                if a[i] == b[j] { len[at(i + 1, j + 1)] + 1 } else { len[at(i + 1, j)].max(len[at(i, j + 1)]) };
        }
    }
    let mut out = vec![None; n];
    let (mut i, mut j) = (0, 0);
    while i < n && j < m {
        if a[i] == b[j] {
            out[i] = Some(j);
            i += 1;
            j += 1;
        } else if len[at(i + 1, j)] >= len[at(i, j + 1)] {
            i += 1;
        } else {
            j += 1;
        }
    }
    Some(out)
}

/// Merges two changes of `base`. Returns the result and true if both sides
/// changed one region in different ways (the result then keeps `ours` in
/// that region).
pub(crate) fn merge3(base: &str, ours: &str, theirs: &str) -> (String, bool) {
    if ours == theirs || theirs == base {
        return (ours.to_owned(), false);
    }
    if ours == base {
        return (theirs.to_owned(), false);
    }
    let (b, o, t) = (lines(base), lines(ours), lines(theirs));
    let (Some(mo), Some(mt)) = (matches(&b, &o), matches(&b, &t)) else {
        return (ours.to_owned(), true);
    };
    let mut out = String::new();
    let mut clash = false;
    let (mut ib, mut io, mut it) = (0, 0, 0);
    loop {
        // The next base line that both sides kept: a stable line.
        let next = (ib..b.len()).find(|&k| matches!((mo[k], mt[k]), (Some(x), Some(y)) if x >= io && y >= it));
        let (kb, ko, kt) = match next {
            Some(k) => (k, mo[k].unwrap(), mt[k].unwrap()),
            None => (b.len(), o.len(), t.len()),
        };
        let (bb, oo, tt) = (&b[ib..kb], &o[io..ko], &t[it..kt]);
        if oo == bb {
            out.extend(tt.iter().copied());
        } else if tt == bb || oo == tt {
            out.extend(oo.iter().copied());
        } else {
            clash = true;
            out.extend(oo.iter().copied());
        }
        if next.is_none() {
            break;
        }
        out.push_str(b[kb]);
        (ib, io, it) = (kb + 1, ko + 1, kt + 1);
    }
    (out, clash)
}

#[cfg(test)]
mod tests {
    use super::merge3;

    #[test]
    fn cases() {
        let base = "a\nb\nc\nd\n";
        assert_eq!(merge3(base, "A\nb\nc\nd\n", "a\nb\nc\nD\n"), ("A\nb\nc\nD\n".into(), false));
        assert_eq!(merge3(base, "a\nb\nX\nc\nd\n", "a\nb\nc\nd\ne\n"), ("a\nb\nX\nc\nd\ne\n".into(), false));
        assert_eq!(merge3(base, "a\nc\nd\n", "a\nb\nc\nd\nZ\n"), ("a\nc\nd\nZ\n".into(), false));
        assert_eq!(merge3(base, "a\nB1\nc\nd\n", "a\nB2\nc\nd\n"), ("a\nB1\nc\nd\n".into(), true));
        assert_eq!(merge3(base, "a\nB\nc\nd\n", "a\nB\nc\nd\n"), ("a\nB\nc\nd\n".into(), false));
        // Changes to adjacent lines, with no unchanged line between them,
        // overlap (as in diff3 and git). No final newline is needed.
        assert_eq!(merge3("x\ny", "X\ny", "x\nY"), ("X\ny".into(), true));
        assert_eq!(merge3("x\n-\ny", "X\n-\ny", "x\n-\nY"), ("X\n-\nY".into(), false));
        assert_eq!(merge3("", "a\n", "b\n"), ("a\n".into(), true));
    }
}
