//! What must hold for every input, checked on generated ones. A case that
//! fails is shrunk to a small one and printed; `PROPTEST_CASES=100000` runs
//! more of them.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use proptest::collection::vec;
use proptest::prelude::*;
use rfbouncer::config::{parse_duration, parse_size};
use rfbouncer::stats::normalize::{normalize, normalize_with};
use rfbouncer::stats::store::{BUCKETS, bucket, quantile};
use rfbouncer::wire::compress::{compress, decompress, decompress_prefix};
use rfbouncer::wire::value::{Column, Value, decode, table};

// ---- the normalizer ----

/// A query as a tree, so that the same query can be written twice with
/// other values and other spacing, or once more with one name changed.
#[derive(Debug, Clone)]
enum Node {
    Name(String),
    /// A value, by its kind: which value is chosen when it is written.
    Literal(u8),
    List(char, Vec<Node>),
}

fn name() -> impl Strategy<Value = String> {
    // Anything the lexer takes for a name or a symbol: it does not open
    // with a digit, and holds no bracket, quote, semicolon or blank.
    "[a-zA-Z'_.:+*<>=!/%&|^~][a-zA-Z0-9'_.:+*<>=!/%&|^~-]{0,7}"
}

fn node() -> impl Strategy<Value = Node> {
    let leaf = prop_oneof![
        name().prop_map(Node::Name),
        any::<u8>().prop_map(Node::Literal)
    ];
    leaf.prop_recursive(4, 40, 6, |inner| {
        (prop::sample::select(vec!['(', '[', '{']), vec(inner, 0..6))
            .prop_map(|(open, items)| Node::List(open, items))
    })
}

fn literal(kind: u8, seed: u64) -> String {
    match kind % 7 {
        0 => (seed % 100_000).to_string(),
        1 => format!("-{}", seed % 1000),
        2 => format!("{}.{}e{}", seed % 100, seed % 7, seed % 5),
        // A string with everything in it that would end a token outside one.
        3 => format!("\"v{seed} \\\" ) ] ; [ (\\\\\""),
        4 => format!("2026.10.{:02}D09:30:{:02}.000", seed % 28 + 1, seed % 60),
        5 => format!("{}i", seed % 9),
        _ => "0Nd".to_owned(),
    }
}

fn tokens(node: &Node, seeds: &mut impl Iterator<Item = u64>, out: &mut Vec<String>) {
    match node {
        Node::Name(name) => out.push(name.clone()),
        Node::Literal(kind) => out.push(literal(*kind, seeds.next().unwrap_or(0))),
        Node::List(open, items) => {
            out.push(open.to_string());
            items.iter().for_each(|item| tokens(item, seeds, out));
            out.push(
                match open {
                    '(' => ")",
                    '[' => "]",
                    _ => "}",
                }
                .to_owned(),
            );
        }
    }
}

/// The source of `node`, its values drawn from `seed` and the room between
/// its tokens from `spacing`: blanks, line ends, comments, or nothing where
/// a bracket makes that legal.
fn write(node: &Node, seed: u64, spacing: &[u8]) -> String {
    let mut seeds =
        (0..).map(|i: u64| seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(i));
    let mut all = Vec::new();
    tokens(node, &mut seeds, &mut all);
    let bracket = |t: &str| matches!(t, "(" | ")" | "[" | "]" | "{" | "}");
    let mut out = String::new();
    for (i, token) in all.iter().enumerate() {
        if i > 0 {
            let choice = spacing.get(i % spacing.len().max(1)).copied().unwrap_or(1);
            let tight = bracket(token) || bracket(&all[i - 1]);
            out.push_str(match choice % 6 {
                0 if tight => "",
                2 => "\n",
                3 => "  \t ",
                4 => " ; a comment ( \" [ 1\n",
                5 => "\r\n",
                _ => " ",
            });
        }
        out.push_str(token);
    }
    out
}

/// The tree with its `nth` name (counted from the left, wrapping) given
/// another spelling; `None` when it has no name.
fn rename(node: &Node, nth: usize) -> Option<Node> {
    fn count(node: &Node) -> usize {
        match node {
            Node::Name(_) => 1,
            Node::Literal(_) => 0,
            Node::List(_, items) => items.iter().map(count).sum(),
        }
    }
    fn at(node: &Node, left: &mut usize) -> Node {
        match node {
            Node::Name(name) => {
                let hit = *left == 0;
                *left = left.wrapping_sub(1);
                Node::Name(if hit {
                    format!("{name}x")
                } else {
                    name.clone()
                })
            }
            Node::Literal(kind) => Node::Literal(*kind),
            Node::List(open, items) => {
                Node::List(*open, items.iter().map(|i| at(i, left)).collect())
            }
        }
    }
    let names = count(node);
    (names > 0).then(|| at(node, &mut (nth % names)))
}

fn any_text() -> impl Strategy<Value = String> {
    prop_oneof![
        any::<String>(),
        // Mostly the bytes the normalizer branches on.
        "[()\\[\\]{}\";'\\\\ \n\t0-9a-z.?-]{0,80}",
    ]
}

proptest! {
    #[test]
    fn values_and_spacing_do_not_change_the_fingerprint(
        tree in node(),
        (seed_a, seed_b) in any::<(u64, u64)>(),
        (spacing_a, spacing_b) in (vec(any::<u8>(), 1..12), vec(any::<u8>(), 1..12)),
    ) {
        let a = write(&tree, seed_a, &spacing_a);
        let b = write(&tree, seed_b, &spacing_b);
        prop_assert_eq!(normalize(&a), normalize(&b), "{:?} and {:?}", a, b);
    }

    #[test]
    fn another_name_is_another_query(
        tree in node(),
        nth in any::<usize>(),
        seed in any::<u64>(),
        spacing in vec(any::<u8>(), 1..12),
    ) {
        let Some(other) = rename(&tree, nth) else { return Ok(()) };
        let (a, b) = (write(&tree, seed, &spacing), write(&other, seed, &spacing));
        let (a, b) = (normalize(&a), normalize(&b));
        prop_assert_ne!(&a.0, &b.0);
        prop_assert_ne!(a.1, b.1);
    }

    #[test]
    fn a_shape_is_its_own_shape_and_no_longer_than_its_source(text in any_text(), symbols in any::<bool>()) {
        let (normal, _) = normalize_with(&text, symbols);
        // A placeholder or a space for each byte of the source at the most.
        prop_assert!(normal.len() <= 2 * text.len(), "{:?} -> {:?}", text, normal);
        prop_assert_eq!(normalize_with(&normal, symbols).0, normal);
    }

    #[test]
    fn values_and_spacing_do_not_change_the_fingerprint_with_symbols_as_values(
        tree in node(),
        (seed_a, seed_b) in any::<(u64, u64)>(),
        (spacing_a, spacing_b) in (vec(any::<u8>(), 1..12), vec(any::<u8>(), 1..12)),
    ) {
        let a = write(&tree, seed_a, &spacing_a);
        let b = write(&tree, seed_b, &spacing_b);
        prop_assert_eq!(normalize_with(&a, true), normalize_with(&b, true), "{:?} and {:?}", a, b);
        // Taking symbols for values only ever takes more for a value.
        let (kept, taken) = (normalize(&a).0, normalize_with(&a, true).0);
        prop_assert!(taken.len() <= kept.len(), "{:?} -> {:?} and {:?}", a, kept, taken);
    }
}

// ---- compression ----

/// Payloads with stretches that repeat and stretches that climb evenly, as
/// vectors of numbers have, and plain noise.
fn payload() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        vec(any::<u8>(), 1..400),
        vec(prop::sample::select(vec![0u8, 0, 0, 0, 1, 8, 255]), 1..1500).prop_map(|deltas| {
            let mut at = 0u8;
            deltas
                .iter()
                .map(|d| {
                    at = at.wrapping_add(*d);
                    at
                })
                .collect()
        }),
    ]
}

proptest! {
    #[test]
    fn what_is_compressed_decompresses_to_itself(plain in payload()) {
        // The compressor declines what it cannot make smaller, as the
        // server does; what it takes must come back whole.
        let Some(wire) = compress(&plain) else { return Ok(()) };
        prop_assert!(wire.len() < plain.len());
        prop_assert_eq!(decompress(&wire, plain.len()), Ok(plain.clone()));
        // One byte under its size it is refused before anything is read.
        prop_assert!(decompress(&wire, plain.len() - 1).is_err());
    }

    #[test]
    fn what_repeats_is_made_smaller(byte in any::<u8>(), step in any::<u8>(), len in 16usize..3000) {
        // A constant stretch and an evenly climbing one are both runs of
        // one delta: about two bytes for every 127. Eight bytes come out
        // as eight and are declined, so the stretch starts longer.
        let plain: Vec<u8> = (0..len).map(|i| byte.wrapping_add(step.wrapping_mul(i as u8))).collect();
        let wire = compress(&plain).expect("runs compress");
        prop_assert!(wire.len() <= 4 + 2 + 2 * len.div_ceil(127), "{} bytes for {}", wire.len(), len);
        prop_assert_eq!(decompress(&wire, len), Ok(plain));
    }

    #[test]
    fn the_start_of_a_payload_is_the_start_of_the_whole(
        plain in payload(),
        limit in 0usize..2000,
        cut in any::<prop::sample::Index>(),
    ) {
        let Some(wire) = compress(&plain) else { return Ok(()) };
        let start = decompress_prefix(&wire, limit);
        prop_assert_eq!(&start[..], &plain[..limit.min(plain.len())]);
        // Broken off anywhere, it gives a start of the whole and no more.
        let part = decompress_prefix(&wire[..cut.index(wire.len() + 1)], limit);
        prop_assert!(plain.starts_with(&part));
    }

    #[test]
    fn any_bytes_decompress_or_are_refused(body in vec(any::<u8>(), 0..200), limit in 0usize..600) {
        let start = decompress_prefix(&body, limit);
        prop_assert!(start.len() <= limit);
        if let Ok(whole) = decompress(&body, 4096) {
            let n = limit.min(whole.len());
            prop_assert_eq!(&start[..n], &whole[..n]);
        }
    }
}

// ---- values ----

fn column(rows: usize) -> impl Strategy<Value = Column> {
    prop_oneof![
        vec(any::<i64>(), rows).prop_map(Column::I64),
        vec(any::<f64>(), rows).prop_map(Column::F64),
        // A symbol ends at a NUL, so it holds none.
        vec("[^\0]{0,12}", rows).prop_map(Column::Sym),
        vec(any::<String>(), rows).prop_map(Column::Str),
    ]
}

fn a_table() -> impl Strategy<Value = Vec<(String, Column)>> {
    (0usize..6).prop_flat_map(|rows| vec(("[^\0]{0,10}", column(rows)), 0..7))
}

/// The cells as bits: a NaN is then equal to itself.
fn bits(columns: &[(String, Column)]) -> Vec<(String, Vec<String>)> {
    columns
        .iter()
        .map(|(name, column)| {
            let cells = match column {
                Column::I64(v) => v.iter().map(|x| format!("i{x}")).collect(),
                Column::F64(v) => v.iter().map(|x| format!("f{:x}", x.to_bits())).collect(),
                Column::Sym(v) => v.iter().map(|x| format!("y{x}")).collect(),
                Column::Str(v) => v.iter().map(|x| format!("s{x}")).collect(),
            };
            (name.clone(), cells)
        })
        .collect()
}

proptest! {
    #[test]
    fn a_table_written_is_the_table_read(columns in a_table()) {
        let wire = table(&columns);
        let Ok(Value::Table(read)) = decode(&wire) else {
            return Err(TestCaseError::fail("a table that was written does not read back"));
        };
        prop_assert_eq!(bits(&read), bits(&columns));
        // And no shorter stretch of it reads as a value at all.
        for cut in 1..wire.len() {
            prop_assert!(decode(&wire[..cut]).is_err(), "cut at {} of {}", cut, wire.len());
        }
    }

    #[test]
    fn any_bytes_decode_or_are_refused(mut bytes in vec(any::<u8>(), 0..120), kind in any::<u8>()) {
        // Half of them open as one of the types the decoder knows.
        let types = [0x62, 0x7f, 0xf3, 0xf4, 0xfb, 0xf9];
        if let (Some(first), true) = (bytes.first_mut(), kind % 2 == 0) {
            *first = types[usize::from(kind / 2) % types.len()];
        }
        let _ = decode(&bytes);
    }
}

// ---- the histogram ----

proptest! {
    #[test]
    fn a_longer_request_never_falls_into_a_lower_bucket(a in any::<u64>(), b in any::<u64>()) {
        let (a, b) = (a.min(b), a.max(b));
        prop_assert!(bucket(a) <= bucket(b) && bucket(b) < BUCKETS);
    }

    #[test]
    fn a_bucket_holds_what_is_under_its_edge(micros in 0u64..(1 << (BUCKETS - 1))) {
        // Every bucket but the last, which takes whatever is longer.
        let i = bucket(micros);
        prop_assert!(micros < 1 << i);
        prop_assert!(i == 0 || micros >= 1 << (i - 1));
    }

    #[test]
    fn a_quantile_is_an_edge_among_the_buckets_that_hold_requests(
        durations in vec(any::<u64>().prop_map(|x| x >> (x % 64)), 1..200),
        (p, q) in (0.0f64..=1.0, 0.0f64..=1.0),
    ) {
        let mut hist = [0u64; BUCKETS];
        durations.iter().for_each(|d| hist[bucket(*d)] += 1);
        let lowest = durations.iter().map(|d| bucket(*d)).min().unwrap();
        let highest = durations.iter().map(|d| bucket(*d)).max().unwrap();
        let (p, q) = (p.min(q), p.max(q));
        let (at_p, at_q) = (quantile(&hist, p), quantile(&hist, q));
        prop_assert!(at_p <= at_q, "q{} = {} over q{} = {}", p, at_p, q, at_q);
        prop_assert!(1 << lowest <= at_p && at_q <= 1 << highest);
        prop_assert_eq!(quantile(&hist, 1.0), 1 << highest);
        // As many requests as the quantile says are at or under it.
        let under = durations.iter().filter(|d| 1u64 << bucket(**d) <= at_q).count();
        prop_assert!(under as f64 >= q * durations.len() as f64);
    }

    #[test]
    fn counts_of_any_size_have_a_quantile(hist in any::<[u64; BUCKETS]>(), q in 0.0f64..=1.0) {
        // The counts are read back from the store, which is another
        // process: sums of them must not be trusted to fit.
        let at = quantile(&hist, q);
        prop_assert!(at == 0 || at.is_power_of_two());
        prop_assert_eq!(at == 0, hist.iter().all(|n| *n == 0));
    }
}

// ---- durations and sizes in the configuration ----

proptest! {
    #[test]
    fn a_duration_is_a_number_and_a_unit(
        n in prop_oneof![any::<u64>(), 0u64..100_000],
        unit in prop::sample::select(vec![("ms", 1u64), ("s", 1000), ("m", 60_000), ("h", 3_600_000), ("d", 86_400_000)]),
        (before, between, after) in ("[ \t]{0,2}", "[ \t]{0,2}", "[ \t]{0,2}"),
    ) {
        let text = format!("{before}{n}{between}{}{after}", unit.0);
        let expected = n.checked_mul(unit.1).map(Duration::from_millis);
        prop_assert_eq!(parse_duration(&text), expected, "{:?}", text);
        // A number alone says nothing of its unit, unless it is zero.
        prop_assert_eq!(parse_duration(&n.to_string()), (n == 0).then_some(Duration::ZERO));
    }

    #[test]
    fn a_size_is_a_number_and_a_unit(
        n in prop_oneof![any::<u64>(), 0u64..100_000],
        unit in prop::sample::select(vec![("", 1u64), ("B", 1), ("KiB", 1 << 10), ("MiB", 1 << 20), ("GiB", 1 << 30)]),
        between in "[ \t]{0,2}",
    ) {
        let text = format!("{n}{between}{}", unit.0);
        let expected = n.checked_mul(unit.1).and_then(|bytes| usize::try_from(bytes).ok());
        prop_assert_eq!(parse_size(&text), expected, "{:?}", text);
    }

    #[test]
    fn any_text_is_a_duration_and_a_size_or_is_not(text in prop_oneof![any::<String>(), "[0-9]{0,22}[ a-zA-Z]{0,4}"]) {
        // What is taken is digits and then a unit, with nothing else.
        let digits = text.trim().trim_end_matches(|c: char| !c.is_ascii_digit());
        if parse_duration(&text).is_some() || parse_size(&text).is_some() {
            prop_assert!(!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()), "{:?}", text);
        }
    }
}

// ---- what the fuzzers found, kept from coming back ----

#[test]
fn an_error_cut_short_of_its_code_is_refused() {
    // Found by the `value_decode` target: the one byte `7f` was read as an
    // error with an empty code, while `error_code` said it was none.
    for cut in 1..9 {
        let short = &[0x7f, b'n', b'a', b'm', b'e', 0, 0, 0, 0][..cut];
        assert!(decode(short).is_err(), "{cut} bytes");
    }
    assert_eq!(
        decode(&[0x7f, b'n', b'a', b'm', b'e', 0, 0, 0, 0]),
        Ok(Value::Error("name".into(), String::new()))
    );
}

#[test]
fn counts_too_large_to_sum_still_have_a_quantile() {
    // Found by `counts_of_any_size_have_a_quantile`: the sum of the counts
    // overflowed, which stops a debug build and misleads a release one.
    let mut hist = [0u64; BUCKETS];
    hist[3] = u64::MAX;
    hist[9] = 2;
    assert_eq!(quantile(&hist, 0.5), 8);
    assert_eq!(quantile(&[u64::MAX; BUCKETS], 0.99), 1);
}

#[test]
fn a_closer_of_another_kind_does_not_end_a_vector() {
    // Found by `a_shape_is_its_own_shape_and_no_longer_than_its_source`
    // with symbols taken for values: the `}` closed the inner `[` for the
    // purpose of telling names from symbols, so `a` was kept as a name, and
    // the shape that came out was not its own shape.
    let (shape, _) = normalize_with("[\"\"[}]a", true);
    assert_eq!(shape, "[? [?] ?");
    assert_eq!(normalize_with(&shape, true).0, shape);
}

#[test]
fn blanks_that_become_one_space_do_not_make_a_character() {
    // Found by the `normalize` fuzz target: a symbol named by a backslash,
    // blanks, and a later quote. With the blanks brought down to one space
    // the shape read as the character `'\ '`, and was not its own shape.
    for symbols in [false, true] {
        let (shape, _) = normalize_with("v '\\\r\r\r'\\\r\r x", symbols);
        assert_eq!(normalize_with(&shape, symbols).0, shape, "{shape:?}");
    }
    assert_eq!(normalize("(f '\\  '\\ y)").0, "(f '\\ '\\ y)");
}
