//! The report filter, read off a URL's query string, and the conditions it
//! writes into the Rayfall source sent to the statistics store. The store
//! takes writes, so nothing of the URL may reach that source but numbers
//! and a pool name of letters, digits, `-` and `_`.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rfbouncer::web::api::Filter;

/// Every word the conditions are made of, besides numbers and the pool.
const WORDS: [&str; 12] = [
    "and", ">=", "<=", "==", "minute", "ts", "pool", "as", "'SYMBOL", "fp", "kind", "'slow",
];

fn name(text: &str) -> bool {
    text.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn check(source: &str, f: &Filter) {
    let mut depth = 0i32;
    for (i, c) in source.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        // One expression: it closes at its last character and not before.
        assert!(depth > 0 || i == source.len() - 1, "{source}");
    }
    assert_eq!(depth, 0, "{source}");
    let mut pools = 0;
    for word in source.split(['(', ')', ' ']).filter(|w| !w.is_empty()) {
        if let Some(quoted) = word.strip_prefix('"') {
            let pool = quoted.strip_suffix('"').expect("a closed string");
            assert!(name(pool) && pool == f.pool && !pool.is_empty(), "{source}");
            pools += 1;
        } else {
            assert!(
                WORDS.contains(&word) || word.parse::<i64>().is_ok(),
                "{word:?} in {source}"
            );
        }
    }
    assert_eq!(pools, usize::from(!f.pool.is_empty()), "{source}");
}

fuzz_target!(|data: &[u8]| {
    // hyper hands the query over undecoded, as text; any text is tried.
    let query = String::from_utf8_lossy(data);
    let f = Filter::parse(&query);
    assert!(name(&f.pool));
    assert!(0 <= f.from && f.from <= f.to && f.to <= 4_102_444_800);
    assert!((1..=500).contains(&f.limit));
    // Each condition the reports build, with the arithmetic they do on the
    // range: none of it may overflow.
    check(&f.minutes(true), &f);
    check(&f.minutes(false), &f);
    let samples = f.condition(
        "ts",
        f.from * 1000,
        f.to * 1000 + 999,
        true,
        " (== kind 'slow)",
    );
    check(&samples, &f);
    let step = (((f.to - f.from) / 60 + 120) / 120).max(1);
    assert!(step >= 1);
});
