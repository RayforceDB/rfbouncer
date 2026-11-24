//! Turns a request's source into the shape it shares with every request
//! that differs only in its values, so they are counted as one query.
//!
//! Follows the lexer in rayforce's `src/lang/parse.c`: a string is `"..."`
//! with backslash escapes, a comment runs from `;` to the end of the line,
//! and a token that opens with a digit, or a minus and a digit, is a number,
//! date, time, timestamp or typed null. Those become `?`. Names stay, and so
//! do symbols (`'trades`, `:key`): they are mostly tables and columns, and
//! the text cannot tell one that is a value from one that is not.
//!
//! A workload that asks for thousands of symbols by name then has thousands
//! of shapes, so `normalize_with` can be told to take symbols for values
//! too. The same lexer says what a symbol is: `'name` (and `'c'`, a
//! character), `:name` where an expression is expected, and every bare name
//! inside `[ ]`, which the parser marks as a literal symbol. What stays is
//! what its place shows to be part of the query and not a value in it: the
//! table after `from:`, a `fn`'s parameters, and the columns after `by:`,
//! `asc:` and `desc:`.

/// The normalized text and its fingerprint, symbols kept.
pub fn normalize(source: &str) -> (String, u64) {
    normalize_with(source, false)
}

/// As `normalize`; with `symbols` set, symbols become `?` as well.
pub fn normalize_with(source: &str, symbols: bool) -> (String, u64) {
    let bytes = source.as_bytes();
    let mut out = String::with_capacity(source.len().min(4096));
    // Where each open `[` starts in `out`, whether all it holds so far is
    // values, and whether a name in it is one.
    let mut vectors: Vec<(usize, bool, bool)> = Vec::new();
    // The brackets open at this point, innermost last.
    let mut open: Vec<u8> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b' ' | b'\t' | b'\n' | b'\r' => i += 1,
            b';' => i = skip_line(bytes, i),
            b'(' | b'[' | b'{' => {
                let names_are_values =
                    symbols && !matches!(last_token(&out), "fn" | "by:" | "asc:" | "desc:");
                glue(&mut out);
                if let Some(v) = vectors.last_mut() {
                    v.1 = false;
                }
                out.push(c as char);
                if c == b'[' {
                    vectors.push((out.len(), true, names_are_values));
                }
                open.push(c);
                i += 1;
            }
            b')' | b']' | b'}' => {
                open.pop();
                if c == b']'
                    && let Some((start, only_values, _)) = vectors.pop()
                    && only_values
                    && out.len() > start
                {
                    out.truncate(start);
                    out.push('?');
                }
                out.push(c as char);
                i += 1;
            }
            b'"' => {
                i = skip_string(bytes, i);
                value(&mut out);
            }
            _ => {
                let end = token_end(bytes, i);
                let number = c.is_ascii_digit()
                    || (c == b'-' && bytes.get(i + 1).is_some_and(u8::is_ascii_digit));
                let symbol = symbols
                    && match c {
                        b'\'' => last_token(&out) != "from:",
                        // In `{from :t}` the colon is the key's own, written
                        // apart from it, and `t` is a name.
                        b':' => end > i + 1 && (open.last() != Some(&b'{') || out.ends_with(':')),
                        _ => open.last() == Some(&b'[') && vectors.last().is_some_and(|v| v.2),
                    };
                if number || symbol {
                    value(&mut out);
                } else {
                    glue(&mut out);
                    if let Some(v) = vectors.last_mut() {
                        v.1 = false;
                    }
                    out.push_str(&String::from_utf8_lossy(&bytes[i..end]));
                }
                i = end;
            }
        }
    }
    let fingerprint = fnv1a(out.as_bytes());
    (out, fingerprint)
}

/// A space before the next token, unless it opens the text or a bracket.
fn glue(out: &mut String) {
    if !matches!(out.as_bytes().last(), None | Some(b'(' | b'[' | b'{')) {
        out.push(' ');
    }
}

/// The token `out` ends with.
fn last_token(out: &str) -> &str {
    out.rsplit([' ', '(', '[', '{']).next().unwrap_or_default()
}

fn value(out: &mut String) {
    glue(out);
    out.push('?');
}

fn skip_line(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }
    i
}

/// The index past the closing quote, or the end of an unterminated string.
fn skip_string(bytes: &[u8], mut i: usize) -> usize {
    i += 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    bytes.len()
}

fn token_end(bytes: &[u8], mut i: usize) -> usize {
    i += 1;
    while i < bytes.len()
        && !matches!(
            bytes[i],
            b' ' | b'\t' | b'\n' | b'\r' | b'(' | b')' | b'[' | b']' | b'{' | b'}' | b'"' | b';'
        )
    {
        i += 1;
    }
    i
}

/// FNV-1a: the fingerprint is stored, so it has to be the same in every
/// build, which the standard library's hasher does not promise.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(source: &str) -> String {
        normalize(source).0
    }

    #[test]
    fn values_become_placeholders_and_names_stay() {
        assert_eq!(n("(+ 1 2)"), "(+ ? ?)");
        assert_eq!(
            n("(select {from: trades where: (and (== sym 'AAPL) (> price 101.5))})"),
            "(select {from: trades where: (and (== sym 'AAPL) (> price ?))})"
        );
        assert_eq!(n("(f \"a \\\" ) ; b\" -3 x-1 - 0Nd)"), "(f ? ? x-1 - ?)");
        assert_eq!(
            n("(within ts [2026.10.20D09:30:00.000 2026.10.20D16:00:00.000])"),
            "(within ts [?])"
        );
    }

    #[test]
    fn requests_that_differ_only_in_values_share_a_fingerprint() {
        let a = normalize("(select {from: t where: (in id [1 2 3])})  ; first");
        let b = normalize("(select {from:   t\n where: (in id [7 8 9 10 11])})");
        assert_eq!(a, b);
        assert_eq!(a.0, "(select {from: t where: (in id [?])})");
        assert_ne!(a.1, normalize("(select {from: u where: (in id [1])})").1);
    }

    #[test]
    fn a_vector_of_names_is_kept() {
        assert_eq!(
            n("(table [a b] (list [1 2] [x y]))"),
            "(table [a b] (list [?] [x y]))"
        );
        assert_eq!(n("[[1 2] [3]]"), "[[?] [?]]");
        assert_eq!(n("[]"), "[]");
        assert_eq!(n("[1 x 2]"), "[? x ?]");
    }

    fn s(source: &str) -> String {
        normalize_with(source, true).0
    }

    #[test]
    fn symbols_become_placeholders_when_asked() {
        let a = "(select {from: trades where: (and (== sym 'AAPL) (> price 101.5))})";
        let b = "(select {from: trades where: (and (== sym 'MSFT) (> price 99))})";
        assert_eq!(
            s(a),
            "(select {from: trades where: (and (== sym ?) (> price ?))})"
        );
        assert_eq!(normalize_with(a, true), normalize_with(b, true));
        assert_ne!(normalize(a).1, normalize(b).1);
        // A keyword is a symbol, and so is a name in a vector.
        assert_eq!(
            s("(select {from: orders where: (and (== side :buy) (in sym [AAPL MSFT GOOG]))})"),
            "(select {from: orders where: (and (== side ?) (in sym [?]))})"
        );
        assert_eq!(s("(in sym ['AAPL 'MSFT])"), s("(in sym [IBM])"));
        assert_eq!(s("(split name ',')"), "(split name ?)");
        assert_eq!(s("[1 x 2]"), "[?]");
    }

    #[test]
    fn what_names_the_query_stays_when_symbols_do_not() {
        // The table read, however it is written.
        assert_eq!(
            s("(select {from: 'trades where: (== sym 'AAPL) take: 10})"),
            "(select {from: 'trades where: (== sym ?) take: ?})"
        );
        assert_ne!(s("(select {from: 'trades})"), s("(select {from: 'quotes})"));
        // The columns grouped and sorted by, and a function's parameters.
        assert_eq!(
            s(
                "(select {from: trades by: [sym side] desc: [size] v: (sum size) where: (in sym [AAPL])})"
            ),
            "(select {from: trades by: [sym side] desc: [size] v: (sum size) where: (in sym [?])})"
        );
        assert_eq!(
            s("(map (fn [x y] (+ x y)) [a b] [1 2])"),
            "(map (fn [x y] (+ x y)) [?] [?])"
        );
        // A key's colon written apart from it does not make a keyword.
        assert_eq!(s("{from :trades}"), "{from :trades}");
        assert_eq!(s("{side: :buy}"), "{side: ?}");
        assert_eq!(s("(f : 'x)"), "(f : ?)");
    }

    #[test]
    fn broken_source_does_not_break_the_normalizer() {
        for broken in [
            "(f \"unterminated",
            "]]) \"\\",
            "",
            "; only a comment",
            "[ ( ] ' : :a 'b",
        ] {
            let _ = s(broken);
        }
        assert_eq!(n("(f \"unterminated"), "(f ?");
        assert_eq!(n("]]) \"\\"), "]]) ?");
        assert_eq!(n(""), "");
        assert_eq!(n("; only a comment"), "");
    }
}
