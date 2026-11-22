//! The few serialized values the bouncer reads or writes itself, in the
//! layout of rayforce's `src/store/serde.c`. Everything else passes through
//! as bytes. Every layout here is pinned by bytes read off a real server.

use super::compress;
use super::frame::{FLAG_COMPRESSED, PAYLOAD_MAX};

/// A string atom: `RAY_STR` (13) negated, a flags byte, an i64 length, bytes.
const TYPE_STR_ATOM: u8 = 0xF3;
const TYPE_ERROR: u8 = 127;
const TYPE_LIST: u8 = 0;
const TYPE_I64: u8 = 5;
const TYPE_F64: u8 = 7;
const TYPE_SYM: u8 = 12;
const TYPE_STR: u8 = 13;
const TYPE_TABLE: u8 = 98;
/// The attrs byte the server writes on a table's schema names.
const SCHEMA_ATTRS: u8 = 0x03;

pub fn str_atom(text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(10 + text.len());
    out.push(TYPE_STR_ATOM);
    out.push(0);
    out.extend_from_slice(&(text.len() as i64).to_le_bytes());
    out.extend_from_slice(text.as_bytes());
    out
}

/// The text of a request payload when it is a string atom, which is how a
/// client sends source to be parsed and evaluated. `None` for anything else.
pub fn request_text(flags: u8, payload: &[u8]) -> Option<String> {
    let plain;
    let payload = if flags & FLAG_COMPRESSED != 0 {
        plain = compress::decompress(payload, PAYLOAD_MAX).ok()?;
        &plain[..]
    } else {
        payload
    };
    let (head, rest) = payload.split_first_chunk::<10>()?;
    if head[0] != TYPE_STR_ATOM {
        return None;
    }
    let mut len = [0u8; 8];
    len.copy_from_slice(&head[2..]);
    let len = usize::try_from(i64::from_le_bytes(len)).ok()?;
    let text = rest.get(..len)?;
    Some(String::from_utf8_lossy(text).into_owned())
}

/// An error object. Wire version 3 carries the 8-byte code alone; version 4
/// adds a length byte and up to 255 bytes of message.
pub fn error(version: u8, code: &str, msg: &str) -> Vec<u8> {
    let mut out = vec![TYPE_ERROR];
    let mut packed = [0u8; 8];
    let n = code.len().min(7);
    packed[..n].copy_from_slice(&code.as_bytes()[..n]);
    out.extend_from_slice(&packed);
    if version >= 4 {
        let msg = &msg.as_bytes()[..msg.len().min(255)];
        out.push(msg.len() as u8);
        out.extend_from_slice(msg);
    }
    out
}

/// The code of an error payload, or `None` when the payload is a value.
pub fn error_code(payload: &[u8]) -> Option<String> {
    if payload.first() != Some(&TYPE_ERROR) {
        return None;
    }
    let code = payload.get(1..9)?;
    let code = code.split(|b| *b == 0).next().unwrap_or_default();
    Some(String::from_utf8_lossy(code).into_owned())
}

#[derive(Debug, PartialEq)]
pub enum Column {
    I64(Vec<i64>),
    F64(Vec<f64>),
    Sym(Vec<String>),
    Str(Vec<String>),
}

impl Column {
    fn len(&self) -> usize {
        match self {
            Self::I64(v) => v.len(),
            Self::F64(v) => v.len(),
            Self::Sym(v) | Self::Str(v) => v.len(),
        }
    }
}

/// A table: the schema names as a symbol vector, then a list of columns.
/// Columns must be the same length; names must be free of NUL.
pub fn table<S: AsRef<str>>(columns: &[(S, Column)]) -> Vec<u8> {
    let mut out = vec![TYPE_TABLE, 0];
    vec_head(&mut out, TYPE_SYM, SCHEMA_ATTRS, columns.len());
    for (name, _) in columns {
        sym_cell(&mut out, name.as_ref());
    }
    vec_head(&mut out, TYPE_LIST, 0, columns.len());
    for (_, column) in columns {
        match column {
            Column::I64(v) => {
                vec_head(&mut out, TYPE_I64, 0, v.len());
                v.iter()
                    .for_each(|x| out.extend_from_slice(&x.to_le_bytes()));
            }
            Column::F64(v) => {
                vec_head(&mut out, TYPE_F64, 0, v.len());
                v.iter()
                    .for_each(|x| out.extend_from_slice(&x.to_le_bytes()));
            }
            Column::Sym(v) => {
                vec_head(&mut out, TYPE_SYM, 0, v.len());
                v.iter().for_each(|x| sym_cell(&mut out, x));
            }
            Column::Str(v) => {
                vec_head(&mut out, TYPE_STR, 0, v.len());
                for x in v {
                    out.extend_from_slice(&(x.len() as i64).to_le_bytes());
                    out.extend_from_slice(x.as_bytes());
                }
            }
        }
        debug_assert_eq!(column.len(), columns[0].1.len());
    }
    out
}

fn vec_head(out: &mut Vec<u8>, ty: u8, attrs: u8, len: usize) {
    out.push(ty);
    out.push(attrs);
    out.extend_from_slice(&(len as i64).to_le_bytes());
}

/// A symbol cell is NUL-terminated, so a NUL inside would end it early.
fn sym_cell(out: &mut Vec<u8>, text: &str) {
    out.extend(text.bytes().filter(|b| *b != 0));
    out.push(0);
}

/// A call with a table as its last argument, as the server would parse it
/// from source: `(function 'target keys table)`. Sent as a value and not as
/// text, so nothing in the table's cells can be read as code.
pub fn call_with_table(function: &str, target: &str, keys: i64, table: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(table.len() + 64);
    vec_head(&mut out, TYPE_LIST, 0, 4);
    for (attrs, name) in [(0, function), (ATTR_QUOTED, target)] {
        out.push(TYPE_SYM_ATOM);
        out.push(attrs);
        sym_cell(&mut out, name);
    }
    out.push(TYPE_I64_ATOM);
    out.push(0);
    out.extend_from_slice(&keys.to_le_bytes());
    out.extend_from_slice(table);
    out
}

const TYPE_SYM_ATOM: u8 = 0xF4;
const TYPE_I64_ATOM: u8 = 0xFB;
const TYPE_F64_ATOM: u8 = 0xF9;
/// On a symbol atom: a literal symbol (`'name`), not a name to look up.
const ATTR_QUOTED: u8 = 0x20;

/// A value read back from a server, as far as the statistics need one.
#[derive(Debug, PartialEq)]
pub enum Value {
    I64(i64),
    F64(f64),
    Text(String),
    Table(Vec<(String, Column)>),
    Error(String, String),
    /// Anything else. It was read without complaint and is not looked at.
    Other,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("cannot read the server's answer: {0}")]
pub struct DecodeError(&'static str);

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.0.len() < n {
            return Err(DecodeError("it ends early"));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn i64(&mut self) -> Result<i64, DecodeError> {
        let mut raw = [0u8; 8];
        raw.copy_from_slice(self.take(8)?);
        Ok(i64::from_le_bytes(raw))
    }

    /// A length that the bytes left could actually hold, at `each` bytes an
    /// element, so a wild number never becomes an allocation.
    fn len(&mut self, each: usize) -> Result<usize, DecodeError> {
        let n = usize::try_from(self.i64()?).map_err(|_| DecodeError("a negative length"))?;
        if n.checked_mul(each).is_none_or(|bytes| bytes > self.0.len()) {
            return Err(DecodeError("a length past the end"));
        }
        Ok(n)
    }

    fn cstr(&mut self) -> Result<String, DecodeError> {
        let end = self
            .0
            .iter()
            .position(|b| *b == 0)
            .ok_or(DecodeError("an unterminated symbol"))?;
        let text = String::from_utf8_lossy(self.take(end)?).into_owned();
        self.take(1)?;
        Ok(text)
    }

    fn column(&mut self) -> Result<Column, DecodeError> {
        let ty = self.u8()?;
        self.u8()?;
        Ok(match ty {
            TYPE_I64 => {
                let n = self.len(8)?;
                Column::I64((0..n).map(|_| self.i64()).collect::<Result<_, _>>()?)
            }
            TYPE_F64 => {
                let n = self.len(8)?;
                Column::F64(
                    (0..n)
                        .map(|_| self.i64().map(|bits| f64::from_bits(bits as u64)))
                        .collect::<Result<_, _>>()?,
                )
            }
            TYPE_SYM => {
                let n = self.len(1)?;
                Column::Sym((0..n).map(|_| self.cstr()).collect::<Result<_, _>>()?)
            }
            TYPE_STR => {
                let n = self.len(8)?;
                let cell = |r: &mut Self| {
                    let len = r.len(1)?;
                    Ok(String::from_utf8_lossy(r.take(len)?).into_owned())
                };
                Column::Str((0..n).map(|_| cell(self)).collect::<Result<_, _>>()?)
            }
            // A column nothing was ever put in has no type yet.
            TYPE_LIST if self.len(0)? == 0 => Column::Str(Vec::new()),
            _ => return Err(DecodeError("a column of a type the statistics do not use")),
        })
    }
}

pub fn decode(payload: &[u8]) -> Result<Value, DecodeError> {
    let mut r = Reader(payload);
    Ok(match r.u8()? {
        TYPE_ERROR => {
            let code = error_code(payload).unwrap_or_default();
            let msg = payload
                .get(10..)
                .map(|m| String::from_utf8_lossy(m).into_owned())
                .unwrap_or_default();
            Value::Error(code, msg)
        }
        TYPE_I64_ATOM => {
            r.u8()?;
            Value::I64(r.i64()?)
        }
        TYPE_F64_ATOM => {
            r.u8()?;
            Value::F64(f64::from_bits(r.i64()? as u64))
        }
        TYPE_STR_ATOM => {
            r.u8()?;
            let len = r.len(1)?;
            Value::Text(String::from_utf8_lossy(r.take(len)?).into_owned())
        }
        TYPE_SYM_ATOM => {
            r.u8()?;
            Value::Text(r.cstr()?)
        }
        TYPE_TABLE => {
            r.u8()?;
            let Column::Sym(names) = r.column()? else {
                return Err(DecodeError("a table without column names"));
            };
            if (r.u8()?, r.u8()?).0 != TYPE_LIST || r.len(2)? != names.len() {
                return Err(DecodeError("a table whose columns do not match its names"));
            }
            let columns = names
                .iter()
                .map(|_| r.column())
                .collect::<Result<Vec<_>, _>>()?;
            Value::Table(names.into_iter().zip(columns).collect())
        }
        _ => Value::Other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    // Every expected byte string below was read off a rayforce 3.0.2 process
    // (wire version 4) with a raw socket.

    #[test]
    fn a_string_atom_is_what_the_server_sends_for_one() {
        assert_eq!(str_atom("hi"), hex("f30002000000000000006869"));
        assert_eq!(request_text(0, &str_atom("(+ 1 2)")).unwrap(), "(+ 1 2)");
        assert_eq!(request_text(0, &hex("fb000300000000000000")), None);
        assert_eq!(request_text(0, &hex("f300ff00000000000000")), None);
    }

    #[test]
    fn an_error_is_what_the_server_sends_for_one() {
        assert_eq!(
            error(4, "access", "restricted"),
            hex("7f61636365737300000a72657374726963746564")
        );
        assert_eq!(error(4, "cancel", ""), hex("7f63616e63656c000000"));
        // Version 3 ends at the code: a type byte and eight bytes.
        assert_eq!(error(3, "cancel", "ignored"), hex("7f63616e63656c0000"));
        assert_eq!(
            error_code(&hex(
                "7f6e616d650000000012276e6f737563682720756e646566696e6564"
            )),
            Some("name".into())
        );
        assert_eq!(error_code(&hex("fb000300000000000000")), None);
    }

    #[test]
    fn a_table_is_what_the_server_sends_for_one() {
        let t = table(&[
            ("a", Column::I64(vec![1, 2])),
            ("b", Column::Sym(vec!["x".into(), "y".into()])),
        ]);
        assert_eq!(
            t,
            hex(concat!(
                "62000c030200000000000000610062000000020000000000000005000200000000000000",
                "010000000000000002000000000000000c00020000000000000078007900"
            ))
        );
        let t = table(&[
            ("a", Column::I64(vec![1, 2])),
            ("s", Column::Str(vec!["ab".into(), "c".into()])),
        ]);
        assert_eq!(
            t,
            hex(concat!(
                "62000c030200000000000000610073000000020000000000000005000200000000000000",
                "010000000000000002000000000000000d0002000000000000000200000000000000",
                "6162010000000000000063"
            ))
        );
    }

    #[test]
    fn what_is_written_is_read_back() {
        let columns = vec![
            ("minute".to_owned(), Column::I64(vec![1, -2])),
            (
                "pool".to_owned(),
                Column::Sym(vec!["127.0.0.1:5".into(), "b c".into()]),
            ),
            (
                "text".to_owned(),
                Column::Str(vec!["(+ \"x\" 1)".into(), String::new()]),
            ),
            ("avg".to_owned(), Column::F64(vec![15.5, 0.0])),
        ];
        assert_eq!(decode(&table(&columns)), Ok(Value::Table(columns)));
    }

    #[test]
    fn a_servers_answers_are_read() {
        // `(select {from: t a: (avg n) m: (max n)})` off a rayforce 3.0.2.
        let answer = hex(concat!(
            "62000c03020000000000000061006d0000000200000000000000070001000000000000000000000000002f40",
            "050001000000000000001500000000000000"
        ));
        let expected = vec![
            ("a".to_owned(), Column::F64(vec![15.5])),
            ("m".to_owned(), Column::I64(vec![21])),
        ];
        assert_eq!(decode(&answer), Ok(Value::Table(expected)));
        // An empty grouped result: the symbol column carries attrs.
        let empty = hex(
            "62000c030200000000000000706f6f6c006e00000002000000000000000c40000000000000000005000000000000000000",
        );
        let expected = vec![
            ("pool".to_owned(), Column::Sym(vec![])),
            ("n".to_owned(), Column::I64(vec![])),
        ];
        assert_eq!(decode(&empty), Ok(Value::Table(expected)));
        assert_eq!(decode(&hex("fb000200000000000000")), Ok(Value::I64(2)));
        assert_eq!(decode(&hex("f4007400")), Ok(Value::Text("t".into())));
        assert_eq!(
            decode(&hex(
                "7f6e616d650000000012276e6f737563682720756e646566696e6564"
            )),
            Ok(Value::Error("name".into(), "'nosuch' undefined".into()))
        );
    }

    #[test]
    fn a_broken_answer_is_refused() {
        let good = table(&[
            ("a", Column::I64(vec![1, 2])),
            ("s", Column::Str(vec!["ab".into(), "c".into()])),
        ]);
        for cut in 1..good.len() {
            assert!(decode(&good[..cut]).is_err(), "cut at {cut}");
        }
        let mut wild = good.clone();
        wild[4..12].copy_from_slice(&i64::MAX.to_le_bytes());
        assert!(decode(&wild).is_err());
    }

    #[test]
    fn a_call_is_what_the_server_parses_from_source() {
        let t = table(&[("a", Column::I64(vec![1]))]);
        let call = call_with_table("upsert", "t", 2, &t);
        let head = hex("00000400000000000000f40075707365727400f4207400fb000200000000000000");
        assert_eq!(&call[..head.len()], &head[..]);
        assert_eq!(&call[head.len()..], &t[..]);
    }
}
