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
pub fn table(columns: &[(&str, Column)]) -> Vec<u8> {
    let mut out = vec![TYPE_TABLE, 0];
    vec_head(&mut out, TYPE_SYM, SCHEMA_ATTRS, columns.len());
    for (name, _) in columns {
        sym_cell(&mut out, name);
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
}
