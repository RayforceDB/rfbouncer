//! The value decoder: it reads what the statistics store answers, and an
//! answer is bytes from another process.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rfbouncer::stats::store::Rows;
use rfbouncer::wire::value::{Column, Value, decode, error_code, table};

fn rows(column: &Column) -> usize {
    match column {
        Column::I64(v) => v.len(),
        Column::F64(v) => v.len(),
        Column::Sym(v) | Column::Str(v) => v.len(),
    }
}

fuzz_target!(|data: &[u8]| {
    let code = error_code(data);
    match decode(data) {
        Ok(Value::Error(c, _)) => assert_eq!(Some(c), code),
        Ok(Value::Table(columns)) => {
            assert!(code.is_none());
            // Nothing decoded can hold more than the bytes it came from.
            assert!(columns.iter().map(|(_, c)| rows(c)).sum::<usize>() <= data.len());
            // Written again and read again it is the same table. Compared
            // as bytes, since a NaN is not equal to itself. The encoder
            // asks for columns of one length, which the decoder does not
            // check, so only such a table is written back.
            let even = columns.windows(2).all(|w| rows(&w[0].1) == rows(&w[1].1));
            if even {
                let written = table(&columns);
                let Ok(Value::Table(again)) = decode(&written) else {
                    panic!("a table that was written does not read back");
                };
                assert_eq!(table(&again), written);
            }
            // The reports read cells by name and row, past the end too.
            let names: Vec<String> = columns.iter().map(|(n, _)| n.clone()).collect();
            let rows = Rows::of(Value::Table(columns)).expect("a table");
            for name in names.iter().map(String::as_str).chain(["missing"]) {
                for row in [0, rows.len() / 2, rows.len(), usize::MAX] {
                    let _ = (rows.int(name, row), rows.text(name, row));
                }
            }
        }
        Ok(_) => assert!(code.is_none()),
        Err(_) => {}
    }
});
