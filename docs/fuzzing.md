# Fuzzing

Everything that reads bytes a peer chooses has a coverage-guided fuzz target
under `fuzz/`, built with [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz)
on libFuzzer. The targets assert more than "it does not panic": each states
what must hold of whatever comes out.

| Target | Reads | Holds |
|---|---|---|
| `frame_header` | `Header::parse` | an accepted header has a size within the bound and encodes back to the same 16 bytes |
| `read_frame` | `client::read_frame` | frames are the stream's own bytes, the same however the stream is cut into reads |
| `greeting` | `read_hello`, `read_credentials`, `connect` | exactly the counted bytes are taken; a replica's answer is accepted or refused |
| `decompress` | `decompress`, `decompress_prefix`, `compress` | the start is the start of the whole and costs no more than was asked for; what is compressed is smaller and comes back whole |
| `status` | `wire::status::decode`, `Reading::after`, `load`, `rank` | a frame holds what it declares and encodes back; rates are shares, never negative or NaN |
| `value_decode` | `decode`, `error_code`, `Rows` | a table read is the table written again; cells past the end read as nothing |
| `request_text` | `pool::request_source`, `value::request_text` | at most `TEXT_CAP` bytes of text, compressed or not |
| `normalize` | `normalize_with`, symbols kept and taken for values | a shape is its own shape and no longer than twice its source |
| `filter` | `Filter::parse`, `minutes`, `condition` | the Rayfall conditions are made of fixed words, integers and a pool name of letters, digits, `-` and `_` |
| `admin` | `admin::run` | every command is answered with a table, `ok` or an error |
| `config` | the TOML, `validate`, `parse_duration`, `parse_size` | a configuration is refused or is safe to resolve credentials against |

## Running

Once:

```sh
rustup toolchain install nightly
cargo install cargo-fuzz
```

The crate itself stays on the toolchain in `rust-toolchain.toml`; only the
fuzz targets are built with nightly, which libFuzzer's instrumentation needs.

```sh
fuzz/run.sh                    # every target, 30 seconds each: what CI does
fuzz/run.sh 3600               # every target for an hour each
fuzz/run.sh 86400 value_decode # one target for a day
```

or one target by hand, on eight cores, until stopped:

```sh
cd fuzz
cargo +nightly fuzz run value_decode corpus/value_decode seeds/value_decode -- -jobs=8 -workers=8
```

`fuzz/seeds/<target>/` is in git: the byte vectors of the unit tests, most of
them read off a running Rayforce. `fuzz/corpus/<target>/` is not; it is where
a run keeps the inputs that reached new code, and the next run starts from
them. It grows by some megabytes a minute at first (the ten targets left
58 MB after four minutes each). Keep it between long runs, and now and then
shrink it:

```sh
cargo +nightly fuzz cmin value_decode corpus/value_decode
```

The targets are built with debug assertions and overflow checks on, so an
arithmetic overflow that a release build would let wrap is found as a crash.

## When a target fails

The input is written to `fuzz/artifacts/<target>/crash-<hash>`.

```sh
cd fuzz
cargo +nightly fuzz run value_decode artifacts/value_decode/crash-<hash>   # again
cargo +nightly fuzz tmin value_decode artifacts/value_decode/crash-<hash>  # smaller
```

Fix the cause, then keep the input from coming back: add it to
`fuzz/seeds/<target>/` under a name that says what it is, and write the case
as a test in `tests/properties.rs`, so that `cargo test` holds it too without
a fuzzer.

## Where the fuzzer looked

```sh
cd fuzz
cargo +nightly fuzz coverage value_decode corpus/value_decode
```

writes `fuzz/coverage/value_decode/coverage.profdata`, to be read with
`llvm-cov show`.

## Beside the fuzzers

- `tests/properties.rs` states invariants over generated inputs with
  `proptest` and runs on every `cargo test`; `PROPTEST_CASES=100000 cargo test
  --test properties` runs more of them.
- `tests/garbage.rs` feeds the same pseudo-random bytes to the parsers on
  every run. It finds nothing new; it keeps a toolchain without nightly from
  going wholly without.
- `node --test web/test/*.test.js` tests the page's script.
- `cargo llvm-cov --summary-only` (with `cargo install cargo-llvm-cov`)
  measures what the whole suite covers; CI prints it.
