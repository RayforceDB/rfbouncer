//! The configuration file. It is the operator's, but it is read again on
//! every SIGHUP by a process that is serving: any text must be accepted or
//! refused, and one that is accepted must be safe to serve with.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rfbouncer::config::{Config, parse_duration, parse_size};

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let _ = (parse_duration(text), parse_size(text));
    let Ok(config) = toml::from_str::<Config>(text) else {
        return;
    };
    if config.validate().is_err() {
        return;
    }
    // What the request path asks of a configuration in force.
    assert!(!config.only_pool().backends.is_empty());
    let _ = config.auth_required();
    for version in [3, 4, 9] {
        let _ = config.speaks(version);
        for user in ["", "rfbouncer", "app"] {
            let _ = config.resolve(user, b"pw", version);
        }
        for pool in &config.pool {
            assert!(config.pool(&pool.name).is_some());
            for user in &pool.users {
                let _ = config.resolve(
                    user,
                    pool.password.as_deref().unwrap_or("").as_bytes(),
                    version,
                );
            }
        }
    }
});
