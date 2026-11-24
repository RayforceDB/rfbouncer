//! The admin console's commands. They come from a peer that knows the admin
//! password, but any text must be answered with a table, `ok` or an error.
#![no_main]

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use libfuzzer_sys::fuzz_target;
use rfbouncer::admin;
use rfbouncer::config::Config;
use rfbouncer::core::Core;
use rfbouncer::stats::Stats;
use rfbouncer::wire::value::{Value, decode};

// Nothing listens on these ports: the replicas stay down, which is all a
// command needs. `reload` finds no file and says so.
const CONFIG: &str = r#"
listen = "127.0.0.1:37990"
admin_password = "root"
[[pool]]
name = "main"
users = ["app"]
backends = ["127.0.0.1:37991"]
connect_timeout = "1s"
[[pool]]
name = "reports"
default = true
backends = ["127.0.0.1:37992", "127.0.0.1:37993"]
connect_timeout = "1s"
"#;

thread_local! {
    static CORE: Arc<Core> = common::block_on(async {
        let config: Config = toml::from_str(CONFIG).expect("the target's own configuration");
        config.validate().expect("the target's own configuration");
        let stats = Arc::new(Stats::start(&config.stats));
        Core::new(PathBuf::from("/nonexistent/rfbouncer.toml"), config, stats)
    });
}

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    let core = CORE.with(Arc::clone);
    match common::block_on(admin::run(&text, &core)) {
        // An answer is something a Rayforce client can read.
        Ok(payload) => assert!(matches!(
            decode(&payload),
            Ok(Value::Table(_) | Value::Text(_))
        )),
        Err(why) => assert!(!why.is_empty()),
    }
});
