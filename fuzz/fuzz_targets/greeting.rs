//! The greeting from both sides: what a client sends before it is known
//! (its hello and its credentials), and what a replica answers the bouncer.
#![no_main]

mod common;

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use rfbouncer::config::Config;
use rfbouncer::wire::greeting::{self, GreetError};

const CONFIG: &str = r#"
listen = "127.0.0.1:37990"
admin_password = "root"
[[pool]]
name = "main"
users = ["app"]
password = "a"
wire_version = 3
backends = ["127.0.0.1:37991"]
[[pool]]
name = "reports"
default = true
backends = ["127.0.0.1:37992"]
"#;

fn config() -> &'static Config {
    static CONFIG_ONCE: OnceLock<Config> = OnceLock::new();
    CONFIG_ONCE.get_or_init(|| {
        let config: Config = toml::from_str(CONFIG).expect("the target's own configuration");
        config.validate().expect("the target's own configuration");
        config
    })
}

fuzz_target!(|data: &[u8]| {
    common::block_on(async {
        // As a server: the two hello bytes, then the credentials.
        let mut wire = data;
        if let Ok(version) = greeting::read_hello(&mut wire).await {
            assert_eq!(version, data[0]);
            let before = wire.len();
            if let Ok(c) = greeting::read_credentials(&mut wire).await {
                // Exactly the length byte and what it counts was taken.
                assert_eq!(before - wire.len(), 1 + usize::from(data[2]));
                assert!(!c.user.contains(':'));
                assert!(c.password.len() <= 255);
                // Whatever was presented names the admin, a pool or nobody.
                let _ = config().resolve(&c.user, &c.password, version);
            }
        }

        // As a client: the input is what the replica answers.
        let Some((&[version, kind], answer)) = data.split_first_chunk::<2>() else {
            return;
        };
        let password = match kind % 3 {
            0 => None,
            1 => Some("pw".to_owned()),
            // Long enough, sometimes, not to fit the one-byte length.
            _ => Some("x".repeat(usize::from(kind))),
        };
        let mut stream = tokio::io::join(answer, tokio::io::sink());
        match greeting::connect(&mut stream, version, password.as_deref()).await {
            Ok(()) => assert_eq!(answer[0], version),
            Err(GreetError::Version { got, expected }) => {
                assert!(got != expected && expected == version);
            }
            Err(_) => {}
        }
    });
});
