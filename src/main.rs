use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

/// One port in front of a pool of read-only Rayforce replicas.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// The configuration file.
    #[arg(short, long, default_value = "rfbouncer.toml")]
    config: PathBuf,
    /// Check the configuration and exit.
    #[arg(long)]
    check: bool,
}

fn main() -> ExitCode {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    if args.check {
        return match rfbouncer::config::Config::load(&args.config) {
            Ok(_) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("{e}");
                ExitCode::FAILURE
            }
        };
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(rfbouncer::run(args.config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
