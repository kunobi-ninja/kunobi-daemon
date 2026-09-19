//! Regenerate checked-in schemas without imposing protoc on consumers.
use std::{env, fs, path::Path};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let check = env::args().skip(1).any(|arg| arg == "--check");
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for (source, package, target) in [
        (
            "proto/lifecycle.proto",
            "lifecycle.rs",
            "src/wire/generated.rs",
        ),
        (
            "examples/proto/cache.proto",
            "cache.rs",
            "examples/support/cache.rs",
        ),
        (
            "tests/proto/future.proto",
            "future.rs",
            "tests/fixtures/future.rs",
        ),
    ] {
        let output = tempfile::tempdir()?;
        let source = root.join(source);
        buffa_build::Config::new()
            .files(&[&source])
            .includes(&[source.parent().unwrap()])
            .out_dir(output.path())
            .generate_views(false)
            .idiomatic_enum_aliases(true)
            .generate_json(false)
            .generate_text(false)
            .compile()?;
        let generated = fs::read(output.path().join(package))?;
        let target = root.join(target);
        if check {
            if fs::read(&target)? != generated {
                return Err(format!("{} is stale; run cargo run --locked --manifest-path tools/proto-gen/Cargo.toml", target.display()).into());
            }
        } else {
            fs::create_dir_all(target.parent().unwrap())?;
            fs::write(target, generated)?;
        }
    }
    Ok(())
}
