//! MSVC import lib names must be lowercase so cargo-xwin's lld-link can
//! open the xwin splat files on Linux. Native `windows-latest` CI does not
//! catch mixed case because MSVC's linker is case-insensitive.

#[test]
fn msvc_import_libs_use_lowercase_names() {
    let mixed: Vec<String> = include_str!("../src/local/windows.rs")
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let rest = line.trim().strip_prefix("#[link(name = \"")?;
            let name = rest.split('"').next()?;
            name.chars()
                .any(|c| c.is_ascii_uppercase())
                .then(|| format!("{}: {name}", index + 1))
        })
        .collect();
    assert!(
        mixed.is_empty(),
        "Windows #[link(name)] must be lowercase for cargo-xwin: {mixed:?}"
    );
}
