//! The command line and environment block `CreateProcessW` takes.
//!
//! Pure functions over UTF-16 units, so they are tested on every platform even
//! though only the Windows spawn uses them. The standard library builds both
//! for `Command::spawn`, but offers no way to reuse them, and a spawn that
//! needs an explicit handle list has to call `CreateProcessW` itself.

use std::io;

const QUOTE: u16 = b'"' as u16;
const BACKSLASH: u16 = b'\\' as u16;
const EQUALS: u16 = b'=' as u16;

/// The command line for `program` and `args`, as the Microsoft C runtime and
/// `CommandLineToArgvW` split it back.
///
/// The program is always quoted and may not contain a quote: the loader, not
/// the C runtime, reads the first token, and it has no escape for one.
/// Arguments are quoted only when they need to be.
pub(crate) fn command_line(program: &[u16], args: &[Vec<u16>]) -> io::Result<Vec<u16>> {
    if program.is_empty() || program.contains(&QUOTE) {
        return Err(invalid("the program path is empty or contains a quote"));
    }
    if program.contains(&0) || args.iter().any(|arg| arg.contains(&0)) {
        return Err(invalid("a command line cannot contain a NUL"));
    }
    let mut line = Vec::with_capacity(program.len() + 2);
    line.push(QUOTE);
    line.extend_from_slice(program);
    line.push(QUOTE);
    for arg in args {
        line.push(b' ' as u16);
        append_argument(&mut line, arg);
    }
    Ok(line)
}

fn needs_quotes(arg: &[u16]) -> bool {
    arg.is_empty()
        || arg
            .iter()
            .any(|&unit| matches!(unit, 0x20 | 0x09 | 0x0a | 0x0b) || unit == QUOTE)
}

fn append_argument(line: &mut Vec<u16>, arg: &[u16]) {
    if !needs_quotes(arg) {
        line.extend_from_slice(arg);
        return;
    }
    line.push(QUOTE);
    let mut backslashes = 0;
    for &unit in arg {
        if unit == BACKSLASH {
            backslashes += 1;
            continue;
        }
        if unit == QUOTE {
            // Backslashes before a quote are escapes, so double them, then
            // escape the quote itself.
            line.extend(std::iter::repeat_n(BACKSLASH, backslashes * 2 + 1));
        } else {
            line.extend(std::iter::repeat_n(BACKSLASH, backslashes));
        }
        backslashes = 0;
        line.push(unit);
    }
    // Trailing backslashes precede the closing quote.
    line.extend(std::iter::repeat_n(BACKSLASH, backslashes * 2));
    line.push(QUOTE);
}

/// One change to the inherited environment, in the order the caller made it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EnvChange {
    Set(Vec<u16>, Vec<u16>),
    Remove(Vec<u16>),
}

/// The environment block for `CREATE_UNICODE_ENVIRONMENT`: the inherited
/// variables with `changes` applied in order, sorted by name, each written as
/// `NAME=value\0`, and the whole block ending in one more `\0`.
///
/// Windows compares variable names without regard to case, so a change to
/// `path` replaces `Path`. Case folding here is ASCII only, which covers every
/// variable a daemon launcher sets or removes.
pub(crate) fn environment_block(
    inherited: Vec<(Vec<u16>, Vec<u16>)>,
    changes: &[EnvChange],
) -> io::Result<Vec<u16>> {
    let mut vars = inherited;
    for change in changes {
        let name = match change {
            EnvChange::Set(name, _) | EnvChange::Remove(name) => name,
        };
        if name.is_empty() || name[1..].contains(&EQUALS) || name.contains(&0) {
            return Err(invalid(
                "an environment name is empty or contains '=' or NUL",
            ));
        }
        vars.retain(|(existing, _)| !same_name(existing, name));
        if let EnvChange::Set(name, value) = change {
            if value.contains(&0) {
                return Err(invalid("an environment value contains NUL"));
            }
            vars.push((name.clone(), value.clone()));
        }
    }
    vars.sort_by(|(a, _), (b, _)| folded(a).cmp(folded(b)));
    let mut block = Vec::new();
    for (name, value) in vars {
        block.extend_from_slice(&name);
        block.push(EQUALS);
        block.extend_from_slice(&value);
        block.push(0);
    }
    if block.is_empty() {
        // An empty block is still two NULs: the empty list and its end.
        block.push(0);
    }
    block.push(0);
    Ok(block)
}

fn folded(name: &[u16]) -> impl Iterator<Item = u16> + '_ {
    name.iter().map(|&unit| match unit {
        0x61..=0x7a => unit - 0x20,
        _ => unit,
    })
}

fn same_name(a: &[u16], b: &[u16]) -> bool {
    a.len() == b.len() && folded(a).eq(folded(b))
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(text: &str) -> Vec<u16> {
        text.encode_utf16().collect()
    }

    fn line(program: &str, args: &[&str]) -> String {
        let args: Vec<Vec<u16>> = args.iter().map(|arg| w(arg)).collect();
        String::from_utf16(&command_line(&w(program), &args).unwrap()).unwrap()
    }

    #[test]
    fn plain_arguments_are_left_alone_and_the_program_is_quoted() {
        assert_eq!(
            line(r"C:\bin\kache.exe", &["daemon", "run"]),
            r#""C:\bin\kache.exe" daemon run"#
        );
    }

    #[test]
    fn arguments_the_c_runtime_would_split_are_quoted() {
        assert_eq!(line("p", &["a b"]), r#""p" "a b""#);
        assert_eq!(line("p", &["tab\there"]), "\"p\" \"tab\there\"");
        assert_eq!(line("p", &[""]), r#""p" """#);
    }

    #[test]
    fn quotes_and_the_backslashes_before_them_are_escaped() {
        assert_eq!(line("p", &[r#"a"b"#]), r#""p" "a\"b""#);
        assert_eq!(line("p", &[r#"a\"b"#]), r#""p" "a\\\"b""#);
        // Backslashes not followed by a quote stay as they are.
        assert_eq!(line("p", &[r"C:\dir\file"]), r#""p" C:\dir\file"#);
    }

    #[test]
    fn trailing_backslashes_are_doubled_only_inside_quotes() {
        assert_eq!(line("p", &[r"C:\my dir\"]), r#""p" "C:\my dir\\""#);
        assert_eq!(line("p", &[r"C:\dir\"]), r#""p" C:\dir\"#);
    }

    #[test]
    fn a_program_with_a_quote_or_any_nul_is_refused() {
        assert!(command_line(&w(r#"a"b"#), &[]).is_err());
        assert!(command_line(&[], &[]).is_err());
        assert!(command_line(&w("p"), &[vec![b'a' as u16, 0]]).is_err());
    }

    fn block(inherited: &[(&str, &str)], changes: &[EnvChange]) -> String {
        let inherited = inherited.iter().map(|(n, v)| (w(n), w(v))).collect();
        String::from_utf16(&environment_block(inherited, changes).unwrap()).unwrap()
    }

    #[test]
    fn changes_apply_in_order_and_ignore_case() {
        let changes = [
            EnvChange::Set(w("path"), w(r"C:\new")),
            EnvChange::Remove(w("KACHE_S3_BUCKET")),
            EnvChange::Set(w("A"), w("1")),
            EnvChange::Remove(w("a")),
        ];
        assert_eq!(
            block(
                &[("Path", r"C:\old"), ("kache_s3_bucket", "b"), ("Z", "z")],
                &changes
            ),
            "path=C:\\new\0Z=z\0\0"
        );
    }

    #[test]
    fn the_block_is_sorted_by_name_without_case() {
        assert_eq!(
            block(&[("b", "2"), ("A", "1"), ("C", "3")], &[]),
            "A=1\0b=2\0C=3\0\0"
        );
    }

    #[test]
    fn an_empty_environment_is_two_nuls() {
        assert_eq!(block(&[], &[]), "\0\0");
    }

    #[test]
    fn bad_names_and_values_are_refused() {
        for change in [
            EnvChange::Set(w(""), w("x")),
            EnvChange::Set(w("A=B"), w("x")),
            EnvChange::Remove(w("A\0")),
            EnvChange::Set(w("A"), vec![0]),
        ] {
            assert!(environment_block(Vec::new(), &[change]).is_err());
        }
    }
}
