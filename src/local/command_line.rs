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

/// Round trips through a reference split of the command line, over arbitrary
/// UTF-16, including lone surrogates, runs of backslashes and quotes.
#[cfg(test)]
mod properties {
    use super::*;
    use proptest::prelude::*;

    const SPACE: u16 = b' ' as u16;
    const TAB: u16 = b'\t' as u16;

    /// How a split reads `""` inside a quoted region. Splitters disagree:
    /// some read one quote and stay quoted, others read one quote and end the
    /// region. A command line built here must mean the same under both.
    #[derive(Clone, Copy, Debug)]
    enum DoubledQuote {
        StaysQuoted,
        EndsQuote,
    }

    /// Split `line` into the program and its arguments the way the loader and
    /// the C runtime do.
    fn split(line: &[u16], doubled: DoubledQuote) -> Vec<Vec<u16>> {
        let mut units = line.iter().copied().peekable();
        // The program name has no escapes: quotes toggle, and whitespace
        // outside them ends it.
        let mut program = Vec::new();
        let mut quoted = false;
        while let Some(&unit) = units.peek() {
            if !quoted && (unit == SPACE || unit == TAB) {
                break;
            }
            units.next();
            if unit == QUOTE {
                quoted = !quoted;
            } else {
                program.push(unit);
            }
        }
        let mut words = vec![program];
        loop {
            while units
                .next_if(|&unit| unit == SPACE || unit == TAB)
                .is_some()
            {}
            if units.peek().is_none() {
                return words;
            }
            let mut word = Vec::new();
            let mut quoted = false;
            loop {
                let mut backslashes = 0usize;
                while units.next_if_eq(&BACKSLASH).is_some() {
                    backslashes += 1;
                }
                match units.peek().copied() {
                    Some(QUOTE) if !backslashes.is_multiple_of(2) => {
                        // An odd run escapes the quote.
                        word.extend(std::iter::repeat_n(BACKSLASH, backslashes / 2));
                        word.push(QUOTE);
                        units.next();
                    }
                    Some(QUOTE) => {
                        word.extend(std::iter::repeat_n(BACKSLASH, backslashes / 2));
                        units.next();
                        if quoted && units.next_if_eq(&QUOTE).is_some() {
                            word.push(QUOTE);
                            quoted = matches!(doubled, DoubledQuote::StaysQuoted);
                        } else {
                            quoted = !quoted;
                        }
                    }
                    other => {
                        word.extend(std::iter::repeat_n(BACKSLASH, backslashes));
                        match other {
                            None => break,
                            Some(unit) if !quoted && (unit == SPACE || unit == TAB) => break,
                            Some(unit) => {
                                word.push(unit);
                                units.next();
                            }
                        }
                    }
                }
            }
            words.push(word);
        }
    }

    /// Code units biased toward the ones quoting has to handle.
    fn unit() -> impl Strategy<Value = u16> {
        prop_oneof![
            2 => Just(QUOTE),
            3 => Just(BACKSLASH),
            2 => Just(SPACE),
            1 => Just(TAB),
            1 => Just(0x0a),
            1 => Just(0x0b),
            3 => 0x21u16..0x7f,
            // Anything else but NUL, lone surrogates included.
            1 => 1u16..=u16::MAX,
        ]
    }

    fn arg() -> impl Strategy<Value = Vec<u16>> {
        proptest::collection::vec(unit(), 0..12)
    }

    fn program() -> impl Strategy<Value = Vec<u16>> {
        proptest::collection::vec(unit().prop_filter("no quote", |&unit| unit != QUOTE), 1..24)
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 512,
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        #[test]
        fn every_argument_survives_the_c_runtime_split(
            program in program(),
            args in proptest::collection::vec(arg(), 0..6),
        ) {
            let line = command_line(&program, &args).unwrap();
            let mut expected = vec![program];
            expected.extend(args);
            for doubled in [DoubledQuote::StaysQuoted, DoubledQuote::EndsQuote] {
                prop_assert_eq!(split(&line, doubled), expected.clone(), "{:?}", doubled);
            }
        }

        #[test]
        fn a_nul_anywhere_or_a_quote_in_the_program_is_refused(
            mut program in program(),
            mut args in proptest::collection::vec(arg(), 1..4),
            which in 0u8..3,
            at in any::<prop::sample::Index>(),
        ) {
            let (target, unit) = match which {
                0 => (&mut program, QUOTE),
                1 => (&mut program, 0),
                _ => (at.get_mut(&mut args), 0),
            };
            let index = at.index(target.len() + 1);
            target.insert(index, unit);
            prop_assert!(command_line(&program, &args).is_err());
        }
    }

    /// A variable name as Windows allows it: no NUL, and no `=` after the
    /// first unit. Mixed case, so changes collide with inherited names.
    fn name() -> impl Strategy<Value = Vec<u16>> {
        (
            proptest::option::weighted(0.1, Just(EQUALS)),
            proptest::collection::vec(
                prop_oneof![
                    Just(b'a' as u16),
                    Just(b'A' as u16),
                    Just(b'b' as u16),
                    Just(b'B' as u16),
                    Just(b'_' as u16)
                ],
                1..4,
            ),
        )
            .prop_map(|(lead, rest)| lead.into_iter().chain(rest).collect::<Vec<u16>>())
    }

    fn value() -> impl Strategy<Value = Vec<u16>> {
        proptest::collection::vec(
            prop_oneof![Just(EQUALS), Just(b'x' as u16), 1u16..=u16::MAX],
            0..6,
        )
    }

    fn change() -> impl Strategy<Value = EnvChange> {
        prop_oneof![
            (name(), value()).prop_map(|(name, value)| EnvChange::Set(name, value)),
            name().prop_map(EnvChange::Remove),
        ]
    }

    /// Inherited variables with distinct names, as a real environment has.
    fn inherited() -> impl Strategy<Value = Vec<(Vec<u16>, Vec<u16>)>> {
        proptest::collection::vec((name(), value()), 0..6).prop_map(|mut vars| {
            let mut seen = Vec::new();
            vars.retain(|(name, _)| {
                let key: Vec<u16> = folded(name).collect();
                let fresh = !seen.contains(&key);
                seen.push(key);
                fresh
            });
            vars
        })
    }

    /// The entries of a block, split at the first `=` after the first unit.
    fn entries(block: &[u16]) -> Vec<(Vec<u16>, Vec<u16>)> {
        let body = block
            .strip_suffix(&[0u16])
            .expect("the block ends in a NUL");
        if body == [0u16] {
            return Vec::new();
        }
        body.strip_suffix(&[0u16])
            .expect("the last entry ends in a NUL")
            .split(|&unit| unit == 0)
            .map(|entry| {
                let at = 1 + entry[1..]
                    .iter()
                    .position(|&unit| unit == EQUALS)
                    .expect("an entry has a name and a value");
                (entry[..at].to_vec(), entry[at + 1..].to_vec())
            })
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 512,
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        #[test]
        fn the_block_holds_the_last_change_to_each_name_and_every_untouched_variable(
            inherited in inherited(),
            changes in proptest::collection::vec(change(), 0..8),
        ) {
            let block = environment_block(inherited.clone(), &changes).unwrap();
            let entries = entries(&block);

            // Sorted by name without case, so also one entry per name.
            for pair in entries.windows(2) {
                prop_assert!(
                    folded(&pair[0].0).lt(folded(&pair[1].0)),
                    "{:?} is not strictly before {:?}",
                    pair[0].0,
                    pair[1].0
                );
            }
            let find = |name: &[u16]| entries.iter().find(|(entry, _)| same_name(entry, name));
            let last_change = |name: &[u16]| {
                changes.iter().rev().find(|change| match change {
                    EnvChange::Set(changed, _) | EnvChange::Remove(changed) => {
                        same_name(changed, name)
                    }
                })
            };
            for change in &changes {
                let name = match change {
                    EnvChange::Set(name, _) | EnvChange::Remove(name) => name,
                };
                match last_change(name) {
                    Some(EnvChange::Set(name, value)) => {
                        prop_assert_eq!(find(name).cloned(), Some((name.clone(), value.clone())));
                    }
                    _ => prop_assert!(find(name).is_none(), "{:?} was removed", name),
                }
            }
            for (name, value) in &inherited {
                if last_change(name).is_none() {
                    prop_assert_eq!(find(name).cloned(), Some((name.clone(), value.clone())));
                }
            }
            // Nothing appears that was neither inherited nor set.
            for (name, _) in &entries {
                prop_assert!(
                    matches!(last_change(name), Some(EnvChange::Set(..)))
                        || (last_change(name).is_none()
                            && inherited.iter().any(|(inherited, _)| inherited == name)),
                    "{:?} came from nowhere",
                    name
                );
            }
        }
    }
}
