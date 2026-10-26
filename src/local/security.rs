//! Process-token evidence for applications that delegate work to another process.
//!
//! Connection authentication and execution authorization are separate decisions.
//! This module does not change [`crate::peer::SameUser`] or grant application
//! access. Consumers decide what the comparison permits and recheck it at their
//! own execution boundaries.

#[cfg(any(windows, test))]
mod attributes;
#[cfg(windows)]
mod windows;

/// The result of comparing the supported primary-token access fields.
///
/// This is evidence for an application policy, not proof that arbitrary process
/// sandboxes, mitigations or future Windows restrictions are equivalent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SecurityContextComparison {
    /// The compared fields match, including the opaque-attribute generation
    /// when one is present. Unchanged token copies can have different token IDs.
    Equivalent,
    /// The compared access fields match, but different token generations carry
    /// only the known `TSA://ProcUnique` shape, or both attribute lists are empty.
    ///
    /// Attribute values can affect access rules. A consumer must explicitly
    /// trust the other process's access context before accepting this result.
    ProcessIdentityDiffers,
    /// An access field differs or an unsupported claim, capability or attribute
    /// prevents establishing a compatible context.
    Incompatible,
}

/// Compare a process's primary token with this process's primary token.
///
/// The borrowed process handle must permit `PROCESS_QUERY_LIMITED_INFORMATION`.
/// Obtain and retain it from an authenticated, pinned peer; a PID from an
/// application message alone does not establish peer identity. This function
/// performs Windows queries and must run outside `peer::Policy::grant`.
///
/// Compares user, logon session, owner, primary group, groups, restricted SIDs,
/// device groups, privileges, integrity, elevation, session, UI access, mandatory
/// policy, virtualization, default DACL, trust level and token type. AppContainer,
/// claim-bearing and capability-bearing tokens return `Incompatible`.
///
/// Different opaque-attribute generations return `ProcessIdentityDiffers` only
/// for matching empty lists or the known UINT64 `TSA://ProcUnique` shape (flags
/// `0x41`, two values). Unknown shapes return `Incompatible`. Query or decoding
/// failures return an error; callers must not turn that into an access grant.
/// Recheck before delegating work because tokens can change after this snapshot.
#[cfg(windows)]
pub fn compare_with_current_process(
    process: std::os::windows::io::BorrowedHandle<'_>,
) -> std::io::Result<SecurityContextComparison> {
    windows::compare_with_current_process(process)
}

#[cfg(any(windows, test))]
fn compare_token_generations(
    same_generation: bool,
    has_opaque_attributes: bool,
    compare: impl FnOnce() -> std::io::Result<bool>,
) -> std::io::Result<SecurityContextComparison> {
    if same_generation || !has_opaque_attributes {
        return Ok(SecurityContextComparison::Equivalent);
    }
    if compare()? {
        Ok(SecurityContextComparison::ProcessIdentityDiffers)
    } else {
        Ok(SecurityContextComparison::Incompatible)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_identity_differences_without_granting_equivalence() {
        assert_eq!(
            compare_token_generations(false, true, || Ok(true)).unwrap(),
            SecurityContextComparison::ProcessIdentityDiffers
        );
        assert_eq!(
            compare_token_generations(false, true, || Ok(false)).unwrap(),
            SecurityContextComparison::Incompatible
        );
        assert!(
            compare_token_generations(false, true, || Err(std::io::Error::other(
                "unavailable attributes"
            )))
            .is_err()
        );
    }

    #[test]
    fn unchanged_generations_and_tokens_without_attributes_need_no_reserved_query() {
        for (same_generation, has_attributes) in [(true, true), (true, false), (false, false)] {
            assert_eq!(
                compare_token_generations(same_generation, has_attributes, || {
                    panic!("the original comparison path must not read reserved attributes")
                })
                .unwrap(),
                SecurityContextComparison::Equivalent
            );
        }
    }
}
