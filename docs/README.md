# Stow Documentation

This directory contains the stable design, API, persistence, operations, and
release documentation for Stow. The public quick start and runnable examples
are in the [root README](../README.md).

## Normative Specifications

- [Design](stow-design.md): scope, architecture, module boundaries, lifecycle,
  recovery, concurrency, security, and acceptance criteria.
- [API contract](stow-api-contract.md): public Java types, lifecycle rules,
  service methods, SPI boundaries, configuration, and exceptions.
- [Persistence contract](stow-persistence-contract.md): on-disk layout,
  SQLite schemas, journal formats, state transitions, and crash invariants.

These three documents are the source of truth for compatibility and durable
data formats. They are intentionally kept separate from user-facing guides.
Their original Simplified Chinese wording is preserved because the exact
contract text is part of the project's design baseline; all user-facing and
operational documentation is maintained in English.

## User And Operator Guides

- [Configuration reference](configuration-reference.md): builder defaults,
  Spring Boot property names, validation, and supported enum values.
- [Logging](logging.md): SLF4J 2 API usage, provider selection, and the legacy
  compatibility profile.
- [Operations and recovery](operations-and-recovery.md): normal processing,
  replay, rebuild, cleanup, orphan recovery, and incident handling.
- [Release verification](release-verification.md): branch checks, Central
  Portal publishing, and required GitHub Actions secrets.
- [Build and version management](build-version-management.md): reactor
  versioning, dependency/plugin management, and publish constraints.

The former implementation-plan document was development-only material and is
not part of the documentation set. Historical task progress belongs in Git
history; stable behavior belongs in the specifications and guides above.
