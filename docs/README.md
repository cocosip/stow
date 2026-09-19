# Stow Documentation

This directory contains the stable English design, API, persistence, and
operations documentation for Stow. The public quick start, Spring Boot
integration guide, runnable examples, logging guidance, and benchmark baseline
are in the [root README](../README.md).

## Normative Specifications

- [Design](stow-design.md): scope, architecture, module boundaries, lifecycle,
  recovery, concurrency, security, and acceptance criteria.
- [API contract](stow-api-contract.md): public Java types, lifecycle rules,
  service methods, SPI boundaries, configuration, and exceptions.
- [Persistence contract](stow-persistence-contract.md): on-disk layout,
  SQLite schemas, journal formats, state transitions, and crash invariants.

These three documents are the source of truth for compatibility and durable
data formats. They are intentionally kept separate from operational guidance.
All normative and user-facing documentation is maintained in English.

## Operations And Release

- [Operations and release guide](operations-and-release.md): runtime
  directories, replay and recovery, incident handling, build/version rules,
  GitHub Actions, and Central Portal publishing.

The former implementation-plan document was development-only material and is
not part of the documentation set. Historical task progress belongs in Git
history; stable behavior belongs in the specifications and guide above.
