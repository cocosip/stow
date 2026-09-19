# Release Verification

This document records the commands and credentials used by the GitHub Actions
release workflow. A release is created only from a non-SNAPSHOT tag matching
`v*`, for example `v1.0.0`.

## Branch Builds

Pushes and pull requests targeting `master` run the following command on both
Ubuntu and Windows with JDK 21:

```bash
./mvnw -B -ntp verify
```

The branch workflow does not call `deploy` and cannot publish Maven artifacts.

## Central Portal Release

The tag workflow derives the Maven version by removing the leading `v` and
runs:

```bash
./mvnw -B -ntp -Drevision="$RELEASE_VERSION" -Prelease deploy
```

The `release` profile signs all published artifacts with the GPG key imported
by `actions/setup-java` and uses the Sonatype Central Publishing Maven Plugin
to upload and auto-publish the bundle. The reactor still builds samples and
benchmarks, but their POMs set `maven.deploy.skip=true`; only these coordinates
are published:

- `io.github.cocosip:stow-core`
- `io.github.cocosip:stow-spring-boot-starter`

Configure the following repository secrets before creating a tag:

| Secret | Purpose |
| --- | --- |
| `MAVEN_CENTRAL_USERNAME` | Sonatype Central Portal user-token username |
| `MAVEN_CENTRAL_TOKEN` | Sonatype Central Portal user-token password |
| `MAVEN_GPG_PRIVATE_KEY` | ASCII-armored private signing key |
| `MAVEN_GPG_PASSPHRASE` | Passphrase for the signing key |

The Maven server id is `central`. No secret is committed to source control.
