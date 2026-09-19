# Build And Version Management

## Goals

The Maven reactor uses one project version and centrally managed dependency and
plugin versions. No module chooses its own release version; `stow-core` and
`stow-spring-boot-starter` always publish with the same version.

## Project Version

The root POM uses Maven's CI-friendly `${revision}` variable and defines
`0.1.0-SNAPSHOT` as the development default.

Every child module:

- references the parent with `${revision}`;
- does not declare an independent project version;
- uses `${project.version}` for reactor dependencies; and
- inherits the same `groupId` and project version.

Normal development changes only the root `revision`. A release can override it
without editing child POMs:

```powershell
.\mvnw.cmd -Drevision=<version> clean deploy
```

## Consumer POMs

Published modules use `flatten-maven-plugin` in `resolveCiFriendliesOnly` mode:

- source POMs retain the maintainable `${revision}`;
- `install` and `deploy` use a flattened POM with a concrete version;
- dependencies, optional flags, scopes, and consumer metadata are preserved; and
- `clean` removes generated flattened POMs.

The root POM supplies this configuration. Samples and benchmarks set
`maven.deploy.skip=true`; the only release artifacts are the two Stow JARs.
The flattened POM resolves `${revision}` to the release version while reactor
dependencies retain `${project.version}` and resolve through the concrete parent.

## Dependency And Plugin Versions

Third-party dependency versions are declared only in the root
`dependencyManagement`. Child modules choose dependencies and scopes without
repeating versions.

Build plugin versions are defined only in the root `pluginManagement` or root
build plugins. Children add module-specific configuration without copying
plugin versions.

When adding a dependency or plugin, add its version property and management
entry to the root POM before using it in a child.

## Drift Prevention

The build-baseline test parses the reactor POM and verifies that:

- all modules are discovered recursively from each aggregator's `modules`;
- the root version is `${revision}` with one development default;
- every child parent version is `${revision}`;
- children do not declare independent project versions;
- reactor dependencies use only `${project.version}`;
- child dependencies and dependency management do not repeat third-party versions;
- child build plugins and plugin management do not repeat plugin versions;
- both release modules resolve to the same effective version; and
- samples and benchmarks remain non-deployable.

The test uses an XML parser with DOCTYPE, external entity, external DTD, and
external schema access disabled. Build validation therefore cannot read outside
the workspace or access the network.

The complete `mvnw verify` build also verifies that flattened POMs resolve to
the requested concrete version and runs formatting, tests, and static analysis.

## Release Constraints

Release versions must be valid Maven versions without `SNAPSHOT`. The release
workflow passes one `revision` to one reactor build; the two release modules are
never deployed separately or with different versions.
