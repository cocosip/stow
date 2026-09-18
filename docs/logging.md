# Logging

`stow-core` depends only on `slf4j-api`. It deliberately does not select a logging
provider, so the host application owns the provider and its configuration.

For a local console sample, the reactor uses `slf4j-simple`. Replace that dependency
with the provider your application already uses; no Stow code changes are required.

Stow uses SLF4J 2.0.17 as its default API baseline:

```xml
<dependency>
  <groupId>org.slf4j</groupId>
  <artifactId>slf4j-api</artifactId>
  <version>2.0.17</version>
</dependency>
```

To verify the core against a legacy SLF4J 1.7 host:

```powershell
mvn -Pslf4j1-compat -pl stow-core test
```

The compatibility profile overrides the API to `1.7.36` and adds `slf4j-simple`
only to the test runtime. It is not packaged into `stow-core`.

For production, select one provider at the application boundary, for example:

```xml
<dependency>
  <groupId>org.slf4j</groupId>
  <artifactId>slf4j-api</artifactId>
  <version>2.0.17</version>
</dependency>
<dependency>
  <groupId>ch.qos.logback</groupId>
  <artifactId>logback-classic</artifactId>
  <version>1.5.18</version>
</dependency>
```

Use exactly one provider. Running with only the API is supported, but SLF4J will
report its standard NOP-provider warning because there is nowhere to send logs.
