# Logging

`stow-core` depends only on `slf4j-api`. It deliberately does not select a logging
provider, so the host application owns the provider and its configuration.

For a local console sample, the reactor uses `slf4j-simple`. Replace that dependency
with the provider your application already uses; no Stow code changes are required.

To verify the core against SLF4J 2.x:

```powershell
mvn -Pslf4j2-compat -pl stow-core test
```

The compatibility profile overrides the API to `2.0.17` and adds `slf4j-simple`
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
