# Integration-test configuration

The selected PDI image may provide an ELM configuration. To override it, provide
an approved `.elmLicInfo.plt` in this directory, or select an existing approved
file using `-Dvertica.it.license-file=/absolute/path`. It
configures an ELM license-service connection, not an offline entitlement. PDI
needs access to that service and a valid entitlement. The default local file is
ignored by Git; do not print, decode or upload its contents.

No JDBC driver is checked in or selected by default. The suite uses a driver in
the selected PDI distribution, or preserves the automation image driver when the
distribution has none. The 332 distribution has no Vertica driver in its `lib`;
the validated `11.0.0.4` automation image supplies JDBC 24.4. To select a
different driver, use `-Dvertica.it.jdbc-file=/absolute/path/to/driver.jar`;
an explicit path must exist. JARs in this directory are
ignored by Git. [prepare-runtime.sh](../pentaho-platform/docker/prepare-runtime.sh)
removes competing runtime drivers for an override and checks JDBC and native COPY
classes. Check the source and checksum recorded in the run evidence. Neither
JDBC nor licensing configuration is downloaded by the suite.

For JDBC 6, add `-Dvertica.it.test-md5-auth=true` to the integration command to
set the disposable `qa_loader` user's password algorithm to MD5. The default is
`false`; do not use this weak authentication setting on a shared or production
database. The CI workflow also defaults to `false`; JDBC 24.4 was validated
with the default authentication.

An override is copied into the running PDI container, not its derived image or
build context. The base image may still contain license configuration, and older
build contexts may contain overrides. Never publish derived images or build
contexts. Clean run-scoped derived images and generated staging/build contexts
after use. CI can obtain an override from its read-only `/it-conf` mount and can
select an alternate JAR on that mount explicitly. See
the [execution guide](../../integration-tests-README.md)
and [SP-7772 QA results](../SP-7772-QA-summary.md).