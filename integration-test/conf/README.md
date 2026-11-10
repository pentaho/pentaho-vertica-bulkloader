# Integration-test configuration

Provide an approved `.elmLicInfo.plt` locally in this directory, or select an
existing approved file using `-Dvertica.it.license-file=/absolute/path`. It
configures an ELM license-service connection, not an offline entitlement. PDI
needs access to that service and a valid entitlement. The default local file is
ignored by Git; do not print, decode or upload its contents.

The checked-in [vertica-jdbc.jar](vertica-jdbc.jar) is the supplied JDBC
`06.00.0000` driver and is the default for the integration tests. To test another
driver without replacing the tracked file, pass
`-Dvertica.it.jdbc-file=/absolute/path/to/other-driver.jar`. Other JARs in this
directory remain ignored by Git. [prepare-runtime.sh](../pentaho-platform/docker/prepare-runtime.sh)
removes competing runtime drivers and checks both JDBC and native COPY classes.
If the selected JAR is absent, it falls back to the selected PDI runtime or base
image driver; check the recorded driver checksum in the run evidence. Neither
JDBC nor licensing configuration is downloaded.

For JDBC 6, add `-Dvertica.it.test-md5-auth=true` to the integration command to
set the disposable `qa_loader` user's password algorithm to MD5. The default is
`false`; do not use this weak authentication setting on a shared or production
database. The CI workflow enables this setting for the disposable database by
default; it can be disabled when testing another driver.

The configuration is copied into the derived PDI image and its build context;
never publish those. Clean run-scoped derived images and generated staging/build
contexts after use. CI obtains the license configuration from its read-only
`/it-conf` mount and can select an alternate JAR on that mount explicitly. See
the [execution guide](../../integration-tests-README.md)
and [SP-7772 QA results](../SP-7772-QA-summary.md).