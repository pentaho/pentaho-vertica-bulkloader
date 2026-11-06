# Integration-test configuration

[.elmLicInfo.plt](.elmLicInfo.plt) is the user-supplied default for this authorized
internal repository. It configures an ELM license-service connection, not an
offline entitlement. PDI needs access to that service and a valid entitlement.
Do not print, decode or upload its contents. The file is not ignored and is
intended for this authorized repository. An existing approved local configuration can be selected using
`-Dvertica.it.license-file=/absolute/path`.

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
contexts after use. CI can select an alternate JAR on its read-only `/it-conf`
mount explicitly. See the [execution guide](../../integration-tests-README.md)
and [QA results](../PDI-20802-QA-summary.md).