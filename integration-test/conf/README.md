# Integration-test configuration

The selected PDI image may provide an ELM configuration. To override it, supply
an approved local `.elmLicInfo.plt` in this directory, or select one with
`-Dvertica.it.license-file=/absolute/path`. The configuration is a license-service
connection, not an offline entitlement; PDI needs access to that service and a
valid entitlement. Do not print, decode, commit or upload its contents. The
default file is ignored by Git; removing it from the current tree does not remove
it from earlier published history.

No JDBC driver is checked in or selected by default. The suite uses a driver in
the selected PDI distribution, or preserves the automation image driver when the
distribution has none. To select a different JAR, pass
`-Dvertica.it.jdbc-file=/absolute/path/to/driver.jar`; an explicit path must
exist. JARs in this directory are ignored by Git.
[prepare-runtime.sh](../pentaho-platform/docker/prepare-runtime.sh) removes
competing runtime drivers for an override and checks JDBC and native COPY classes.
Check the source and checksum recorded in the run evidence. Neither JDBC nor
licensing configuration is downloaded by the suite.

For JDBC 6, add `-Dvertica.it.test-md5-auth=true` to the integration command to
set the disposable `qa_loader` user's password algorithm to MD5. The default is
`false`; do not use this weak authentication setting on a shared or production
database. The CI workflow also defaults to `false`; JDBC 24.4 was validated
with the default authentication.

An override is copied into the derived PDI image and its build context; the base
image may also contain license configuration. Never publish derived images or
build contexts. Clean run-scoped derived images and generated staging/build
contexts after use. CI can select an alternate JAR on its read-only `/it-conf`
mount explicitly. See the [execution guide](../../integration-tests-README.md)
and [QA results](../PDI-20802-QA-summary.md).