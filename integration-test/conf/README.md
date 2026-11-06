# Integration-test configuration

[.elmLicInfo.plt](.elmLicInfo.plt) is the user-supplied default for this authorized
internal repository. It configures an ELM license-service connection, not an
offline entitlement. PDI needs access to that service and a valid entitlement.
Do not print, decode or upload its contents. The file is not ignored and is
intended for this authorized repository. An existing approved local configuration can be selected using
`-Dvertica.it.license-file=/absolute/path`.

The tested 11.1 automation image supplies Vertica JDBC 9.2.1-0.
[prepare-runtime.sh](../pentaho-platform/docker/prepare-runtime.sh) preserves the
image driver when replacing the runtime. An existing `vertica-jdbc.jar` in this
directory or `-Dvertica.it.jdbc-file=/absolute/path` overrides the runtime and
image drivers; the script removes competing runtime drivers and checks both JDBC
and native COPY classes. JARs in this directory are ignored by Git. Neither JDBC
nor licensing configuration is downloaded.

The configuration is copied into the derived PDI image and its build context;
never publish those. Clean run-scoped derived images and generated staging/build
contexts after use. CI's read-only `/it-conf` mount is an optional fallback, not a
requirement for both files. See the [execution guide](../../integration-tests-README.md)
and [QA results](../PDI-20802-QA-summary.md).