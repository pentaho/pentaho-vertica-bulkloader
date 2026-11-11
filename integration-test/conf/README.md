# Integration-test configuration

Provide an approved `.elmLicInfo.plt` in this directory or pass
`-Dvertica.it.license-file=/absolute/path`. It is an ELM license-service
configuration, not an offline entitlement; service access and a valid
entitlement are required. The file is ignored by Git and must not be printed or
uploaded.

The supplied [vertica-jdbc.jar](vertica-jdbc.jar) is JDBC `06.00.0000` and is
the default. Select another JAR with `-Dvertica.it.jdbc-file=/absolute/path`.
If the selected file is absent, the harness falls back to the distribution or
base-image driver; verify the recorded source and checksum. Other JARs in this
directory are ignored by Git.

JDBC 6 requires `-Dvertica.it.test-md5-auth=true` for the disposable test user
on the tested Vertica server. MD5 is weak and is not suitable for a shared or
production database. The default is `false`; CI enables it by default for the
supplied driver. Neither JDBC nor license configuration is downloaded by the
suite. See the [execution guide](../../integration-tests-README.md) and
[SP-7583 QA summary](../SP-7583-QA-summary.md).