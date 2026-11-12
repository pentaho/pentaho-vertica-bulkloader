# Integration-test configuration

Provide an approved `.elmLicInfo.plt` in this directory or pass
`-Dvertica.it.license-file=/absolute/path`. It is an ELM license-service
configuration, not an offline entitlement; service access and a valid
entitlement are required. The file is ignored by Git and must not be printed or
uploaded.

No JDBC JAR is tracked here. By default the harness uses the distribution's
driver, or the automation image's driver when the distribution has none.
Select an existing JAR with `-Dvertica.it.jdbc-file=/absolute/path`; a missing
explicit override fails staging. JARs in this directory are ignored by Git.
Verify the recorded source and checksum for each run.

Historical JDBC 6 tests require `-Dvertica.it.test-md5-auth=true` for the
disposable test user on the tested Vertica server. MD5 is weak and is not
suitable for a shared or production database. The default is `false` locally
and in CI. Neither JDBC nor license configuration is downloaded by the suite.
See the [execution guide](../../integration-tests-README.md) and
[SP-7583 QA summary](../SP-7583-QA-summary.md).