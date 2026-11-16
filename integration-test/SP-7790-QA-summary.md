# SP-7790 QA validation summary

## Scope and conclusion

[SP-7790](https://pentaho-eng.atlassian.net/browse/SP-7790) backports support
for loading existing `LONG VARCHAR` and `LONG VARBINARY` columns to 10.2.
The fix is validated for the configuration below, including values over 65,000
bytes, with a maximum-width memory limitation. Spoon-generated DDL is separate
[PDI-18512](https://pentaho-eng.atlassian.net/browse/PDI-18512).

Both 32,000,000-byte tests failed with `OutOfMemoryError` using the distribution's
default Pan heap and passed with `-Xmx4g`. The limitation is tracked in
[PDI-21096](https://pentaho-eng.atlassian.net/browse/PDI-21096); the product
owner and developer agreed to track it separately.

## Environment and execution

Tests ran on November 16, 2026 using real Pan/Kitchen, native Vertica COPY and
independent database checks. The Jira build comment identifies 10.2.0.11-459.

| Component | Tested configuration |
| --- | --- |
| Distribution | PDI `10.2.0.11-459`, bundled plugin |
| Candidate | Plugin built with Java 11 from the 10.2 checkout at `f06431ca`, tested in the same distribution |
| PDI image | `repo.pentaho.com/pnt-docker/automation/pdi-client:10.2.0.11` |
| Database | Vertica CE `24.1.0-0`, disposable container |
| JDBC | Image-supplied Vertica JDBC `24.4.0-0` |
| License | Existing approved local service configuration; service connectivity required |

The resolver verified the archive and recorded SHA-256
`064497fe4ad9de7daf457a47df4c13105ccf1083ac61c667ecfe60c63cb79e35`.
The staged image driver SHA-256 is
`cc720c960d2280643b5c1d463a15e05a97fe9f456b30d1dfb5f16405ecbb0637`;
this is staging provenance, not a JVM class-load trace.

## Results

| Scenario | Bundled build 459 | 10.2 candidate |
| --- | --- | --- |
| 13 normal long-type tests | Pass | Pass |
| 12 existing lifecycle and regression tests | Pass | Pass |
| 32,000,000-byte text and binary, default heap | Both fail with `OutOfMemoryError` | Not run |
| 32,000,000-byte text and binary, `-Xmx4g` | Both pass | Not run |

The normal long-type suite covers mixed and default-width columns, values around
65,000 bytes, longer UTF-8 and patterned binary values, null and empty values,
Janino conversion, three loader copies, 1,048,575 rows, and over-width rejection
with recovery. Large values are independently checked in bounded SHA-256 chunks.
Janino was temporarily enabled in the disposable container and restored afterward.
The Java 11 candidate build also passed 74 project unit tests and two harness
helper tests.

## Remaining coverage and evidence

Non-direct loading of actual values over 65,000 bytes, full default-width values,
multiple maximum-width loader copies, Spoon UI and Pentaho Server were not tested.
Results apply to the stated configuration; no JVM class-load trace was captured.
CI for this test backport has not been run.

Reproduction options are in the [integration test README](../integration-tests-README.md).
Local evidence is retained under the platform module's `target/evidence-sp7790-*`
directories and `target/reports-sp7790-*`; review logs before sharing.