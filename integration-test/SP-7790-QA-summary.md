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

The [PR integration run](https://github.com/pentaho/pentaho-vertica-bulkloader/actions/runs/43897286717)
also passed with the checkout-built candidate on build `10.2.0.11-459`: 74 project
unit tests, two helper tests and 25 normal integration tests passed. The two
opt-in maximum-width tests were skipped in CI.

## Scenarios and transformations

All 15 long-type scenarios use Pan and native COPY. The harness prepares the
source and target tables and independently verifies target values and sessions.
[long-types.ktr](pentaho-platform/src/test/resources/transformations/vertica_bulk_loader/long-types.ktr)
connects **Read source** (Table Input) to **Load long types** (Vertica Bulk
Loader). Every scenario below uses that fixture except the Janino scenario,
which uses [long-types-janino.ktr](pentaho-platform/src/test/resources/transformations/vertica_bulk_loader/long-types-janino.ktr):
**Read source** -> **Convert to UTF-8** (Janino) -> **Load long types**.
The harness renders mapping, COPY mode and copy count as needed; it does not
run a Kitchen job for these scenarios.

| Test | Scenario and expected result |
| --- | --- |
| `mappedDirectCopiesTextAndBinary` | Small UTF-8 text and binary survive reordered direct mapping. |
| `longVarcharWithOrdinaryVarbinary` | Long text and ordinary binary both match. |
| `longVarbinaryWithOrdinaryVarchar` | Long binary and ordinary text both match. |
| `defaultWidthLongVarchar` | A small value in default-width long text matches. |
| `defaultWidthLongVarbinary` | A small value in default-width long binary matches. |
| `copiesValuesAroundShortTypeLimit` | Text and binary at 64,999, 65,000 and 65,001 bytes match. |
| `mappedDirectCopiesValuesBeyondShortTypeLimit` | Longer UTF-8 text and patterned binary match. |
| `positionalNonDirectCopiesNullAndEmptyValues` | Null and empty remain distinct in non-direct COPY. |
| `janinoConvertsTextToLongBinary` | Janino converts null, empty, small and long text to matching UTF-8 binary. |
| `multipleCopiesPreserveLongValues` | Three copies process rows; all 600 distinct IDs and values match. |
| `highVolumeLongColumnsPreserveAllRows` | All 1,048,575 unique IDs and text/binary values match. |
| `overWidthTextFailsAndRecovers` | Over-width text is rejected, the committed row survives and a valid retry succeeds. |
| `overWidthBinaryFailsAndRecovers` | Over-width binary is rejected, the committed row survives and a valid retry succeeds. |
| `maximumWidthLongVarchar` | The 32,000,000-byte text value and length match with a 4 GB heap; default heap fails. |
| `maximumWidthLongVarbinary` | The 32,000,000-byte binary value and length match with a 4 GB heap; default heap fails. |

## Remaining coverage and evidence

Non-direct loading of actual values over 65,000 bytes, full default-width values,
multiple maximum-width loader copies, Spoon UI and Pentaho Server were not tested.
Results apply to the stated configuration; no JVM class-load trace was captured.

Reproduction options are in the [integration test README](../integration-tests-README.md).
For interpretation, see its [evidence guide](../integration-tests-README.md#reading-test-evidence).

The post-rebase [CI run](https://github.com/pentaho/pentaho-vertica-bulkloader/actions/runs/43943349774)
passed with the checkout-built candidate on PDI `10.2.0.11-459`: 74 unit tests,
two helper tests and 25 normal integration tests passed; the two wide tests were
skipped. Download `vertica-evidence-43943349774-1` from the run's **Artifacts**
as a ZIP. Its Failsafe report identifies the skipped cases, and normal long-type
logs are under `evidence/PDI-20182-<id>/<test-name>-<id>/`; it contains no wide
execution evidence. CI artifacts are retained for 30 days; this artifact was
listed as expiring on December 16, 2026, but retention can change.

The following paths are under `integration-test/pentaho-platform/target/` in
the original local SP-7790 test worktree, not the public PR worktree:

| Evidence directory | Matching Failsafe reports | Outcome |
| --- | --- | --- |
| `evidence-sp7790-bundled-wide-default` | `reports-sp7790-wide-default` | Bundled build 459, default heap: both wide tests failed with `OutOfMemoryError`. |
| `evidence-sp7790-bundled-wide-4g` | `reports-sp7790-wide-4g` | Bundled build 459, 4 GB heap: both wide tests passed. |

Each pair contains per-test Pan logs, status and SQL output alongside the saved
JUnit reports. Review logs and metadata for sensitive details before packaging
and attaching the local pairs separately from the CI ZIP. Never attach license
configuration or Docker build contexts. Local artifacts are not uploaded to CI.