# PDI-20182 QA validation summary

## Scope and conclusion

[PDI-20182](https://pentaho-eng.atlassian.net/browse/PDI-20182) reports that the
Vertica Bulk Loader rejects existing `LONG VARCHAR` and `LONG VARBINARY` columns
as unsupported. Jira's acceptance-criteria fields are empty. The passing tests
directly validate the reported behavior for both types, including values over
65,000 bytes, in the configuration below. Spoon-generated DDL is tracked separately in
[PDI-18512](https://pentaho-eng.atlassian.net/browse/PDI-18512).

**Conclusion: the core fix is validated for the configuration below, with a
significant maximum-width limitation.** Values of both long types, including
values over 65,000 bytes, loaded correctly. However, both maximum-width tests
failed with the distribution's default 2 GB Pan heap; they passed with a 4 GB heap.
Full maximum-width support with default settings is therefore not validated.
The product owner and developer agreed to track the limitation separately in
[PDI-21096](https://pentaho-eng.atlassian.net/browse/PDI-21096).

## Environment and execution

The tests ran on November 13, 2026 using real Pan, native Vertica COPY and
independent database assertions. Inputs were generated in disposable Vertica
tables; large values were compared in bounded SHA-256 chunks.

| Component | Tested configuration |
| --- | --- |
| PDI | `11.1.0.0-468`, bundled Vertica Bulk Loader plugin |
| PDI image | `repo.pentaho.com/pnt-docker/automation/pdi-client:11.1` |
| JDBC | Image-supplied Vertica JDBC `24.4.0-0`, the supported target |
| Database | Vertica CE `24.1.0-0` in a disposable container |
| Test branch | Local `PDI-20182_tests`, based on `PDI_20802_tests` |

The 468 archive did not supply the JDBC JAR; the harness preserved the driver
from the PDI image. Its recorded SHA-256 is
`cc720c960d2280643b5c1d463a15e05a97fe9f456b30d1dfb5f16405ecbb0637`.
No JDBC override was used. This is staged-driver provenance, not a JVM class-load
trace. Results are specific to this configuration.

The [PR integration run](https://github.com/pentaho/pentaho-vertica-bulkloader/actions/runs/43856426183)
passed on November 16, 2026 using the checkout-built candidate plugin with PDI
`11.1.0.0-472`: 74 project unit tests, two helper tests and 25 normal integration
tests passed. The two opt-in maximum-width tests were skipped in CI. This CI
result is separate from the bundled build 468 results below.

## Test results

The normal combined run passed **13 PDI-20182 tests and all 12 existing
PDI-20802/lifecycle regression tests**. Two opt-in maximum-width tests were
skipped in that run and executed separately, as shown below.

| Scenario | Expected result | Result |
| --- | --- | --- |
| Small UTF-8 text and binary, direct/reordered mapping | Both values match. | Pass |
| Long text with ordinary binary | Both values match. | Pass |
| Long binary with ordinary text | Both values match. | Pass |
| Default-width long text with ordinary binary | Both small values match. | Pass |
| Default-width long binary with ordinary text | Both small values match. | Pass |
| 64,999-, 65,000- and 65,001-byte text and binary | Contents and lengths match. | Pass |
| Text and binary above 65,000 bytes | Contents and lengths match, including UTF-8 and bytes `00`, `80` and `FF`. | Pass |
| Null and empty values, non-direct/positional mapping | Null and empty remain distinct. | Pass |
| Ticket-inspired Janino UTF-8 conversion | Null, empty, small and long converted values match. | Pass; Janino temporarily enabled |
| Three loader copies | All 600 distinct IDs and values match; each copy processes rows. | Pass |
| 1,048,575-row load | All IDs are unique and all text and binary values match. | Pass |
| Over-width text | SQL rejection preserves the previously committed row; a subsequent valid load succeeds. | Pass |
| Over-width binary | SQL rejection preserves the previously committed row; a subsequent valid load succeeds. | Pass |
| 32,000,000-byte long text | All chunks and the length match. | Fail with default 2 GB heap; pass with 4 GB heap |
| 32,000,000-byte long binary | All chunks and the length match. | Fail with default 2 GB heap; pass with 4 GB heap |

The Janino fixture adapts the ticket's UTF-8 expression, rather than copying its
environment-specific transformation verbatim. Janino is disabled in the tested
archive by `.kettle-ignore`; the test temporarily enables it inside the disposable
container and restores the marker afterward.

### Transformations and scenarios

All 15 scenarios run in Pan; none uses a Kitchen job. The harness creates the
source and target tables and renders scenario-specific settings before execution.

| Fixture | Steps | Scenarios |
| --- | --- | --- |
| [long-types.ktr](pentaho-platform/src/test/resources/transformations/vertica_bulk_loader/long-types.ktr) | **Read source** (Table Input) reads the generated source table; **Load long types** (Vertica Bulk Loader) writes the existing target using native COPY. | All scenarios in the table above except the Janino conversion; the harness varies target types, mapping, direct mode, source values and loader copies. |
| [long-types-janino.ktr](pentaho-platform/src/test/resources/transformations/vertica_bulk_loader/long-types-janino.ktr) | **Read source** feeds **Convert to UTF-8** (Janino), then **Load long types** writes text and converted binary. | Ticket-inspired Janino UTF-8 conversion. |

The [test class](pentaho-platform/src/test/java/org/pentaho/di/verticabulkload/it/VerticaBulkLoaderLongTypesIT.java)
prepares data and independently checks target values, lengths and sessions; those
checks are not all transformation steps. The test names and expected outcomes are
listed in the [integration test README](../integration-tests-README.md).

A negative-control run against bundled PDI `11.1.0.0-461` failed with
`Column type LONG VARBINARY not supported`, as expected for the older plugin.
It also logged a separate missing `com.vertica.jdbc.SConnection` cleanup class;
this comparison does not independently prove the older `LONG VARCHAR` behavior.

## Maximum-width finding

For both long types, loading a 32,000,000-byte value into a maximum-width target
failed with `OutOfMemoryError: Java heap space` in `StreamEncoder` before COPY,
using the distribution's default `-Xmx2048m`. The encoder requests a byte buffer
close to 2 GB for these declared widths, leaving insufficient room in that heap
for other allocations. Both values passed when Pan was explicitly configured
with `-Xmx4g` and verified independently in 32,000-byte chunks. This does not
establish that every deployment has enough memory or that multiple maximum-width
loader copies will work.

## Remaining coverage

| Item | Status and priority |
| --- | --- |
| Non-direct COPY with values over 65,000 bytes | Not tested; highest-value remaining mode check because non-direct currently covers only null and empty values. |
| Direct/positional and non-direct/reordered mapping with long values | Not tested; lower priority unless these combinations are required workflows. |
| Default 1 MB width with a 1 MB value | Not tested; small values in default-width columns passed. |
| Maximum width with default heap | Failed; tracked separately in PDI-21096. |
| PDI 10.2 and 11.0 backports | Tested separately; see the [SP-7790](https://github.com/pentaho/pentaho-vertica-bulkloader/blob/SP-7790_tests/integration-test/SP-7790-QA-summary.md) and [SP-7791](https://github.com/pentaho/pentaho-vertica-bulkloader/blob/SP-7791_tests/integration-test/SP-7791-QA-summary.md) QA summaries. |
| Spoon-generated DDL and Pentaho Server | Not tested by the headless Pan harness; generated DDL belongs to PDI-18512. |

The supported JDBC target is the PDI image's 24.4 driver. Other widths, data patterns, multiple maximum-width
copies and the full cross-product of loading options were not exhaustively tested.

## Reproduction and evidence

The [integration test README](../integration-tests-README.md) contains run
commands, an [evidence guide](../integration-tests-README.md#reading-test-evidence)
and the opt-in maximum-width configuration. The suite is
`VerticaBulkLoaderLongTypesIT`; the normal run uses the default Pan heap, and the
maximum-width run requires `vertica.it.run-wide-tests=true` and a suitably
provisioned isolated Docker host.

### CI evidence to attach

The post-rebase [CI run, attempt 5](https://github.com/pentaho/pentaho-vertica-bulkloader/actions/runs/43943289889/attempts/5)
passed with the checkout-built candidate on PDI `11.1.0.0-472`: 74 project unit
tests, two helper tests and 25 normal integration tests passed; the two wide tests
were skipped. Attempts 1 and 4 failed during database setup; attempts 2 and 3
timed out in lifecycle tests. The later pass does not establish that the
intermittent failures are resolved.

From that run's **Artifacts**, download `vertica-evidence-43943289889-5` as a
ZIP. It contains the Failsafe reports and the same per-test evidence structure as
local runs. In `failsafe-reports/TEST-org.pentaho.di.verticabulkload.it.VerticaBulkLoaderLongTypesIT.xml`,
13 cases passed and the two maximum-width cases have `skipped` children; there
are no wide-test execution logs in this CI ZIP. Normal test logs are under
`evidence/PDI-20182-<id>/<test-name>-<id>/`. Artifacts are retained for 30 days;
download while available and review logs and metadata for sensitive information
before attaching them to Jira. The successful attempt's artifact was listed as
expiring on December 16, 2026; retention can change. Never attach license
configuration or Docker build contexts.

### Local maximum-width evidence to attach

The following paths are under `integration-test/pentaho-platform/target/` in
the local PDI-20182 test worktree. They are generated evidence, not committed
certification:

| Evidence directory | Plugin and outcome |
| --- | --- |
| `evidence-pdi20182-maximum` | Bundled PDI 468, default 2 GB Pan heap: both wide tests failed with `OutOfMemoryError`; both Pan status files record `exit=1`. |
| `evidence-pdi20182-maxfinal` | Bundled PDI 468, 4 GB Pan heap: both wide tests passed; both Pan status files record `exit=0`. |
| `evidence-pdi20182-rebase-wide-1116` | Locally built candidate on PDI 468, 4 GB Pan heap: both wide tests passed; this is separate from the bundled run. |

Each directory contains `build.txt` and per-test `PDI-20182-<id>/maximumWidth...`
directories with Pan logs, status and SQL query output. The 4 GB successes verify
the 32,000,000-byte length and 1,000 bounded chunk hashes for each value; the
Java test contains the expected assertions, since recorded SQL commands are
redacted. These evidence directories do not contain their Failsafe reports.
At the time of this review, `target/failsafe-reports` contains the matching
candidate 4 GB report: its recorded evidence directory is
`evidence-pdi20182-rebase-wide-1116`, its source is `local`, and its heap is
`-Xmx4g`. That default report directory is overwritten by later runs; verify its
properties before pairing it with evidence. It is not the report for the bundled
default-heap or bundled 4 GB runs. Review the desired logs, reports and metadata,
then package them separately from the CI ZIP for Jira, clearly labeling plugin
source, heap and outcome. Local artifacts remain on the test host and are not
included in CI uploads.