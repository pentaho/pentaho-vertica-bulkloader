# PDI-20182 QA validation summary

## Scope and conclusion

[PDI-20182](https://pentaho-eng.atlassian.net/browse/PDI-20182) addresses loading
values into existing Vertica `LONG VARCHAR` and `LONG VARBINARY` columns with the
Vertica Bulk Loader. Spoon-generated DDL is tracked separately in
[PDI-18512](https://pentaho-eng.atlassian.net/browse/PDI-18512).

**Conclusion: the core fix is validated for the configuration below, with a
significant maximum-width limitation.** Values of both long types, including
values over 65,000 bytes, loaded correctly. However, both maximum-width tests
failed with the distribution's default 2 GB Pan heap; they passed with a 4 GB heap.
Full maximum-width support with default settings is therefore not validated.
QA sign-off depends on whether that limitation is acceptable or tracked separately.

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
| Maximum width with default heap | Failed; product/QA decision needed on whether the limitation blocks acceptance. |
| PDI 10.2 and 11.0 backports | Not tested; validate separately if those releases are in scope. |
| Spoon-generated DDL and Pentaho Server | Not tested by the headless Pan harness; generated DDL belongs to PDI-18512. |

The supported JDBC target is the PDI image's 24.4 driver; JDBC 24.2 is not a
required target for this ticket. Other widths, data patterns, multiple maximum-width
copies and the full cross-product of loading options were not exhaustively tested.

## Reproduction and evidence

The [integration test README](../integration-tests-README.md) contains run
commands, transformation details and the opt-in maximum-width configuration.
The suite is `VerticaBulkLoaderLongTypesIT`; the normal run uses the default Pan
heap, and the maximum-width run requires `vertica.it.run-wide-tests=true` and a
suitably provisioned isolated Docker host. Local evidence is retained under the
platform module's `target/evidence-pdi20182-regression`,
`target/evidence-pdi20182-maximum` and `target/evidence-pdi20182-maxfinal`.
These are local generated logs, not committed certification; review before sharing.