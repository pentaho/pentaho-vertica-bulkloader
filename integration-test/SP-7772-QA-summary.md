# SP-7772 QA validation summary

## Scope

[SP-7772](https://pentaho-eng.atlassian.net/browse/SP-7772) is the 11.0
backport of PDI-20802, which addresses Vertica Bulk Loader hangs after certain
Vertica SQL exceptions. The test target is PDI 11.0.0.4 with the plugin built
from the 11.0 branch, as well as the plugin bundled with the tested PDI build.
The initial backport was merged in [PR #106](https://github.com/pentaho/pentaho-vertica-bulkloader/pull/106),
commit `3718dcbd`; the JDBC 24 follow-up was merged in
[PR #108](https://github.com/pentaho/pentaho-vertica-bulkloader/pull/108),
commit `2ac850697eb271f74fa4e8bbd2d0e584eeb9fd36`. The SP-7772 Jira comment
identifies `11.0.0.4-332` as containing the follow-up.

## Results

### Latest CI: build 334

On November 13, 2026, [11.0 PR CI attempt 1](https://github.com/pentaho/pentaho-vertica-bulkloader/actions/runs/43491538873/attempts/1)
passed all 12 integration tests with no failures or skips. The candidate plugin
from commit `b1b00844` was tested on PDI `11.0.0.4-334` with JDBC `24.4.0-0`
and disposable Vertica CE `24.1.0-0`, on runner `pentaho-runner-shrrb-0`.
No external license override was supplied, so the image configuration was used.
This CI run tested the candidate, not the distribution-bundled plugin.

An [earlier CI run](https://github.com/pentaho/pentaho-vertica-bulkloader/actions/runs/43431178584)
had a license TLS stall; the successful run does not establish that the
intermittent licensing issue is resolved.

For help interpreting reports and per-test logs, see the
[evidence guide](../integration-tests-README.md#reading-test-evidence).

### Latest local comparison: build 332 with the 11.0.0.4 image

On November 12, 2026, both the published `11.0.0.4-332` plugin and the 11.0
candidate were tested with the `11.0.0.4` automation image, its JDBC 24.4 driver,
and default SHA-512 authentication. No JDBC override was configured.

| Plugin under test | Result | Duration / finish (UTC) |
| --- | --- | --- |
| Plugin bundled with `11.0.0.4-332` | 12 passed, 0 failed, 0 skipped | 4:42 / 15:46:33 |
| 11.0 branch candidate | 12 passed, 0 failed, 0 skipped | 4:39 / 15:51:24 |

| Provenance | Value |
| --- | --- |
| PDI archive | `pdi-ee-client-11.0.0.4-332.zip`; catalog SHA-1 `e3d2f791aa442c4040fd024e098668b2ddc8ea22`; SHA-256 `906814a1bfef1b8c7364bf46ae0d1afa5e3869a8fbb3e476bb91470203cf1549` |
| Bundled plugin | Vertica Bulk Loader included in the verified `11.0.0.4-332` archive |
| Candidate assembly | `vertica-bulkloader-plugin-11.0.0.0-SNAPSHOT.zip`, built from the 11.0 branch at `2ac85069`; SHA-256 `98b285f300d5e227638141b6a279c25eae733b1645f393a976c7f390a29b9c59` |
| PDI automation image | `repo.pentaho.com/pnt-docker/automation/pdi-client:11.0.0.4`; digest and image ID `sha256:2f90205ff816ae2b51ee927363444b837001fce73af0efbe31424e547fd46efe` |
| Staged Vertica JDBC | Image-supplied `vertica-jdbc-24.4.0-0.jar`; SHA-256 `cc720c960d2280643b5c1d463a15e05a97fe9f456b30d1dfb5f16405ecbb0637`; default authentication, no JDBC override |
| PDI runtime | Ubuntu 22.04.5 LTS, Linux x86-64, OpenJDK `21.0.13.0.3`; Docker amd64 emulation on macOS 15.8.2 ARM64 |
| Vertica | `repo.pentaho.com/docker/vertica-ce:24.1.0-0`; image ID not captured in run evidence |

The exact 332 archive replaces the image runtime and has no Vertica JDBC JAR in
`lib`; the harness preserved the image's JDBC 24.4 JAR. Both test classes in both
runs recorded its source and checksum, and each final runtime contained that
Vertica-named JAR in `lib`. No JVM class-load trace was captured, so this is
staged-driver provenance, not independent proof of loaded class origin.
Evidence, including separate Failsafe reports and archive provenance, is retained
locally under `target/evidence/sp7772-332-image-bundled` and
`target/evidence/sp7772-332-image-candidate`.

Both variants passed the seven original and five lifecycle scenarios, including
natural SQL failure and recovery, active-COPY Abort and pooled same-JVM recovery.
The candidate unit suite passed 72 tests, including two tests for
pooled-lease release ordering; these are mock-based unit tests, not integration
proof of the forced-stop race. The bundled and candidate binaries are separate
variants and are not assumed to be identical.

## CI scenario results

The following scenarios all passed in the build 334 candidate CI run above.
The templates are rendered with scenario-specific settings before execution.

| Scenario | Goal and expected result | Coverage area | Transformation and job fixtures |
| --- | --- | --- | --- |
| `healthyEofLargeStream` | 100,000 mapped/direct rows finish at EOF and preserve values. | Normal completion | [load.ktr][load-fixture] |
| `defaultMappingNonDirect` | 17 positional/non-direct rows finish and preserve values. | Mapping and COPY mode | [load.ktr][load-fixture] |
| `emptyInput` | An empty stream succeeds without target rows. | Empty stream | [load.ktr][load-fixture] |
| `copyStartPermissionDenied` | A COPY permission error propagates and commits no rows. | COPY-start error | [load.ktr][load-fixture] |
| `serverCheckRejectionAborts` | A server CHECK rejection propagates and commits no rows. | Server-side rejection | [load.ktr][load-fixture] |
| `kitchenFailureBranch` | The error hop writes one audit marker and ABORT fails the job. | Job error handling | [load.ktr][load-fixture], [failure-handler.kjb][failure-fixture] |
| `repeatedFailureThenRecovery` | Two failures are followed by a successful 23-row load in separate PDI processes. | Failure and recovery | [load.ktr][load-fixture] |
| `multipleCopiesCompleteAtEof` | Three copies commit all 100,000 correct rows. | Concurrent copies; EOF | [load.ktr][load-fixture] |
| `slowInputCompletesAtEof` | 100 rows stream for over five seconds and commit at EOF. | Slow upstream input | [slow-input.ktr][slow-fixture] |
| `multipleCopiesStopAfterCopyFailure` | Three copies finish after a permission error and commit no rows. | Concurrent copies; failure | [load.ktr][load-fixture] |
| `pooledConnectionRecoversInSameKitchenJvm` | After a pooled failure, restored access permits a 100,000-row load in the same Kitchen JVM. | Pooling and recovery | [load.ktr][load-fixture], [pooled-recovery.kjb][pooled-fixture] |
| `transformationAbortDuringCopy` | Abort during server-confirmed active COPY terminates naturally and commits no rows. | Active COPY cancellation | [abort.ktr][abort-fixture] |

### Fixture steps and job entries

Step and entry names below are prefixed with `PDI-20802` in the fixtures.

| Fixture | Steps or job entries and purpose |
| --- | --- |
| [load.ktr][load-fixture] | **Generate Rows** (`generate rows`) creates an ID and payload; **Vertica Bulk Loader** (`bulk load`) loads them using native COPY. The harness selects the row count, mapping, direct mode, pooling and number of copies. |
| [slow-input.ktr][slow-fixture] | **Text File Input** (`slow rows`) reads the harness-fed FIFO; **Vertica Bulk Loader** (`bulk load`) loads the rows until EOF. |
| [abort.ktr][abort-fixture] | **Generate Rows** (`generate rows`) feeds **Vertica Bulk Loader** (`bulk load`), followed by **Abort** (`abort`), configured with a 5,000-row threshold. The test separately verifies that COPY was active when Abort fired. |
| [failure-handler.kjb][failure-fixture] | **Start** runs **Transformation** (`load`). On failure, **SQL** (`failure audit`) writes an audit marker and **Abort** (`preserve failure`) fails the job. Unexpected success goes to a separate **Abort** (`unexpected success`). |
| [pooled-recovery.kjb][pooled-fixture] | **Start** runs **Transformation** (`denied load`). On failure, **SQL** (`restore access`) grants INSERT and **Transformation** (`pooled recovery`) retries in the same Kitchen JVM. Unexpected success goes to **Abort** (`unexpected success`). |

The Java harness prepares permissions and test data and independently checks
results and sessions; those actions are not all steps in the fixtures.

[load-fixture]: pentaho-platform/src/test/resources/transformations/vertica_bulk_loader/load.ktr
[slow-fixture]: pentaho-platform/src/test/resources/transformations/vertica_bulk_loader/slow-input.ktr
[abort-fixture]: pentaho-platform/src/test/resources/transformations/vertica_bulk_loader/abort.ktr
[failure-fixture]: pentaho-platform/src/test/resources/jobs/vertica_bulk_loader/failure-handler.kjb
[pooled-fixture]: pentaho-platform/src/test/resources/jobs/vertica_bulk_loader/pooled-recovery.kjb

## Coverage and limitations

The real Pan/Kitchen and Vertica integration suites cover healthy and empty
loads, SQL failures and recovery, multi-copy execution, slow input, transformation
Abort and pooled recovery. Both variants' permission-denied and CHECK-rejection
cases demonstrate natural failure for those specific SQL errors, and their Kitchen
failure-branch cases verify an audit marker and propagated job failure.

SP-7772's acceptance criteria require the step to terminate and report failure,
the transformation not to hang or leave active threads, and job error handling
to run without manual termination for Vertica SQL exceptions. The passing proxy
cases provide partial evidence, not proof for every SQL exception or pooled
connection. Original mergeout-queue, disk-space and broken-projection failures,
Spoon Stop, slow COPY finalization, the exact pooled-wrapper lease race and
multiple-copy forced stop require separate validation. The tests do not prove the
absence of every in-JVM thread leak or measure an immediate-failure latency
requirement. A timeout or forced termination is a failure. See the
[execution guide](../integration-tests-README.md) for setup and evidence details.