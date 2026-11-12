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

### Latest: build 332 with the 11.0.0.4 image

On November 12, 2026, both the published `11.0.0.4-332` plugin and the 11.0
candidate were tested with the `11.0.0.4` automation image, its JDBC 24.4 driver,
and default SHA-512 authentication. No JDBC override was configured.

| Plugin under test | Result | Duration / finish (UTC) |
| --- | --- | --- |
| Plugin bundled with `11.0.0.4-332` | 12 passed, 0 failed, 0 skipped | 4:42 / 15:46:33 |
| 11.0 branch candidate | 12 passed, 0 failed, 0 skipped | 4:39 / 15:51:24 |

| Provenance | Value |
| --- | --- |
| PDI archive | `11.0.0.4-332`; catalog SHA-1 `e3d2f791aa442c4040fd024e098668b2ddc8ea22`; SHA-256 `906814a1bfef1b8c7364bf46ae0d1afa5e3869a8fbb3e476bb91470203cf1549` |
| PDI automation image | `repo.pentaho.com/pnt-docker/automation/pdi-client:11.0.0.4`; digest and image ID `sha256:2f90205ff816ae2b51ee927363444b837001fce73af0efbe31424e547fd46efe` |
| JDBC image JAR | `vertica-jdbc-24.4.0-0.jar`; SHA-256 `cc720c960d2280643b5c1d463a15e05a97fe9f456b30d1dfb5f16405ecbb0637` |
| Candidate assembly used in integration run | SHA-256 `98b285f300d5e227638141b6a279c25eae733b1645f393a976c7f390a29b9c59` |
| PDI runtime | Ubuntu 22.04.5 LTS, Linux x86-64, OpenJDK `21.0.13.0.3`; Docker amd64 emulation on macOS 15.8.2 ARM64 |
| Database | Vertica CE `24.1.0-0` |

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
remote CI require separate validation. A timeout or forced termination is a
failure. See the [execution guide](../integration-tests-README.md) for setup and
evidence details.